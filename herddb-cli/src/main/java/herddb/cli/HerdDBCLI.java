/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.cli;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBConnection;
import herddb.jdbc.HerdDBConnection;
import herddb.jdbc.HerdDBDataSource;
import herddb.model.TableSpace;
import herddb.utils.IntHolder;
import herddb.utils.SimpleBufferedOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import javax.script.ScriptException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.InsertDeParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 *
 * @author enrico.olivelli
 */
public class HerdDBCLI {

    static volatile int exitCode = 0;

    public static void main(String... args) throws IOException {
        try {
            DefaultParser parser = new DefaultParser();
            Options options = new Options();
            options.addOption("x", "url", true, "JDBC URL");
            options.addOption("u", "username", true, "JDBC Username");
            options.addOption("pwd", "password", true, "JDBC Password");
            options.addOption("q", "query", true, "Execute inline query");
            options.addOption("v", "verbose", false, "Verbose output");
            options.addOption("s", "schema", true, "Default tablespace (SQL schema)");
            options.addOption("f", "file", true, "SQL Script to execute (statement separated by 'GO' lines)");
            options.addOption("at", "autotransaction", false, "Execute scripts in autocommit=false mode and commit automatically");
            options.addOption("atbs", "autotransactionbatchsize", true, "Batch size for 'autotransaction' mode");
            options.addOption("g", "script", true, "Groovy Script to execute");
            options.addOption("i", "ignoreerrors", false, "Ignore SQL Errors during file execution");
            options.addOption("sc", "sqlconsole", false, "Execute SQL console in interactive mode");
            options.addOption("fmd", "mysql", false, "Intruct the parser that the script is coming from a MySQL Dump");
            options.addOption("rwst", "rewritestatements", false, "Rewrite all statements to use JDBC parameters");
            options.addOption("d", "dump", true, "Dump tablespace");
            options.addOption("r", "restore", true, "Restore tablespace");
            options.addOption("nl", "newleader", true, "Leader for new restored tablespace");
            options.addOption("ns", "newschema", true, "Name for new restored tablespace");
            options.addOption("tsm", "tablespacemapper", true, "Path to groovy script with a custom functin to map table names to tablespaces");
            options.addOption("dfs", "dumpfetchsize", true, "Fetch size for dump operations."
                + "Defaults to chunks of 100000 records");
            org.apache.commons.cli.CommandLine commandLine;
            try {
                commandLine = parser.parse(options, args);
            } catch (ParseException error) {
                println("Syntax error: " + error);
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("herddb", options, true);
                exitCode = 1;
                return;
            }
            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("herddb", options, true);
                exitCode = 1;
                return;
            }

            final boolean verbose = commandLine.hasOption("verbose");
            if (!verbose) {
                LogManager.getLogManager().reset();
            }
            String url = commandLine.getOptionValue("url", "jdbc:herddb:server:localhost:7000");
            String username = commandLine.getOptionValue("username", ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
            String password = commandLine.getOptionValue("password", ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
            String schema = commandLine.getOptionValue("schema", TableSpace.DEFAULT);
            String query = commandLine.getOptionValue("query", "");
            String file = commandLine.getOptionValue("file", "");
            String dump = commandLine.getOptionValue("dump", "");
            String restore = commandLine.getOptionValue("restore", "");
            String newschema = commandLine.getOptionValue("newschema", "");
            String leader = commandLine.getOptionValue("newleader", "");
            String script = commandLine.getOptionValue("script", "");
            String tablespacemapperfile = commandLine.getOptionValue("tablespacemapper", "");
            int dumpfetchsize = Integer.parseInt(commandLine.getOptionValue("dumpfetchsize", 100000 + ""));
            final boolean ignoreerrors = commandLine.hasOption("ignoreerrors");
            boolean sqlconsole = commandLine.hasOption("sqlconsole");
            final boolean frommysqldump = commandLine.hasOption("mysql");
            final boolean rewritestatements = commandLine.hasOption("rewritestatements") || !tablespacemapperfile.isEmpty() || frommysqldump;
            boolean autotransaction = commandLine.hasOption("autotransaction") || frommysqldump;
            int autotransactionbatchsize = Integer.parseInt(commandLine.getOptionValue("autotransactionbatchsize", 100000 + ""));
            if (!autotransaction) {
                autotransactionbatchsize = 0;
            }
            TableSpaceMapper tableSpaceMapper = buildTableSpaceMapper(tablespacemapperfile);
            try (HerdDBDataSource datasource = new HerdDBDataSource()) {
                datasource.setUrl(url);
                datasource.setUsername(username);
                datasource.setPassword(password);

                try (Connection connection = datasource.getConnection();
                    Statement statement = connection.createStatement()) {
                    connection.setSchema(schema);
                    if (sqlconsole) {
                        runSqlConsole(connection, statement);
                    } else if (!query.isEmpty()) {
                        executeStatement(verbose, ignoreerrors, false, false, query, statement, tableSpaceMapper);
                    } else if (!file.isEmpty()) {
                        if (autotransactionbatchsize > 0) {
                            connection.setAutoCommit(false);
                        }
                        long _start = System.currentTimeMillis();
                        final IntHolder doneCount = new IntHolder();
                        final IntHolder totalDoneCount = new IntHolder();
                        File f = new File(file);
                        long fileSize = f.length();
                        try (FileInputStream rawStream = new FileInputStream(file);
                            BufferedInputStream buffer = new BufferedInputStream(rawStream);
                            CounterInputStream counter = new CounterInputStream(buffer);
                            InputStream fIn = wrapStream(f, counter);
                            CounterInputStream counterUnzipped = new CounterInputStream(fIn);
                            InputStreamReader ii = new InputStreamReader(counterUnzipped, "utf-8");) {
                            int _autotransactionbatchsize = autotransactionbatchsize;
                            SQLFileParser.parseSQLFile(ii, (st) -> {
                                if (!st.comment) {
                                    int count = executeStatement(verbose, ignoreerrors, frommysqldump, rewritestatements, st.content, statement, tableSpaceMapper);
                                    doneCount.value += count;
                                    totalDoneCount.value += count;
                                    if (_autotransactionbatchsize > 0 && doneCount.value > _autotransactionbatchsize) {
                                        long _now = System.currentTimeMillis();
                                        long countZipped = counter.count;
                                        int percent = (int) (counter.count * 100.0 / fileSize);
                                        long delta = (_now - _start);
                                        long countUnzipped = counterUnzipped.count;
                                        double speed = ((countUnzipped * 60000.0) / (1.0 * delta));

                                        double speedZipped = ((countZipped * 60000.0) / (1.0 * delta));

                                        if (countUnzipped != counter.count) {
                                            System.out.println(new java.sql.Timestamp(System.currentTimeMillis())
                                                + " COMMIT after " + totalDoneCount.value + " records, read " + formatBytes(counter.count) + " (" + formatBytes(countUnzipped) + " unzipped) over " + formatBytes(fileSize) + ". " + percent + "%, " + formatBytes(speed) + "/min (UNZIPPED "+formatBytes(speedZipped)+"/min)");
                                        } else {
                                            System.out.println(new java.sql.Timestamp(System.currentTimeMillis())
                                                + " COMMIT after " + totalDoneCount.value + " records, read " + formatBytes(counter.count) + " over " + formatBytes(fileSize) + ". " + percent + "%, " + formatBytes(speed) + " /min");
                                        }
                                        connection.commit();
                                        doneCount.value = 0;
                                    }
                                }
                            });
                        }
                        if (!connection.getAutoCommit()) {
                            System.out.println("final COMMIT after " + totalDoneCount.value + " records");
                            connection.commit();
                        }
                    } else if (!script.isEmpty()) {
                        Map<String, Object> variables = new HashMap<>();
                        variables.put("connection", connection);
                        variables.put("datasource", datasource);
                        variables.put("statement", statement);
                        GroovyShell shell = new GroovyShell(new Binding(variables));
                        shell.evaluate(new File(script));
                    } else if (!dump.isEmpty()) {
                        List<String> tablesToDump = new ArrayList<>();
                        try (ResultSet rs = statement.executeQuery("SELECT table_name FROM " + schema + ".systables WHERE systemtable='false'")) {
                            while (rs.next()) {
                                String tablename = rs.getString(1).toLowerCase();
                                tablesToDump.add(tablename);
                            }
                        }
                        Path outputfile = Paths.get(dump).toAbsolutePath();
                        println("Dumping tables " + tablesToDump + " from tablespace " + schema + " to " + outputfile);

                        try (OutputStream fout = Files.newOutputStream(outputfile, StandardOpenOption.CREATE_NEW);
                            SimpleBufferedOutputStream oo = new SimpleBufferedOutputStream(fout, 16 * 1024 * 1024);) {
                            HerdDBConnection hcon = connection.unwrap(HerdDBConnection.class);
                            HDBConnection hdbconnection = hcon.getConnection();
                            BackupUtils.dumpTableSpace(schema, dumpfetchsize, hdbconnection, oo, new ProgressListener() {
                                @Override
                                public void log(String actionType, String message, Map<String, Object> context) {
                                    println(message);
                                }

                            });
                        }
                        println("Dumping finished");
                    } else if (!restore.isEmpty()) {
                        Path inputfile = Paths.get(restore).toAbsolutePath();
                        if (leader.isEmpty() || newschema.isEmpty()) {
                            println("options 'newleader' and 'newschema' are required");
                            HelpFormatter formatter = new HelpFormatter();
                            formatter.printHelp("herddb", options, true);
                            exitCode = 1;
                            return;
                        }
                        List<String> nodes = new ArrayList<>();
                        try (ResultSet rs = statement.executeQuery("SELECT nodeid FROM sysnodes")) {
                            while (rs.next()) {
                                String nodeid = rs.getString(1);
                                nodes.add(nodeid);
                            }
                        }

                        println("Restoring tablespace " + newschema + " with leader " + leader + " from file " + inputfile);
                        if (!nodes.contains(leader)) {
                            println("There is no node with node id '" + leader + "'");
                            println("Valid nodes:");
                            for (String nodeid : nodes) {
                                println("* " + nodeid);
                            }
                            return;
                        }
                        try (InputStream fin = Files.newInputStream(inputfile);
                            InputStream bin = new BufferedInputStream(fin, 16 * 1024 * 1024)) {
                            HerdDBConnection hcon = connection.unwrap(HerdDBConnection.class);
                            HDBConnection hdbconnection = hcon.getConnection();
                            BackupUtils.restoreTableSpace(newschema, leader, hdbconnection, bin, new ProgressListener() {
                                @Override
                                public void log(String actionType, String message, Map<String, Object> context) {
                                    println(message);
                                }

                            });
                        }
                        println("Restore finished");
                    } else {
                        HelpFormatter formatter = new HelpFormatter();
                        formatter.printHelp("herddb", options, true);
                        exitCode = 1;
                        return;
                    }
                }
                exitCode = 0;
            } catch (Exception error) {
                if (verbose) {
                    error.printStackTrace();
                } else {
                    println("error:" + error);
                }
                exitCode = 1;
            }
        } finally {
            System.exit(exitCode);
        }
    }

    private static int executeStatement(boolean verbose, boolean ignoreerrors, boolean frommysqldump, boolean rewritestatements, String query, final Statement statement,
        final TableSpaceMapper tableSpaceMapper) throws SQLException, ScriptException {
        query = query.trim();

        if (query.isEmpty()
            || query.startsWith("--")) {
            return 0;
        }
        String formattedQuery = query.toLowerCase();
        if (formattedQuery.endsWith(";")) {
            // mysqldump
            formattedQuery = formattedQuery.substring(0, formattedQuery.length() - 1);
        }
        if (formattedQuery.equals("exit") || formattedQuery.equals("quit")) {
            System.out.println("Connection closed.");
            System.exit(0);
        }
        if (frommysqldump && (formattedQuery.startsWith("lock tables") || formattedQuery.startsWith("unlock tables"))) {
            // mysqldump
            return 0;
        }
        Boolean setAutoCommit = null;
        if (formattedQuery.startsWith("autocommit=")) {
            String value = "";
            if (formattedQuery.split("=").length > 1) {
                value = formattedQuery.split("=")[1];
            }
            switch (value) {
                case "true":
                    setAutoCommit = true;
                    break;
                case "false":
                    setAutoCommit = false;
                    break;
                default:
                    System.out.println("No valid value for autocommit. Only true and false allowed.");
                    return 0;
            }
        }
        if (verbose) {
            System.out.println("Executing query:" + query);
        }
        try {
            if (setAutoCommit != null) {
                statement.getConnection().setAutoCommit(setAutoCommit);
                System.out.println("Set autocommit=" + setAutoCommit + " executed.");
                return 0;
            }
            if (formattedQuery.equals("commit")) {
                statement.getConnection().commit();
                System.out.println("Commit executed.");
                return 0;
            }
            if (formattedQuery.equals("rollback")) {
                statement.getConnection().rollback();
                System.out.println("Rollback executed.");
                return 0;
            }

            QueryWithParameters rewritten = null;
            if (rewritestatements) {
                rewritten = rewriteQuery(query, tableSpaceMapper, frommysqldump);
            }
            if (rewritten != null) {
                if (rewritten.schema != null) {
                    HerdDBConnection connection = statement.getConnection().unwrap(HerdDBConnection.class);
                    if (connection != null && !connection.getSchema().equalsIgnoreCase(rewritten.schema)) {
                        changeSchemaAndCommit(connection, rewritten.schema);
                    }
                }
                try (PreparedStatement ps = statement.getConnection().prepareStatement(rewritten.query);) {
                    int i = 1;
                    for (Object o : rewritten.jdbcParameters) {
                        ps.setObject(i++, o);
                    }
                    boolean resultSet = ps.execute();
                    return reallyExecuteStatement(ps, resultSet, verbose);
                }
            } else {
                boolean resultSet = statement.execute(query);
                return reallyExecuteStatement(statement, resultSet, verbose);
            }
        } catch (SQLException err) {
            if (ignoreerrors) {
                println("ERROR:" + err);
                return 0;
            } else {
                throw err;
            }
        }
    }

    private static int reallyExecuteStatement(final Statement statement, boolean resultSet, boolean verbose) throws SQLException {

        if (resultSet) {
            try (ResultSet rs = statement.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                List<String> columns = new ArrayList<>();
                int ccount = md.getColumnCount();
                for (int i = 1; i <= ccount; i++) {
                    columns.add(md.getColumnName(i));
                }
                System.out.println(columns.stream().collect(Collectors.joining(";")));

                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= ccount; i++) {
                        String value = rs.getString(i);
                        if (value == null) {
                            value = "<NULL>";
                        }
                        values.add(value);
                    }
                    System.out.println(values.stream().collect(Collectors.joining(";")));
                }
            }
            return 0;
        } else {
            int updateCount = statement.getUpdateCount();
            if (verbose) {
                System.out.println("UPDATE COUNT: " + updateCount);
            }
            return updateCount >= 0 ? updateCount : 0;
        }
    }

    private static void println(Object msg) {
        System.out.println(msg);
    }

    private static void runSqlConsole(Connection connection, Statement statement) throws IOException {
        Terminal terminal = TerminalBuilder.builder()
            .system(true)
            .build();
        LineReader reader = LineReaderBuilder.builder()
            .history(new DefaultHistory())
            .terminal(terminal)
            .build();
        String prompt = "herd: ";
        while (true) {
            String line = null;
            try {
                line = reader.readLine(prompt);
                if (line == null) {
                    return;
                }
                executeStatement(true, true, false, false, line, statement, null);
            } catch (UserInterruptException | EndOfFileException e) {
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private static InputStream wrapStream(File file, InputStream rawStream) throws IOException {
        if (file.getName().endsWith(".gz")) {
            return new GZIPInputStream(rawStream);
        } else if (file.getName().endsWith(".zip")) {
            return new ZipInputStream(rawStream, StandardCharsets.UTF_8);
        } else {
            return rawStream;
        }

    }

    private final static Cache<String, net.sf.jsqlparser.statement.Statement> PARSER_CACHE = CacheBuilder
        .newBuilder()
        .maximumSize(50)
        .build();

    private static QueryWithParameters rewriteQuery(String query, TableSpaceMapper mapper, boolean frommysqldump) throws ScriptException {
        try {

            List<Object> parameters = new ArrayList<>();

            if (frommysqldump && query.startsWith("INSERT INTO")) {
                // this is faster than CCJSqlParserUtil and will allow the cache to work at "client-side" too
                QueryWithParameters rewriteSimpleInsertStatement = MySqlDumpInsertStatementRewriter.rewriteSimpleInsertStatement(query);
                if (rewriteSimpleInsertStatement != null) {
                    query = rewriteSimpleInsertStatement.query;
                    parameters.addAll(rewriteSimpleInsertStatement.jdbcParameters);
                    String schema = mapper == null ? null : mapper.getTableSpace(rewriteSimpleInsertStatement.tableName);
                    return new QueryWithParameters(query, rewriteSimpleInsertStatement.tableName, parameters, schema);
                }
            }

            String _query = query;
            net.sf.jsqlparser.statement.Statement stmt = PARSER_CACHE.get(_query, () -> {
                return CCJSqlParserUtil.parse(_query);
            });
            if (stmt instanceof Insert) {
                boolean somethingdone = false;
                Insert insert = (Insert) stmt;
                ItemsList itemlist = insert.getItemsList();
                if (itemlist instanceof ExpressionList) {
                    ExpressionList list = (ExpressionList) itemlist;
                    List<Expression> expressions = list.getExpressions();
                    for (int i = 0; i < expressions.size(); i++) {
                        Expression e = expressions.get(i);
                        boolean done = false;
                        if (e instanceof StringValue) {
                            StringValue sv = (StringValue) e;
                            parameters.add(sv.getValue());
                            done = true;
                        } else if (e instanceof LongValue) {
                            LongValue sv = (LongValue) e;
                            parameters.add(sv.getValue());
                            done = true;
                        } else if (e instanceof NullValue) {
                            NullValue sv = (NullValue) e;
                            parameters.add(null);
                            done = true;
                        } else if (e instanceof TimestampValue) {
                            TimestampValue sv = (TimestampValue) e;
                            parameters.add(sv.getValue());
                            done = true;
                        } else if (e instanceof DoubleValue) {
                            DoubleValue sv = (DoubleValue) e;
                            parameters.add(sv.getValue());
                            done = true;
                        }
                        if (done) {
                            somethingdone = true;
                            expressions.set(i, new JdbcParameter());
                        }
                    }
                    if (somethingdone) {
                        StringBuilder queryResult = new StringBuilder();
                        InsertDeParser deparser = new InsertDeParser(new ExpressionDeParser(null, queryResult), null, queryResult);
                        deparser.deParse(insert);
                        query = queryResult.toString();
                    }
                } else if (itemlist instanceof MultiExpressionList) {
                    MultiExpressionList mlist = (MultiExpressionList) itemlist;
                    List<ExpressionList> lists = mlist.getExprList();
                    for (ExpressionList list : lists) {
                        List<Expression> expressions = list.getExpressions();
                        for (int i = 0; i < expressions.size(); i++) {
                            Expression e = expressions.get(i);
                            boolean done = false;
                            if (e instanceof StringValue) {
                                StringValue sv = (StringValue) e;
                                parameters.add(sv.getValue());
                                done = true;
                            } else if (e instanceof LongValue) {
                                LongValue sv = (LongValue) e;
                                parameters.add(sv.getValue());
                                done = true;
                            } else if (e instanceof NullValue) {
                                NullValue sv = (NullValue) e;
                                parameters.add(null);
                                done = true;
                            } else if (e instanceof TimestampValue) {
                                TimestampValue sv = (TimestampValue) e;
                                parameters.add(sv.getValue());
                                done = true;
                            } else if (e instanceof DoubleValue) {
                                DoubleValue sv = (DoubleValue) e;
                                parameters.add(sv.getValue());
                                done = true;
                            }
                            if (done) {
                                somethingdone = true;
                                expressions.set(i, new JdbcParameter());
                            }
                        }
                    }
                    if (somethingdone) {
                        StringBuilder queryResult = new StringBuilder();
                        InsertDeParser deparser = new InsertDeParser(new ExpressionDeParser(null, queryResult), null, queryResult);
                        deparser.deParse(insert);
                        query = queryResult.toString();
                    }
                }
                String schema = mapper == null ? null : mapper.getTableSpace(stmt);
                return new QueryWithParameters(query, null, parameters, schema);
            } else {
                String schema = mapper == null ? null : mapper.getTableSpace(stmt);
                return new QueryWithParameters(query, null, Collections.emptyList(), schema);
            }
        } catch (ExecutionException err) {
            System.out.println("error for query: " + query + " -> " + err.getCause());
            return null;
        }
    }

    private static TableSpaceMapper buildTableSpaceMapper(String tablespacemapperfile) throws IOException {
        if (tablespacemapperfile.isEmpty()) {
            return null;
        } else {
            byte[] content = Files.readAllBytes(Paths.get(tablespacemapperfile));
            return new TableSpaceMapper(new String(content, StandardCharsets.UTF_8));
        }
    }

    private static final Set<String> existingTableSpaces = new HashSet<>();

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    private static void changeSchemaAndCommit(HerdDBConnection connection, String schema) throws SQLException {
        boolean autocommit = connection.getAutoCommit();
        if (!autocommit) {
            System.out.println("Forcing COMMIT in order to set schema to " + schema + " !");
            connection.commit();
        }
        if (!autocommit) {
            connection.setAutoCommit(true);
        }

        if (existingTableSpaces.isEmpty()) {
            try (ResultSet tableSpaces = connection.getMetaData().getSchemas()) {
                while (tableSpaces.next()) {
                    existingTableSpaces.add(tableSpaces.getString("TABLE_SCHEM").toLowerCase());
                }
            }

        }

        if (!existingTableSpaces.contains(schema.toLowerCase())) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLESPACE '" + schema + "','wait:60000'");
                existingTableSpaces.add(schema.toLowerCase());
            }
        }
        if (!autocommit) {
            connection.setAutoCommit(false);
        }

        connection.setSchema(schema);
    }

    private static String formatBytes(double bytes) {
        if (bytes > 1024 * 1024) {
            return (long) (bytes / (1024 * 1024)) + " MB";
        } else if (bytes > 1024) {
            return (long) (bytes / (1024)) + " KB";
        } else {
            return (long) bytes + " bytes";
        }
    }
}
