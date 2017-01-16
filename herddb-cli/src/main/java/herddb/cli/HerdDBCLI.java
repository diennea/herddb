/*
 * Copyright 2016 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.cli;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBConnection;
import herddb.jdbc.HerdDBConnection;
import herddb.jdbc.HerdDBDataSource;
import herddb.model.TableSpace;
import herddb.utils.SimpleBufferedOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
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

    public static void main(String... args) {
        DefaultParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("x", "url", true, "JDBC URL");
        options.addOption("u", "username", true, "JDBC Username");
        options.addOption("pwd", "password", true, "JDBC Password");
        options.addOption("q", "query", true, "Execute inline query");
        options.addOption("v", "verbose", false, "Verbose output");
        options.addOption("s", "schema", true, "Default tablespace (SQL schema)");
        options.addOption("f", "file", true, "SQL Script to execute (statement separated by 'GO' lines)");
        options.addOption("g", "script", true, "Groovy Script to execute");
        options.addOption("i", "ignoreerrors", false, "Ignore SQL Errors during file execution");
        options.addOption("sc", "sqlconsole", false, "Execute SQL console in interactive mode");
        options.addOption("d", "dump", true, "Dump tablespace");
        options.addOption("r", "restore", true, "Restore tablespace");
        options.addOption("nl", "newleader", true, "Leader for new restored tablespace");
        options.addOption("ns", "newschema", true, "Name for new restored tablespace");
        options.addOption("dfs", "dumpfetchsize", true, "Fetch size for dump operations."
            + "Defaults to chunks of 100000 records");
        org.apache.commons.cli.CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException error) {
            println("Syntax error: " + error);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("herddb", options, true);
            System.exit(1);
            return;
        }
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("herddb", options, true);
            System.exit(1);
            return;
        }

        boolean verbose = commandLine.hasOption("verbose");
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
        int dumpfetchsize = Integer.parseInt(commandLine.getOptionValue("dumpfetchsize", 100000 + ""));
        boolean ignoreerrors = commandLine.hasOption("ignoreerrors");
        boolean sqlconsole = commandLine.hasOption("sqlconsole");
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
                    executeStatement(verbose, ignoreerrors, query, statement);
                } else if (!file.isEmpty()) {
                    StringBuilder currentStatement = new StringBuilder();
                    for (String line : Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8)) {
                        if (line.trim().equalsIgnoreCase("GO")) {
                            executeStatement(verbose, ignoreerrors, currentStatement.toString(), statement);
                            currentStatement.setLength(0);
                        } else {
                            currentStatement.append(line + "\n");
                        }
                    }
                    executeStatement(verbose, ignoreerrors, currentStatement.toString(), statement);
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
                            public void log(String message, Map<String, Object> context) {
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
                        System.exit(1);
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
                            public void log(String message, Map<String, Object> context) {
                                println(message);
                            }

                        });
                    }
                    println("Restore finished");
                } else {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("herddb", options, true);
                    System.exit(1);
                    return;
                }
            }
            System.exit(0);
        } catch (Exception error) {
            if (verbose) {
                error.printStackTrace();
            } else {
                println("error:" + error);
            }
            System.exit(1);
        }
    }

    private static void executeStatement(boolean verbose, boolean ignoreerrors, String query, final Statement statement) throws SQLException {
        if (query.trim().isEmpty()) {
            return;
        }
        String formattedQuery = query.trim().toLowerCase();
        if (formattedQuery.equals("exit") || formattedQuery.equals("quit")) {
            System.out.println("Connection closed.");
            System.exit(0);
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
                    return;
            }
        }
        if (verbose) {
            System.out.println("Executing query:" + query);
        }
        try {
            if (setAutoCommit != null) {
                statement.getConnection().setAutoCommit(setAutoCommit);
                System.out.println("Set autocommit=" + setAutoCommit + " executed.");
                return;
            }
            if (formattedQuery.equals("commit")) {
                statement.getConnection().commit();
                System.out.println("Commit executed.");
                return;
            }
            if (formattedQuery.equals("rollback")) {
                statement.getConnection().rollback();
                System.out.println("Rollback executed.");
                return;
            }

            boolean resultSet = statement.execute(query);
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
            } else {
                int updateCount = statement.getUpdateCount();
                System.out.println("UPDATE COUNT: " + updateCount);
            }
        } catch (SQLException err) {
            if (ignoreerrors) {
                println("ERROR:" + err);
            } else {
                throw err;
            }
        }
    }

    private static void println(Object msg) {
        System.out.println(msg);
    }

    private static void runSqlConsole(Connection connection, Statement statement) throws IOException, SQLException {
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
                executeStatement(true, true, line, statement);
            } catch (UserInterruptException e) {
                // Ignore
            } catch (EndOfFileException e) {
                return;
            }

        }
    }
}
