/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cli;

import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import herddb.backup.BackupUtils;
import herddb.backup.ProgressListener;
import herddb.client.ClientConfiguration;
import herddb.client.HDBConnection;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.file.FileCommitLog;
import herddb.file.FileCommitLog.CommitFileReader;
import herddb.file.FileCommitLog.LogEntryWithSequenceNumber;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.blink.BLinkKeyToPageIndex.MetadataSerializer;
import herddb.index.blink.BLinkMetadata;
import herddb.jdbc.HerdDBConnection;
import herddb.jdbc.HerdDBDataSource;
import herddb.jdbc.PreparedStatementAsync;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.NodeMetadata;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
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
import org.codehaus.groovy.control.CompilationFailedException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * HerdDB command line interface
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION", justification = "This is a spotbugs bug")
public class HerdDBCLI {

    static volatile int exitCode = 0;

    static final boolean PRETTY_PRINT = true;

    public static void main(String... args) throws IOException {
        try {
            DefaultParser parser = new DefaultParser();
            Options options = new Options();
            options.addOption("x", "url", true, "JDBC URL");
            options.addOption("u", "username", true, "JDBC Username");
            options.addOption("pwd", "password", true, "JDBC Password");
            options.addOption("q", "query", true, "Execute inline query");
            options.addOption("v", "verbose", false, "Verbose output");
            options.addOption("a", "async", false, "Use (experimental) executeBatchAsync for sending DML");
            options.addOption("s", "schema", true, "Default tablespace (SQL schema)");
            options.addOption("fi", "filter", true, "SQL filter mode: all|ddl|dml");
            options.addOption("f", "file", true, "SQL Script to execute (statement separated by 'GO' lines)");
            options.addOption("at", "autotransaction", false,
                    "Execute scripts in autocommit=false mode and commit automatically");
            options.addOption("atbs", "autotransactionbatchsize", true, "Batch size for 'autotransaction' mode");
            options.addOption("g", "script", true, "Groovy Script to execute");
            options.addOption("i", "ignoreerrors", false, "Ignore SQL Errors during file execution");
            options.addOption("sc", "sqlconsole", false, "Execute SQL console in interactive mode");
            options.addOption("fmd", "mysql", false, "Intruct the parser that the script is coming from a MySQL Dump");
            options.addOption("rwst", "rewritestatements", false, "Rewrite all statements to use JDBC parameters");
            options.addOption("b", "backup", false, "Backup one or more tablespaces (selected with --schema)");
            options.addOption("r", "restore", false, "Restore tablespace");
            options.addOption("nl", "newleader", true, "Leader for new restored tablespace");
            options.addOption("ns", "newschema", true, "Name for new restored tablespace");
            options.addOption("tsm", "tablespacemapper", true,
                    "Path to groovy script with a custom functin to map table names to tablespaces");
            options.addOption("dfs", "dumpfetchsize", true,
                    "Fetch size for dump operations. Defaults to chunks of 100000 records");
            options.addOption("n", "nodeid", true, "Node id");
            options.addOption("t", "table", true, "Table name");
            options.addOption("p", "param", true, "Parameter name");
            options.addOption("val", "values", true, "Parameter values");
            options.addOption("lts", "list-tablespaces", false, "List available tablespaces");
            options.addOption("ln", "list-nodes", false, "List available nodes");
            options.addOption("sts", "show-tablespace", false,
                    "Show full informations about a tablespace (needs -s option)");
            options.addOption("lt", "list-tables", false, "List tablespace tables (needs -s option)");
            options.addOption("st", "show-table", false,
                    "Show full informations about a table (needs -s and -t options)");

            options.addOption("sl", "set-leader", false, "Set the leader for a tablespace (needs -s and -nl options)");
            options.addOption("ar", "add-replica", false, "Add a replica to the tablespace (needs -s and -r options)");
            options.addOption("rr", "remove-replica", false,
                    "Remove a replica from the tablespace (needs -s and -r options)");
            options.addOption("adt", "create-tablespace", false, "Create a tablespace (needs -ns and -nl options)");
            options.addOption("at", "alter-tablespace", false,
                    "Alter a tablespace (needs -s, -param and --values options)");

            options.addOption("d", "describe", false, "Checks and describes a raw file");
            options.addOption("ft", "filetype", true,
                    "Checks and describes a raw file (valid options are txlog, datapage, tablecheckpoint, indexcheckpoint, tablesmetadata, bkledger");
            options.addOption("mdf", "metadatafile", true, "Tables metadata file, required for 'datapage' filetype");
            options.addOption("tsui", "tablespaceuuid", true, "Tablespace UUID, used for describing raw files");
            options.addOption("lid", "ledgerid", true, "Ledger ID on BookKeeper");
            options.addOption("fromid", "fromid", true, "Starting entry Id for filetype=bkledger, default to 0");
            options.addOption("toid", "toid", true,
                    "Starting entry Id for filetype=bkledger, default to LastAddConfirmed");

            org.apache.commons.cli.CommandLine commandLine;
            try {
                commandLine = parser.parse(options, args);
            } catch (ParseException error) {
                println("Syntax error: " + error);
                failAndPrintHelp(options);
                return;
            }
            if (args.length == 0) {
                failAndPrintHelp(options);
                return;
            }

            String schema = commandLine.getOptionValue("schema", TableSpace.DEFAULT);
            String tablespaceuuid = commandLine.getOptionValue("tablespaceuuid", "");
            final boolean verbose = commandLine.hasOption("verbose");
            final boolean async = commandLine.hasOption("async");
            final String filter = commandLine.getOptionValue("filter", "all");
            if (!verbose) {
                LogManager.getLogManager().reset();
            }
            String file = commandLine.getOptionValue("file", "");
            String tablesmetadatafile = commandLine.getOptionValue("metadatafile", "");
            String table = commandLine.getOptionValue("table", "");
            boolean describe = commandLine.hasOption("describe");
            String filetype = commandLine.getOptionValue("filetype", "");
            long ledgerId = Long.parseLong(commandLine.getOptionValue("ledgerid", "0"));
            long fromId = Long.parseLong(commandLine.getOptionValue("fromid", "0"));
            long toId = Long.parseLong(commandLine.getOptionValue("toid", "-1")); // -1 = LAC
            String url = commandLine.getOptionValue("url", "jdbc:herddb:server:localhost:7000");
            String username = commandLine.getOptionValue("username",
                    ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
            String password = commandLine.getOptionValue("password",
                    ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT);
            if (describe) {
                try {
                    if (filetype.equals("bkledger")) {
                        try (HerdDBDataSource datasource = new HerdDBDataSource()) {
                            datasource.setUrl(url);
                            describeRawLedger(ledgerId, fromId, toId, datasource);
                        }
                    } else {
                        if (file.isEmpty()) {
                            throw new IllegalArgumentException("file option is required");
                        }
                        describeRawFile(tablespaceuuid, table, tablesmetadatafile, file, filetype);
                    }
                } catch (Exception error) {
                    if (verbose) {
                        error.printStackTrace();
                    } else {
                        println("error:" + error);
                    }
                    exitCode = 1;
                }
                return;
            }

            String query = commandLine.getOptionValue("query", "");

            boolean backup = commandLine.hasOption("backup");
            boolean restore = commandLine.hasOption("restore");
            String newschema = commandLine.getOptionValue("newschema", "");
            String leader = commandLine.getOptionValue("newleader", "");
            String script = commandLine.getOptionValue("script", "");
            String tablespacemapperfile = commandLine.getOptionValue("tablespacemapper", "");
            int dumpfetchsize = Integer.parseInt(commandLine.getOptionValue("dumpfetchsize", 100000 + ""));
            final boolean ignoreerrors = commandLine.hasOption("ignoreerrors");
            boolean sqlconsole = commandLine.hasOption("sqlconsole");
            final boolean frommysqldump = commandLine.hasOption("mysql");
            final boolean rewritestatements = commandLine.hasOption("rewritestatements") || !tablespacemapperfile.
                    isEmpty() || frommysqldump;
            boolean autotransaction = commandLine.hasOption("autotransaction") || frommysqldump;
            int autotransactionbatchsize = Integer.parseInt(commandLine.getOptionValue("autotransactionbatchsize",
                    100000 + ""));
            if (!autotransaction) {
                autotransactionbatchsize = 0;
            }

            String nodeId = commandLine.getOptionValue("nodeid", "");
            String param = commandLine.getOptionValue("param", "");
            String values = commandLine.getOptionValue("values", "");

            boolean listTablespaces = commandLine.hasOption("list-tablespaces");
            boolean listNodes = commandLine.hasOption("list-nodes");
            boolean showTablespace = commandLine.hasOption("show-tablespace");
            boolean listTables = commandLine.hasOption("list-tables");
            boolean showTable = commandLine.hasOption("show-table");
            if (showTable) {
                if (table.equals("")) {
                    println("Specify the table (-t <table>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
            }

            boolean createTablespace = commandLine.hasOption("create-tablespace");
            if (createTablespace) {
                if (newschema.equals("")) {
                    println("Specify the tablespace name (--newschema <schema>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (leader.equals("")) {
                    println("Specify the leader node (--newleader <nodeid>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
            }
            boolean alterTablespace = commandLine.hasOption("alter-tablespace");
            if (alterTablespace) {
                if (commandLine.getOptionValue("schema", null) == null) {
                    println("Cowardly refusing to assume the default schema in an alter command. Explicitly use \"-s " + TableSpace.DEFAULT + "\" instead");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (param.equals("")) {
                    println("Specify the parameter (--param <par>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (values.equals("")) {
                    println("Specify values (--values <vals>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }

            }
            boolean setLeader = commandLine.hasOption("set-leader");
            if (setLeader) {
                if (leader.isEmpty()) {
                    println("Specify the node (-nl <nodeid>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (commandLine.getOptionValue("schema", null) == null) {
                    println("Cowardly refusing to assume the default schema in an alter command. Explicitly use \"-s " + TableSpace.DEFAULT + "\" instead");
                    exitCode = 1;
                    System.exit(exitCode);
                }
            }
            boolean addReplica = commandLine.hasOption("add-replica");
            if (addReplica) {
                if (commandLine.getOptionValue("schema", null) == null) {
                    println("Cowardly refusing to assume the default schema in an alter command. Explicitly use \"-s " + TableSpace.DEFAULT + "\" instead");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (nodeId.equals("")) {
                    println("Specify the node (-n <nodeid>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
            }
            boolean removeReplica = commandLine.hasOption("remove-replica");
            if (removeReplica) {
                if (commandLine.getOptionValue("schema", null) == null) {
                    println("Cowardly refusing to assume the default schema in an alter command. Explicitly use \"-s " + TableSpace.DEFAULT + "\" instead");
                    exitCode = 1;
                    System.exit(exitCode);
                }
                if (nodeId.equals("")) {
                    println("Specify the node (-n <nodeid>)");
                    exitCode = 1;
                    System.exit(exitCode);
                }
            }

            TableSpaceMapper tableSpaceMapper = buildTableSpaceMapper(tablespacemapperfile);
            ZookeeperMetadataStorageManager metadataStorageManager = null;
            try (HerdDBDataSource datasource = new HerdDBDataSource()) {
                datasource.setUrl(url);
                datasource.setUsername(username);
                datasource.setPassword(password);

                try (Connection connection = datasource.getConnection();
                        Statement statement = connection.createStatement()) {

                    metadataStorageManager = buildMetadataStorageManager(datasource);

                    connection.setSchema(schema);
                    if (sqlconsole) {
                        runSqlConsole(connection, statement, PRETTY_PRINT);
                    } else if (backup) {
                        performBackup(statement, schema, file, options, connection, dumpfetchsize);
                    } else if (restore) {
                        performRestore(file, leader, newschema, options, statement, connection);
                    } else if (!query.isEmpty()) {
                        executeStatement(verbose, ignoreerrors, false, false, query, statement, tableSpaceMapper, false,
                                PRETTY_PRINT);
                    } else if (!file.isEmpty()) {
                        executeSqlFile(autotransactionbatchsize, connection, file, verbose, async,
                                ignoreerrors, frommysqldump, rewritestatements,
                                statement, tableSpaceMapper, PRETTY_PRINT, filter,
                                datasource);
                    } else if (!script.isEmpty()) {
                        executeScript(connection, datasource, statement, script);
                    } else if (listTablespaces) {
                        printTableSpaces(verbose, ignoreerrors, statement, tableSpaceMapper, metadataStorageManager);
                    } else if (listNodes) {
                        printNodes(verbose, ignoreerrors, statement, tableSpaceMapper, metadataStorageManager);
                    } else if (showTablespace) {
                        printTableSpaceInfos(verbose, ignoreerrors, statement, tableSpaceMapper, schema,
                                metadataStorageManager);
                    } else if (listTables) {
                        listTables(verbose, ignoreerrors, statement, tableSpaceMapper, schema);
                    } else if (showTable) {
                        printTableInfos(verbose, ignoreerrors, statement, tableSpaceMapper, schema, table);
                    } else if (setLeader) {
                        setLeader(metadataStorageManager, schema, leader);
                    } else if (addReplica) {
                        changeReplica(metadataStorageManager, schema, nodeId, ChangeReplicaAction.ADD);
                    } else if (removeReplica) {
                        changeReplica(metadataStorageManager, schema, nodeId, ChangeReplicaAction.REMOVE);
                    } else if (createTablespace) {
                        createTablespace(verbose, ignoreerrors, statement, tableSpaceMapper, newschema, leader);
                    } else if (alterTablespace) {
                        alterTablespace(metadataStorageManager, schema, param, values);
                    } else {
                        failAndPrintHelp(options);
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
            } finally {
                if (metadataStorageManager != null) {
                    try {
                        metadataStorageManager.close();
                    } catch (MetadataStorageManagerException ex) {

                    }
                }
            }
        } finally {
            System.exit(exitCode);
        }
    }

    private static boolean checkNodeExistence(
            ZookeeperMetadataStorageManager metadataStorageManager,
            String nodeId
    ) throws MetadataStorageManagerException {
        return metadataStorageManager.listNodes().stream().anyMatch(n -> n.nodeId.equals(nodeId));
    }

    private static boolean checkNodeExistence(
            boolean verbose, boolean ignoreerrors, Statement statement, TableSpaceMapper tableSpaceMapper,
            String nodeId
    ) throws SQLException, ScriptException {

        ExecuteStatementResult check = executeStatement(verbose, ignoreerrors, false, false,
                "select * from sysnodes where nodeid='" + nodeId + "'", statement, tableSpaceMapper, true, false);
        return !check.results.isEmpty();
    }

    private static void createTablespace(
            boolean verbose, boolean ignoreerrors, Statement statement, TableSpaceMapper tableSpaceMapper,
            String newschema, String leader
    ) throws SQLException, ScriptException {

        if (!checkNodeExistence(verbose, ignoreerrors, statement, tableSpaceMapper, leader)) {
            println("Unknown node " + leader);
            exitCode = 1;
            System.exit(exitCode);
        }

        ExecuteStatementResult res = executeStatement(verbose, ignoreerrors, false, false,
                "CREATE TABLESPACE '" + newschema + "','leader:" + leader + "'", statement, tableSpaceMapper, true,
                false);

        if (res != null && res.updateCount > 0) {
            println("Successfully created " + newschema + " tablespace");
        }
    }

    private static void alterTablespace(
            ZookeeperMetadataStorageManager clusterManager,
            String schema, String param, String values
    ) throws SQLException, ScriptException, MetadataStorageManagerException {

        if (clusterManager == null) {
            println("You are not managing a cluster. This command cannot be used");
            exitCode = 1;
            System.exit(exitCode);
            return;
        }
        TableSpace tableSpace = clusterManager.describeTableSpace(schema);
        if (tableSpace == null) {
            println("Cannot find tablespace " + schema);
            exitCode = 1;
            System.exit(exitCode);
            return;
        }

        TableSpace.Builder newMetadata = TableSpace
                .builder()
                .cloning(tableSpace);
        switch (param) {
            case "expectedreplicacount":
                int expectedreplicacount = Integer.parseInt(values);
                if (expectedreplicacount < 0 || expectedreplicacount > 10) {
                    println("Bad value for parameter " + param);
                    exitCode = 1;
                    System.exit(exitCode);
                }
                newMetadata.expectedReplicaCount(expectedreplicacount);
                break;
            case "maxleaderinactivitytime":
                int maxleaderinactivitytime = Integer.parseInt(values);
                if (maxleaderinactivitytime < 0) {
                    println("Bad value for parameter " + param);
                    exitCode = 1;
                    System.exit(exitCode);
                }
                newMetadata.maxLeaderInactivityTime(maxleaderinactivitytime);
                break;
            default:
                println("Bad parameter " + param + ", only 'expectedreplicacount' and 'maxleaderinactivitytime' are supported from this interface.");
                exitCode = 1;
                System.exit(exitCode);
                return;
        }
        boolean ok = clusterManager.updateTableSpace(newMetadata.build(), tableSpace);
        if (!ok) {
            println("Failed to alter " + schema + " tablespace");
        } else {
            println("Successfully altered " + schema + " tablespace");
        }
    }

    private static void describeRawLedger(long ledgerId, long fromId, long toId, HerdDBDataSource datasource) throws Exception {

        if (!datasource.getUrl().contains(":zookeeper:")) {
            //not cluster
            System.out.println("Not a cluster URL: " + datasource.getUrl());
            return;
        }
        datasource.getConnection().close(); // start embedded client
         ClientConfiguration configuration = datasource.getClient().getConfiguration();
        try (ZookeeperMetadataStorageManager zk = buildMetadataStorageManager(datasource);) {
            BookkeeperCommitLogManager.scanRawLedger(ledgerId, fromId, toId, configuration, zk,
                    (BookkeeperCommitLogManager.LogEntryWithSequenceNumber nextEntry) -> {
                        println(nextEntry.logSequenceNumber.ledgerId + "," + nextEntry.logSequenceNumber.offset + ","
                                + nextEntry.entry.toString());
                    });
        }
    }

    private static void describeRawFile(String tablespaceuuid, String tableName, String tablesmetadatafile,
                                        String rawfile, String mode) throws Exception {
        Path path = Paths.get(rawfile);
        switch (mode) {
            case "txlog": {
                try (FileCommitLog.CommitFileReader reader = CommitFileReader.openForDescribeRawfile(path)) {
                    LogEntryWithSequenceNumber nextEntry = reader.nextEntry();
                    while (nextEntry != null) {
                        println(nextEntry.logSequenceNumber.ledgerId + "," + nextEntry.logSequenceNumber.offset + "," + nextEntry.entry.
                                toString());
                        nextEntry = reader.nextEntry();
                    }
                }
                break;
            }
            case "datapage": {
                if (tablesmetadatafile.isEmpty()) {
                    throw new IllegalArgumentException("metadatafile option is required in order to analize a datapage");
                }
                if (tableName.isEmpty()) {
                    throw new IllegalArgumentException("table option is required in order to analize a database");
                }
                if (tablespaceuuid.isEmpty()) {
                    throw new IllegalArgumentException(
                            "tablespaceuuid option is required in order to analize a database");
                }
                Path pathtablesmetadata = Paths.get(tablesmetadatafile);
                List<Table> tables = FileDataStorageManager.readTablespaceStructure(pathtablesmetadata, tablespaceuuid,
                        null);
                println("File " + pathtablesmetadata.getFileName() + " contains the following table schematas:");
                for (Table t : tables) {
                    println("Table: " + t.uuid + " - " + t.tablespace + "." + t.name);
                }
                Table table = tables.stream().filter(t -> t.name.equals(tableName)).findAny().orElse(null);
                if (table == null) {
                    println("No such table " + tableName);
                    return;
                }
                List<Record> records = FileDataStorageManager.rawReadDataPage(path);
                for (Record record : records) {
                    StringBuilder line = new StringBuilder();
                    DataAccessor dataAccessor = record.getDataAccessor(table);
                    for (int i = 0; i < table.columns.length; i++) {
                        Object value = dataAccessor.get(i);
                        if (i > 0) {
                            line.append(',');
                        }
                        line.append(value);
                    }
                    println(line);
                }
                break;
            }
            case "tablecheckpoint": {
                TableStatus tableStatus = FileDataStorageManager.readTableStatusFromFile(path);
                println("TableName:" + tableStatus.tableName);
                println("Sequence Number:" + tableStatus.sequenceNumber.ledgerId + ", " + tableStatus.sequenceNumber.offset);
                println("Next Page Id:" + tableStatus.nextPageId);
                println("Next Primary key value:" + (tableStatus.nextPrimaryKeyValue != null ? Bytes.from_array(
                        tableStatus.nextPrimaryKeyValue) : "null"));
                println("Active pages:" + tableStatus.activePages);
                break;
            }
            case "indexcheckpoint": {
                IndexStatus indexStatus = FileDataStorageManager.readIndexStatusFromFile(path);
                println("IndexName:" + indexStatus.indexName);
                println("Sequence Number:" + indexStatus.sequenceNumber.ledgerId + ", " + indexStatus.sequenceNumber.offset);
                println("Active pages:" + indexStatus.activePages);
                try {
                    BLinkMetadata<Bytes> blinkMetadata = MetadataSerializer.INSTANCE.read(indexStatus.indexData);
                    println("BLink Metadata: " + blinkMetadata);
                    println("BLink Metadata nodex: " + blinkMetadata.nodesToStrings());
                } catch (IOException err) {
                }
                break;
            }
            case "tablespacemetadata": {
                TableSpace tableSpace = FileMetadataStorageManager.readTableSpaceMetadataFile(path);
                println("Name:" + tableSpace.name);
                println("UUID:" + tableSpace.uuid);
                println("Leader:" + tableSpace.leaderId);
                break;
            }
            case "tablesmetadata": {
                if (tablespaceuuid.isEmpty()) {
                    throw new IllegalArgumentException(
                            "tablespaceuuid option is required in order to analize a database");
                }
                List<Table> tables = FileDataStorageManager.readTablespaceStructure(path, tablespaceuuid, null);
                for (Table table : tables) {
                    println("Table");
                    println("Name: " + table.name);
                    println("Tablespace: " + table.tablespace);
                    println("Table UUID: " + table.uuid);
                    for (Column c : table.columns) {
                        println("Column : " + c.name + ", serialPosition: " + c.serialPosition + ", type " + ColumnTypes.
                                typeToString(c.type) + " (" + c.type + ")");
                    }
                }
                break;
            }
            default:
                System.out.println(
                        "Unknown file type " + mode + " valid options are txlog, datapage, tablecheckpoint, indexcheckpoint, tablesmetadata");
        }
    }

    private static boolean isDDL(String uppercase) {
        return uppercase.startsWith("CREATE")
                || uppercase.startsWith("DROP")
                || uppercase.startsWith("LOCK")
                || uppercase.startsWith("UNLOCK");
    }

    private static ZookeeperMetadataStorageManager buildMetadataStorageManager(HerdDBDataSource datasource) throws MetadataStorageManagerException {
        ClientConfiguration configuration = datasource.getClient().getConfiguration();
        if (!datasource.getUrl().contains(":zookeeper:")) {
            //not cluster
            return null;
        }
        String zkAddress = configuration.getString(PROPERTY_ZOOKEEPER_ADDRESS, PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        String zkPath = configuration.getString(PROPERTY_ZOOKEEPER_PATH, PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        int sessionTimeout = configuration.getInt(PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 60000);
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, zkPath);
        zk.start(false /*
         * formatIfNeeded
         */);
        return zk;
    }

    private enum ChangeReplicaAction {
        ADD, REMOVE
    }

    private static void setLeader(ZookeeperMetadataStorageManager clusterManager, String schema, String nodeId) throws SQLException, ScriptException, MetadataStorageManagerException {
        if (clusterManager == null) {
            println("You are not managing a cluster. This command cannot be used");
            exitCode = 1;
            System.exit(exitCode);
            return;
        }
        if (!checkNodeExistence(clusterManager, nodeId)) {
            println("Unknown node " + nodeId);
            exitCode = 1;
            System.exit(exitCode);
            return;
        }
        TableSpace tableSpace = clusterManager.describeTableSpace(schema);
        if (tableSpace == null) {
            println("Cannot find tablespace " + schema);
            exitCode = 1;
            System.exit(exitCode);
            return;
        }
        if (!tableSpace.replicas.contains(nodeId)) {
            println("Node " + nodeId + " is not in replica list: " + tableSpace.replicas);
            exitCode = 1;
            System.exit(exitCode);
            return;
        }
        TableSpace.Builder newMetadata = TableSpace
                .builder()
                .cloning(tableSpace)
                .leader(nodeId);
        boolean ok = clusterManager.updateTableSpace(newMetadata.build(), tableSpace);
        if (!ok) {
            println("Failed to alter " + schema + " tablespace");
        } else {
            println("Successfully altered " + schema + " tablespace");
        }
    }

    private static void changeReplica(
            ZookeeperMetadataStorageManager clusterManager, String schema, String nodeId, ChangeReplicaAction action
    ) throws SQLException, ScriptException, MetadataStorageManagerException {
        if (clusterManager == null) {
            println("Not in cluster mode!");
            exitCode = 1;
            System.exit(exitCode);
        }
        if (!checkNodeExistence(clusterManager, nodeId)) {
            println("Unknown node " + nodeId);
            exitCode = 1;
            System.exit(exitCode);
        }

        TableSpace tableSpace = clusterManager.describeTableSpace(schema);
        if (tableSpace == null) {
            println("Unknown tablespace " + schema);
            exitCode = 1;
            System.exit(exitCode);
        }
        TableSpace.Builder newMetadata = TableSpace.builder().cloning(tableSpace);
        switch (action) {
            case ADD:
                if (tableSpace.replicas.contains(nodeId)) {
                    println("Node " + nodeId + " is already a replica for tablespace " + schema);
                    exitCode = 1;
                    System.exit(exitCode);
                }
                newMetadata.replica(nodeId);
                break;
            case REMOVE:
                if (!tableSpace.replicas.contains(nodeId)) {
                    println("Node " + nodeId + " is not a replica for tablespace " + schema);
                    exitCode = 1;
                    System.exit(exitCode);
                }
                Set<String> copy = new HashSet<>(tableSpace.replicas);
                copy.remove(nodeId);
                newMetadata.replicas(copy);
                break;
        }

        boolean ok = clusterManager.updateTableSpace(newMetadata.build(), tableSpace);
        if (!ok) {
            println("Failed to alter " + schema + " tablespace");
        } else {
            println("Successfully altered " + schema + " tablespace");
        }
    }

    private static void printTableSpaces(
            boolean verbose, boolean ignoreerrors, Statement statement,
            TableSpaceMapper tableSpaceMapper, ZookeeperMetadataStorageManager clusterManager
    ) throws SQLException, ScriptException, MetadataStorageManagerException {

        if (clusterManager != null) {
            Collection<String> tablespaces = clusterManager.listTableSpaces();

            println(" Tablespaces:");
            println("");
            for (String row : tablespaces) {
                println("   " + row);
            }
            println("");
        } else {
            ExecuteStatementResult tablespaces = executeStatement(verbose, ignoreerrors, false, false,
                    "select * from systablespaces", statement, tableSpaceMapper, true, false);

            println("");
            if (tablespaces == null || tablespaces.results.isEmpty()) {
                throw new RuntimeException("Impossibile");
            }
            println(" Tablespaces:");
            println("");
            for (Map<String, Object> row : tablespaces.results) {
                println("   " + row.get("tablespace_name"));
            }
            println("");
        }
    }

    private static void printNodes(
            boolean verbose, boolean ignoreerrors, Statement statement, TableSpaceMapper tableSpaceMapper,
            ZookeeperMetadataStorageManager clusterManager
    ) throws SQLException, ScriptException, MetadataStorageManagerException {
        if (clusterManager != null) {
            println("");
            println(" Nodes:");
            println("");
            List<NodeMetadata> listNodes = clusterManager.listNodes();
            if (listNodes.isEmpty()) {
                println("   No nodes to show");
            }
            for (NodeMetadata row : listNodes) {
                println("   Node: " + row.nodeId);
                println("   Address: " + row.host + ":" + row.port);
                println("   SSL: " + row.ssl);
                println("");
            }
        } else {
            ExecuteStatementResult nodes = executeStatement(verbose, ignoreerrors, false, false,
                    "select * from sysnodes", statement, tableSpaceMapper, true, false);

            println("");
            println(" Nodes:");
            println("");
            if (nodes.results.isEmpty()) {
                println("   No nodes to show");
            }
            for (Map<String, Object> row : nodes.results) {
                println("   Node: " + row.get("nodeid"));
                println("   Address: " + row.get("address"));
                println("   SSL: " + row.get("ssl"));
                println("");
            }
        }

    }

    private static void printTableSpaceInfos(
            boolean verbose, boolean ignoreerrors, Statement statement, TableSpaceMapper tableSpaceMapper, String schema,
            ZookeeperMetadataStorageManager clusterManager
    ) throws SQLException, ScriptException, MetadataStorageManagerException {
        if (clusterManager != null) {
            TableSpace tablespace = clusterManager.describeTableSpace(schema);
            if (tablespace == null) {
                println("Unknown tablespace " + schema);
                exitCode = 1;
                System.exit(exitCode);
            }

            List<TableSpaceReplicaState> nodes =
                    clusterManager.getTableSpaceReplicaState(tablespace.uuid);

            println(" Tablespace: " + tablespace.name);
            println(" Leader node: " + tablespace.leaderId);
            println(" Replication nodes: " + tablespace.replicas);
            println(" Expected replica count: " + tablespace.expectedReplicaCount);
            println(" Max leader inactivity time: " + (tablespace.maxLeaderInactivityTime / 1000) + "s");
            println(" UUID: " + tablespace.uuid);

            if (nodes != null) {

                println("");
                println(" Replication nodes (systablespacereplicastate):");

                if (nodes.isEmpty()) {
                    println("");
                    println("   No nodes to show");
                }
                for (TableSpaceReplicaState node : nodes) {
                    println("");
                    if (!tablespace.replicas.contains(node.nodeId)) {
                        println("   Node ID: " + node.nodeId + " (no more in replica list)");
                    } else {
                        println("   Node ID: " + node.nodeId);
                    }
                    println("   Mode: " + node.mode);
                    println("   Last activity: " + new java.sql.Timestamp(node.timestamp));
                    println("   Inactivity time: " + (Float.valueOf(System.currentTimeMillis() - node.timestamp) / 1000) + "s");
                }
            }

            println("");
        } else {
            ExecuteStatementResult tablespace = executeStatement(verbose, ignoreerrors, false, false,
                    "select * from systablespaces where tablespace_name='" + schema + "'", statement, tableSpaceMapper,
                    true, false);
            ExecuteStatementResult nodes = executeStatement(verbose, ignoreerrors, false, false,
                    "select * from systablespacereplicastate where tablespace_name='" + schema + "'", statement,
                    tableSpaceMapper, true, false);
            if (tablespace.results.isEmpty()) {
                println("Unknown tablespace " + schema);
                exitCode = 1;
                System.exit(exitCode);
            }

            println("");

            Map<String, Object> ts = tablespace.results.get(0);
            println(" Tablespace: " + ts.get("tablespace_name"));
            println(" Leader node: " + ts.get("leader"));
            println(" Replication nodes: " + ts.get("replica"));
            println(" Expected replica count: " + ts.get("expectedreplicacount"));
            println(" Max leader inactivity time: " + (Float.valueOf((String) ts.get("maxleaderinactivitytime")) / 1000)
                    + "s");
            println(" UUID: " + ts.get("uuid"));

            if (nodes != null) {

                println("");
                println(" Replication nodes (systablespacereplicastate):");

                if (nodes.results.isEmpty()) {
                    println("");
                    println("   No nodes to show");
                }
                for (Map<String, Object> node : nodes.results) {
                    println("");
                    println("   Node ID: " + node.get("nodeid"));
                    println("   Mode: " + node.get("mode"));
                    println("   Last activity: " + node.get("timestamp"));
                    println("   Inactivity time: " + (Float.valueOf((String) node.get("inactivitytime")) / 1000) + "s");
                    println("   Max leader inactivity time: " + (Float.valueOf((String) node.get(
                            "maxleaderinactivitytime")) / 1000) + "s");
                }
            }

            println("");
        }
    }

    private static void listTables(boolean verbose, boolean ignoreerrors, Statement statement,
                                   TableSpaceMapper tableSpaceMapper, String schema) throws SQLException, ScriptException {

        println("");
        println(" Tables in tablespace " + schema + ":");
        ExecuteStatementResult tables = executeStatement(verbose, ignoreerrors, false, false,
                "select * from systablestats", statement, tableSpaceMapper, true, false);
        if (tables.results.isEmpty()) {
            println("");
            println("   No tables found");
        } else {
            executeStatement(verbose, ignoreerrors, false, false, "select * from systablestats", statement,
                    tableSpaceMapper, false, true);
        }
        println("");

    }

    private static void printTableInfos(boolean verbose, boolean ignoreerrors, Statement statement,
                                        TableSpaceMapper tableSpaceMapper, String schema, String table) throws SQLException, ScriptException {
        ExecuteStatementResult stats = executeStatement(verbose, ignoreerrors, false, false,
                "select * from systablestats where table_name = '" + table + "'", statement, tableSpaceMapper, true,
                false);

        if (stats.results.isEmpty()) {
            println("\n No table " + table + " in tablespace " + schema + "\n");
            return;
        }

        println("");
        println(" Table " + schema + "." + table + ":");
        println("");
        for (Entry<String, Object> entry : stats.results.get(0).entrySet()) {
            println("    " + entry.getKey() + ": " + entry.getValue());
        }
        println("");
        println(" Columns: ");
        executeStatement(verbose, ignoreerrors, false, false,
                "select * from syscolumns where table_name = '" + table + "'", statement, tableSpaceMapper, false, true);

    }

    private static void executeScript(final Connection connection, final HerdDBDataSource datasource,
                                      final Statement statement, String script) throws IOException, CompilationFailedException {
        Map<String, Object> variables = new HashMap<>();
        variables.put("connection", connection);
        variables.put("datasource", datasource);
        variables.put("statement", statement);
        GroovyShell shell = new GroovyShell(new Binding(variables));
        shell.evaluate(new File(script));
    }

    private static void executeSqlFile(
            int autotransactionbatchsize, final Connection connection, String file, final boolean verbose,
            final boolean async, final boolean ignoreerrors, final boolean frommysqldump,
            final boolean rewritestatements,
            final Statement statement, TableSpaceMapper tableSpaceMapper, boolean pretty, String filter,
            final HerdDBDataSource datasource
    ) throws Exception {
        if (autotransactionbatchsize > 0) {
            connection.setAutoCommit(false);
        }
        long _start = System.currentTimeMillis();

        final IntHolder totalDoneCount = new IntHolder();
        File f = new File(file);
        long fileSize = f.length();
        boolean allowDDL = filter.equals("all") || filter.equals("ddl");
        boolean allowDML = filter.equals("all") || filter.equals("dml");
        SqlFileStatus fileStatus = new SqlFileStatus(verbose, async, ignoreerrors,
                frommysqldump, rewritestatements, pretty, tableSpaceMapper, datasource);
        try (FileInputStream rawStream = new FileInputStream(file);
                BufferedInputStream buffer = new BufferedInputStream(rawStream);
                CounterInputStream counter = new CounterInputStream(buffer);
                InputStream fIn = wrapStream(f.getName(), counter);
                CounterInputStream counterUnzipped = new CounterInputStream(fIn);
                InputStreamReader ii = new InputStreamReader(counterUnzipped, StandardCharsets.UTF_8)) {
            int _autotransactionbatchsize = autotransactionbatchsize;
            SQLFileParser.parseSQLFile(ii, (st) -> {
                if (!st.comment) {
                    boolean isDDL = isDDL(st.content.toUpperCase());
                    int totalBefore = fileStatus.executedOperations;
                    if ((allowDDL && isDDL) || (allowDML && !isDDL)) {
                        executeStatementInSqlFile(st.content, statement, fileStatus);
                    }
                    int count = fileStatus.executedOperations - totalBefore;

                    if (_autotransactionbatchsize > 0 && fileStatus.pendingOperations > _autotransactionbatchsize) {
                        totalDoneCount.value += fileStatus.pendingOperations;
                        fileStatus.flushAndCommit(connection);
                        long _now = System.currentTimeMillis();
                        long countZipped = counter.count;
                        int percent = (int) (counter.count * 100.0 / fileSize);
                        long delta = (_now - _start);
                        long countUnzipped = counterUnzipped.count;
                        double speed = ((countUnzipped * 60000.0) / (1.0 * delta));

                        double speedZipped = ((countZipped * 60000.0) / (1.0 * delta));

                        if (countUnzipped != counter.count) {
                            System.out.println(new java.sql.Timestamp(System.currentTimeMillis())
                                    + " COMMIT after " + totalDoneCount.value + " ops, read " + formatBytes(
                                            counter.count) + " (" + formatBytes(countUnzipped) + " unzipped) over " + formatBytes(
                                    fileSize) + ". " + percent + "%, " + formatBytes(speedZipped) + "/min (UNZIPPED " + formatBytes(
                                    speed) + "/min)");
                        } else {
                            System.out.println(new java.sql.Timestamp(System.currentTimeMillis())
                                    + " COMMIT after " + totalDoneCount.value + " ops, read " + formatBytes(
                                            counter.count) + " over " + formatBytes(fileSize) + ". " + percent + "%, " + formatBytes(
                                    speed) + " /min");
                        }
                    }
                }
            });
        }
        if (!connection.getAutoCommit()) {
            totalDoneCount.value += fileStatus.pendingOperations;
            fileStatus.flushAndCommit(connection);
        }
        System.out.println(
                "Import completed, " + totalDoneCount.value + " ops, in " + ((System.currentTimeMillis() - _start) / 60000) + " minutes");

    }

    private static void performRestore(String file, String leader, String newschema, Options options,
                                       final Statement statement, final Connection connection) throws Exception {
        if (file.isEmpty()) {
            println("Please provide --file option");
            failAndPrintHelp(options);
            return;
        }
        Path inputfile = Paths.get(file).toAbsolutePath();
        if (leader.isEmpty() || newschema.isEmpty()) {
            println("options 'newleader' and 'newschema' are required");
            failAndPrintHelp(options);
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
        try (InputStream fin = wrapStream(file, Files.newInputStream(inputfile));
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
    }

    private static void failAndPrintHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("herddb", options, true);
        exitCode = 1;
    }

    private static void performBackup(final Statement statement, String schema, String file, Options options,
                                      final Connection connection, int dumpfetchsize) throws Exception {
        if (file.isEmpty()) {
            println("Please provide --file option");
            failAndPrintHelp(options);
            return;
        }
        if (schema.equals("*")) {
            connection.setSchema(TableSpace.DEFAULT);
            List<String> tablespacesToDump = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery("SELECT tablespace_name FROM systablespaces")) {
                while (rs.next()) {
                    String tablename = rs.getString(1).toLowerCase();
                    tablespacesToDump.add(tablename);
                }
            }
            for (String tableSpace : tablespacesToDump) {
                backupTableSpace(statement, tableSpace, file, tableSpace, connection, dumpfetchsize);
            }
        } else {
            backupTableSpace(statement, schema, file, null, connection, dumpfetchsize);
        }
    }

    private static void backupTableSpace(final Statement statement, String schema, String file, String suffix,
                                         final Connection connection, int dumpfetchsize) throws Exception {
        List<String> tablesToDump = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery("SELECT table_name"
                + " FROM " + schema + ".systables"
                + " WHERE systemtable='false'")) {
            while (rs.next()) {
                String tablename = rs.getString(1).toLowerCase();
                tablesToDump.add(tablename);
            }
        }
        int dot = file.lastIndexOf('.');
        String ext = "";
        if (dot >= 0) {
            ext = file.substring(dot);
            file = file.substring(0, dot);
        }
        String finalFile = (suffix == null ? file : file + suffix) + ext;
        Path outputfile = Paths.get(finalFile).toAbsolutePath();
        println("Backup tables " + tablesToDump + " from tablespace " + schema + " to " + outputfile);

        try (OutputStream fout = wrapOutputStream(Files.newOutputStream(outputfile, StandardOpenOption.CREATE_NEW), ext);
                SimpleBufferedOutputStream oo = new SimpleBufferedOutputStream(fout, 16 * 1024 * 1024)) {
            HerdDBConnection hcon = connection.unwrap(HerdDBConnection.class);
            HDBConnection hdbconnection = hcon.getConnection();
            BackupUtils.dumpTableSpace(schema, dumpfetchsize, hdbconnection, oo, new ProgressListener() {
                @Override
                public void log(String actionType, String message, Map<String, Object> context) {
                    println(message);
                }

            });
        }
        println("Backup finished for tablespace " + schema);
    }

    private static ExecuteStatementResult executeStatement(
            boolean verbose, boolean ignoreerrors, boolean frommysqldump, boolean rewritestatements, String query,
            final Statement statement,
            final TableSpaceMapper tableSpaceMapper, boolean getResults, boolean prettyPrint
    ) throws SQLException, ScriptException {
        query = query.trim();

        if (query.isEmpty()
                || query.startsWith("--")) {
            return null;
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
            return null;
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
                    return null;
            }
        }
        if (verbose) {
            System.out.println("Executing query:" + query);
        }
        try {
            if (setAutoCommit != null) {
                statement.getConnection().setAutoCommit(setAutoCommit);
                System.out.println("Set autocommit=" + setAutoCommit + " executed.");
                return null;
            }
            if (formattedQuery.equals("commit")) {
                statement.getConnection().commit();
                System.out.println("Commit executed.");
                return null;
            }
            if (formattedQuery.equals("rollback")) {
                statement.getConnection().rollback();
                System.out.println("Rollback executed.");
                return null;
            }

            QueryWithParameters rewritten = null;
            if (rewritestatements) {
                rewritten = rewriteQuery(query, tableSpaceMapper, frommysqldump);
            }
            if (rewritten != null) {
                if (rewritten.schema != null) {
                    HerdDBConnection connection = statement.getConnection().unwrap(HerdDBConnection.class);
                    if (connection != null && !connection.getSchema().equalsIgnoreCase(rewritten.schema)) {
                        commitAndChangeSchema(connection, rewritten.schema);
                    }
                }
                try (PreparedStatement ps = statement.getConnection().prepareStatement(rewritten.query)) {
                    int i = 1;
                    for (Object o : rewritten.jdbcParameters) {
                        ps.setObject(i++, o);
                    }
                    boolean resultSet = ps.execute();
                    return reallyExecuteStatement(ps, resultSet, verbose, getResults, prettyPrint);
                }
            } else {
                boolean resultSet = statement.execute(query);
                return reallyExecuteStatement(statement, resultSet, verbose, getResults, prettyPrint);
            }
        } catch (SQLException err) {
            if (ignoreerrors) {
                println("ERROR:" + err);
                return null;
            } else {
                throw err;
            }
        }
    }

    private static class SqlFileStatus {

        // LinkedHashMap preserve the order
        // this is important because CREATE TABLE must precede INSERTs on the same table
        private Map<String, PreparedStatement> currentStatements = new LinkedHashMap<>();
        private HerdDBDataSource datasource;
        private int pendingOperations;
        private int executedOperations;
        final boolean verbose;
        final boolean async;
        final boolean ignoreerrors;
        final boolean frommysqldump;
        final boolean rewritestatements;
        final boolean prettyPrint;
        final TableSpaceMapper tableSpaceMapper;

        public SqlFileStatus(
                boolean verbose, boolean async, boolean ignoreerrors,
                boolean frommysqldump, boolean rewritestatements, boolean prettyPrint,
                TableSpaceMapper tableSpaceMapper, HerdDBDataSource datasource
        ) {
            this.verbose = verbose;
            this.async = async;
            this.ignoreerrors = ignoreerrors;
            this.frommysqldump = frommysqldump;
            this.rewritestatements = rewritestatements;
            this.prettyPrint = prettyPrint;
            this.tableSpaceMapper = tableSpaceMapper;
            this.datasource = datasource;
        }

        private void flushAndCommit(Connection connection) throws SQLException {
            flush();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }

        private void rollback(Connection connection) throws SQLException {
            if (verbose) {
                System.out.println("dropping " + currentStatements.size() + " statements on rollback");
            }
            for (PreparedStatement ps : currentStatements.values()) {
                ps.close();
            }
            currentStatements.clear();
            pendingOperations = 0;
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        }

        private void flush() throws SQLException {
            if (verbose) {
                System.out.println(
                        "flushing " + currentStatements.size() + " statements, with " + pendingOperations + " pending ops");
            }
            List<CompletableFuture<?>> futures = new ArrayList<>();
            for (Map.Entry<String, PreparedStatement> entry : currentStatements.entrySet()) {
                if (verbose) {
                    System.out.println("flushing " + entry.getKey());
                }
                PreparedStatement ps = entry.getValue();
                if (async) {
                    CompletableFuture<?> future = ps.unwrap(PreparedStatementAsync.class)
                            .executeBatchAsync();

                    future.whenComplete((res, error) -> {
                        if (error != null) {
                            error.printStackTrace();
                        }
                        try {
                            // we are creating one connection per PreparedStatement
                            // in async mode
                            Connection psCon = ps.getConnection();
                            if (!psCon.getAutoCommit()) {
                                psCon.commit();
                            }
                            ps.close();
                            psCon.close();
                        } catch (SQLException err) {
                            throw new RuntimeException(err);
                        }
                    });

                } else {
                    ps.executeBatch();
                    ps.close();
                }
            }
            if (async) {
                for (CompletableFuture<?> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException err) {
                        if (err.getCause() instanceof SQLException) {
                            throw (SQLException) err.getCause();
                        } else {
                            throw new SQLException(err.getCause());
                        }
                    } catch (InterruptedException err) {
                        throw new SQLException(err);
                    }
                }
            }
            currentStatements.clear();
            executedOperations += pendingOperations;
            pendingOperations = 0;

        }

        @SuppressFBWarnings(value = "ODR_OPEN_DATABASE_RESOURCE",
                justification = "Connection will be closed in async mode while closing PreparedStatement")
        private PreparedStatement prepareStatement(Connection connection, String query) throws SQLException {
            PreparedStatement ps = currentStatements.get(query);
            if (ps != null) {
                return ps;
            }
            if (async) {
                // in asyc mode we have to create a new connection for each statement
                Connection newConnection = datasource.getConnection();
                newConnection.setAutoCommit(false);
                ps = newConnection.prepareStatement(query);
            } else {
                ps = connection.prepareStatement(query);
            }
            currentStatements.put(query, ps);
            return ps;
        }

        private void countPendingOp() {
            pendingOperations++;
        }

    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    private static void executeStatementInSqlFile(
            String query, final Statement statement, SqlFileStatus sqlFileStatus
    ) throws SQLException, ScriptException {
        query = query.trim();

        if (query.isEmpty()
                || query.startsWith("--")) {
            return;
        }
        String formattedQuery = query.toLowerCase();
        if (formattedQuery.endsWith(";")) {
            // mysqldump
            formattedQuery = formattedQuery.substring(0, formattedQuery.length() - 1);
        }
        if (formattedQuery.equals("exit") || formattedQuery.equals("quit")) {
            throw new SQLException("explicit END of script with '" + formattedQuery + "'");
        }
        if (sqlFileStatus.frommysqldump && (formattedQuery.startsWith("lock tables") || formattedQuery.startsWith(
                "unlock tables"))) {
            // mysqldump
            return;
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
        if (sqlFileStatus.verbose) {
            System.out.println("Executing query:" + query);
        }
        try {
            if (setAutoCommit != null) {
                statement.getConnection().setAutoCommit(setAutoCommit);
                System.out.println("Set autocommit=" + setAutoCommit + " executed.");
                return;
            }
            if (formattedQuery.equals("commit")) {
                sqlFileStatus.flushAndCommit(statement.getConnection());

                System.out.println("Commit executed.");
                return;
            }
            if (formattedQuery.equals("rollback")) {
                sqlFileStatus.rollback(statement.getConnection());
                statement.getConnection().rollback();
                System.out.println("Rollback executed.");
                return;
            }

            QueryWithParameters rewritten = null;
            if (sqlFileStatus.rewritestatements) {
                rewritten = rewriteQuery(query, sqlFileStatus.tableSpaceMapper, sqlFileStatus.frommysqldump);
            }
            if (rewritten != null) {
                if (rewritten.schema != null) {
                    HerdDBConnection connection = statement.getConnection().unwrap(HerdDBConnection.class);
                    if (connection != null && !connection.getSchema().equalsIgnoreCase(rewritten.schema)) {
                        sqlFileStatus.flushAndCommit(connection);
                        commitAndChangeSchema(connection, rewritten.schema);
                    }
                }
                PreparedStatement ps = sqlFileStatus.prepareStatement(statement.getConnection(), rewritten.query);
                int i = 1;
                for (Object o : rewritten.jdbcParameters) {
                    ps.setObject(i++, o);
                }
                ps.addBatch();
            } else {
                PreparedStatement ps = sqlFileStatus.prepareStatement(statement.getConnection(), query);
                ps.addBatch();
            }
            sqlFileStatus.countPendingOp();
        } catch (SQLException err) {
            if (sqlFileStatus.ignoreerrors) {
                println("ERROR:" + err);
                return;
            } else {
                throw err;
            }
        }
    }

    private static class ExecuteStatementResult {

        public final boolean update;
        public final int updateCount;
        public final List<Map<String, Object>> results;

        public ExecuteStatementResult(int updateCount) {
            this.update = true;
            this.updateCount = updateCount;
            this.results = null;
        }

        public ExecuteStatementResult(List<Map<String, Object>> results) {
            this.update = false;
            this.updateCount = 0;
            this.results = results;
        }
    }

    private static ExecuteStatementResult reallyExecuteStatement(final Statement statement, boolean resultSet,
                                                                 boolean verbose, boolean getResults,
                                                                 boolean prettyPrint) throws SQLException {

        if (resultSet) {
            try (ResultSet rs = statement.getResultSet()) {
                List<Map<String, Object>> results = new ArrayList<>();
                TextTableBuilder tb = new TextTableBuilder();

                ResultSetMetaData md = rs.getMetaData();
                List<String> columns = new ArrayList<>();
                int ccount = md.getColumnCount();
                for (int i = 1; i <= ccount; i++) {
                    columns.add(md.getColumnName(i));
                }

                if (!getResults) {
                    if (prettyPrint) {
                        tb.addIntestation(columns);
                    } else {
                        System.out.println(columns.stream().collect(Collectors.joining(";")));
                    }
                }

                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= ccount; i++) {
                        String value = rs.getString(i);
                        if (value == null) {
                            value = "<NULL>";
                        }
                        values.add(value);
                    }

                    if (getResults) {
                        Map<String, Object> row = new LinkedHashMap<>(); // Preserving order
                        int i = 0;
                        for (String col : columns) {
                            row.put(col, values.get(i++));
                        }
                        results.add(row);
                    } else {
                        if (prettyPrint) {
                            tb.addRow(values);
                        } else {
                            System.out.println(values.stream().collect(Collectors.joining(";")));
                        }
                    }
                }

                if (getResults) {
                    return new ExecuteStatementResult(results);
                }
                if (prettyPrint) {
                    System.out.println("\n" + tb.toString());
                }
                return null;
            }
        } else {
            int updateCount = statement.getUpdateCount();
            if (verbose) {
                System.out.println("UPDATE COUNT: " + updateCount);
            }
            return new ExecuteStatementResult(updateCount >= 0 ? updateCount : 0);
        }
    }

    private static void println(Object msg) {
        System.out.println(msg);
    }

    private static void runSqlConsole(Connection connection, Statement statement, boolean pretty) throws IOException {
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
                executeStatement(true, true, false, false, line, statement, null, false, pretty);
            } catch (UserInterruptException | EndOfFileException e) {
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private static InputStream wrapStream(String filename, InputStream rawStream) throws IOException {
        if (filename.endsWith(".gz")) {
            return new GZIPInputStream(rawStream);
        } else if (filename.endsWith(".zip")) {
            return new ZipInputStream(rawStream, StandardCharsets.UTF_8);
        } else {
            return rawStream;
        }

    }

    private static OutputStream wrapOutputStream(OutputStream rawStream, String ext) throws IOException {
        if (ext.endsWith(".gz")) {
            return new GZIPOutputStream(rawStream);
        } else {
            return rawStream;
        }

    }

    private static final Cache<String, net.sf.jsqlparser.statement.Statement> PARSER_CACHE = CacheBuilder
            .newBuilder()
            .maximumSize(50)
            .build();

    private static QueryWithParameters rewriteQuery(String query, TableSpaceMapper mapper, boolean frommysqldump) throws ScriptException {
        try {

            List<Object> parameters = new ArrayList<>();

            if (frommysqldump && query.startsWith("INSERT INTO")) {
                // this is faster than CCJSqlParserUtil and will allow the cache to work at "client-side" too
                QueryWithParameters rewriteSimpleInsertStatement = MySqlDumpInsertStatementRewriter.
                        rewriteSimpleInsertStatement(query);
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
                        InsertDeParser deparser = new InsertDeParser(new ExpressionDeParser(null, queryResult), null,
                                queryResult);
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
                        InsertDeParser deparser = new InsertDeParser(new ExpressionDeParser(null, queryResult), null,
                                queryResult);
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
    private static void commitAndChangeSchema(HerdDBConnection connection, String schema) throws SQLException {
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
