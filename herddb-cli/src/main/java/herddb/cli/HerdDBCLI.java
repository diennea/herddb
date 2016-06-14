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

import herddb.client.ClientConfiguration;
import herddb.jdbc.HerdDBDataSource;
import herddb.model.TableSpace;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author enrico.olivelli
 */
public class HerdDBCLI {
     public static void main(String ... args) {
        DefaultParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("x", "url", true, "JDBC URL");
        options.addOption("u", "username", true, "JDBC Username");
        options.addOption("pwd", "password", true, "JDBC Password");
        options.addOption("q", "query", true, "Execute inline query");
        options.addOption("v", "verbose", false, "Verbose output");
        options.addOption("s", "schema", true, "Default tablespace (SQL schema)");
        org.apache.commons.cli.CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException error) {
            System.out.println("Syntax error: " + error);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("herddb", options, true);
            return;
        }
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("herddb", options, true);
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
        try (HerdDBDataSource datasource = new HerdDBDataSource()) {
            datasource.setUrl(url);
            datasource.setUsername(username);
            datasource.setPassword(password);
            
            if (!query.isEmpty()) {
                if (verbose) {
                    System.out.println("Executing query:" + query);
                }
                try (Connection connection = datasource.getConnection();
                        Statement statement = connection.createStatement()) {
                    connection.setSchema(schema);
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
                }
            }

        } catch (SQLException error) {
            if (verbose) {
                error.printStackTrace();
            } else {
                System.out.println("error:" + error);
            }
        }
    }
}
