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
package herddb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.server.Server;
import herddb.server.ServerConfiguration;

/**
 * Prepared statements set parameters testing
 * 
 * @author diego.salvi
 */
public class PreparedStatemetParametersTest
{

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    /**
     * Execute a prepared statement without a needed parameter
     */
    @Test(expected = SQLException.class, timeout = 2000L)
    public void missingParameter() throws Exception
    {
        try ( Server server = new Server(new ServerConfiguration(folder.newFolder().toPath())) )
        {
            server.start();
            server.waitForStandaloneBoot();
            
            try ( HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath())) )
            {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server) );
                
                try ( BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client) )
                {
                    
                    try ( Connection con = dataSource.getConnection();
                          Statement statement = con.createStatement(); )
                    {
                        statement.execute("CREATE TABLE mytable (c1 int primary key)");
                    }
                    
                    try ( Connection con = dataSource.getConnection();
                          PreparedStatement statement = con.prepareStatement("INSERT INTO mytable values (?)"); )
                    {
                        int rows = statement.executeUpdate();
                        
                        Assert.assertEquals(1, rows);
                    }
                    
                }
            }
        }
    }
    
}
