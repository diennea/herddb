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
package herddb.data.integrity;

import herddb.core.DBManager;
import static herddb.core.TestUtils.execute;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import herddb.model.commands.TableIntegrityCheckStatement;
import herddb.model.commands.TableSpaceIntegrityCheckStatement;
/**
 *
 * @author Hamado.Dene
 */
public class TableDataCheckSumTest{
    
    @Test
    public void test() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(), new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager, "CREATE TABLE tblspace1.tsql (k1 string primary key,n1 int,s1 string)", Collections.emptyList());            
            execute(manager, "CREATE TABLE tblspace1.tsql1 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());            
            execute(manager, "CREATE TABLE tblspace1.tsql2 (k1 string primary key,n1 int,s1 string)", Collections.emptyList());
            
            for(int i=0; i<10; i++){
                System.out.println("insert number " + i);
                execute(manager, "INSERT INTO tblspace1.tsql (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));                
                execute(manager, "INSERT INTO tblspace1.tsql1 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));                
                execute(manager, "INSERT INTO tblspace1.tsql2 (k1,n1 ,s1) values (?,?,?)", Arrays.asList(i, 1, "b"));
            }
            TableIntegrityCheckStatement statement = new TableIntegrityCheckStatement("tblspace1", "tsql");            
            manager.executeStatement(statement, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);            
            TableSpaceIntegrityCheckStatement statement2  = new TableSpaceIntegrityCheckStatement("tblspace1");
            manager.executeStatement(statement2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
        }
    }

}





