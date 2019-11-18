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

import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import java.util.logging.Logger;
import java.util.logging.Level;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.FullTableScanPredicate;
import herddb.model.StatementEvaluationContext;
import herddb.model.commands.ScanStatement;
import herddb.core.TableSpaceManager;
import herddb.utils.DataAccessor;
import herddb.codec.RecordSerializer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
/**
 *
 * @author Hamado.Dene
 */
public class TableDataChecksum{

          
    private static final Logger LOGGER = Logger.getLogger(TableDataChecksum.class.getName());

    TableSpaceManager manager;
    String tableSpace;
    Table table;
    private long digest=0;
    Checksum checksum;
    
    public TableDataChecksum (TableSpaceManager manager,String tableSpace,Table table){
        this.tableSpace=tableSpace;
        this.manager= manager;
        this.table=table;
        checksum = new CRC32();
    }
    
        
    public void createChecksum(TableSpaceManager manager,String tableSpace,Table table){
        try ( DataScanner scan = manager.scan(new ScanStatement(tableSpace, table, new FullTableScanPredicate()),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION, false,false);){
            while(scan.hasNext()){
                // if next exist, get the next value
                DataAccessor data = scan.next();
                //create an object array
                Object[] obj = data.getValues();            
                //Serialize object for CRC
                //not sure for type
                byte[] serialize = RecordSerializer.serialize(obj, 0);               
                //update record in digest
                checksum.update(serialize, 0 , serialize.length);
            }
            //get final checksum value
            digest=checksum.getValue();
            
        } catch (DataScannerException ex) {
           LOGGER.log(Level.SEVERE,null, ex);
        }      
    }
 
    
    public long getChecksum(){
        return this.digest;
    }
      
}





