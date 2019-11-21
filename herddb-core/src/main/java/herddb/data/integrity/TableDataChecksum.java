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
import herddb.model.TransactionContext;
import herddb.model.FullTableScanPredicate;
import herddb.model.StatementEvaluationContext;
import herddb.model.commands.ScanStatement;
import herddb.core.TableSpaceManager;
import herddb.utils.DataAccessor;
import herddb.codec.RecordSerializer;
import herddb.model.Column;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
/**
 *
 * @author Hamado.Dene
 */
public class TableDataChecksum{

          
    private static final Logger LOGGER = Logger.getLogger(TableDataChecksum.class.getName());

    private final XXHashFactory factory;
    private final int SEED = 0x9747b28c;
    private StreamingXXHash64 hash64;
    
    public TableDataChecksum (){
        this.factory = XXHashFactory.fastestInstance();
    }
    
        
    public long createChecksum(TableSpaceManager manager,String tableSpace,String table){
        
        //da passargli anche il comparator per l'ordinamento
        ScanStatement statement = new ScanStatement(tableSpace, table, null,new FullTableScanPredicate(),null,null);
        
        try ( DataScanner scan = manager.scan(statement,StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION, false,false);){
            
            hash64 = factory.newStreamingHash64(SEED);
            byte[] serialize=null;
            while(scan.hasNext()){
                // if next exist, get the next value
                DataAccessor data = scan.next();
                //create an object array
                Object[] obj = data.getValues();
                
                Column[] schema = scan.getSchema();
                
                for(int i=0; i< schema.length;i++){
                    int type=schema[i].type;
                    serialize = RecordSerializer.serialize(obj[i],type);
                }
                //update record in digest
                hash64.update(serialize, 0, SEED);
            }
            
        } catch (DataScannerException ex) {
           LOGGER.log(Level.SEVERE,null, ex);
           //Se lo Scan fallisce, non deve essere bloccante, deve continuare con quello che stava facendo e skippare la creazione del checksum per questa tabella
           
        } 
        return hash64.getValue();
    }
    
}
