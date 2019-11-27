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
import herddb.model.TupleComparator;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
/**
 * digest creation by scanning the table
 * @author Hamado.Dene
 */
public abstract class TableDataChecksum{
         
    private static final Logger LOGGER = Logger.getLogger(TableDataChecksum.class.getName());
    private static final XXHashFactory factory=XXHashFactory.fastestInstance();
    private static final int SEED = 0x9747b28c;
    public static final  int DIGEST_NOT_AVAILABLE = 0;
    public static final String HASH_TYPE="StreamingXXHash64";
    public static int NUM_RECORD=0;

    public static long createChecksum(TableSpaceManager manager,String tableSpace,String table){
        // ??? how this work?
        TupleComparator comparator=null;
        ScanStatement statement = new ScanStatement(tableSpace, table, null,new FullTableScanPredicate(),comparator,null);
        
        try ( DataScanner scan = manager.scan(statement,StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION, false,false);){
            StreamingXXHash64 hash64 = factory.newStreamingHash64(SEED);

            byte[] serialize;
            while(scan.hasNext()){
                NUM_RECORD++;
                DataAccessor data = scan.next();
                Object[] obj = data.getValues();               
                Column[] schema = scan.getSchema();                
                for(int i=0; i< schema.length;i++){
                    serialize = RecordSerializer.serialize(obj[i],schema[i].type);
                    hash64.update(serialize, 0, SEED);
                }
            }
            LOGGER.log(Level.FINER,"Number of processed records for table {0}.{1} = {2} ", new Object[]{tableSpace,table, NUM_RECORD});
           return hash64.getValue();
        } catch (DataScannerException ex) {
            LOGGER.log(Level.SEVERE,"Scan failled", ex);
            return DIGEST_NOT_AVAILABLE;
        } 
    }
    
    public String hashType(){
        return HASH_TYPE;
    }
    public int numRecords(){
        return NUM_RECORD;
    }
}



