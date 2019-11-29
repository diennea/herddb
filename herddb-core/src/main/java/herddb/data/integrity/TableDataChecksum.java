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
import herddb.model.commands.ScanStatement;
import herddb.core.TableSpaceManager;
import herddb.utils.DataAccessor;
import herddb.codec.RecordSerializer;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.model.Column;
import herddb.model.Table;
import herddb.sql.TranslatedQuery;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    public static final  boolean DIGEST_NOT_AVAILABLE = true;
    public static final String HASH_TYPE="StreamingXXHash64";
    public static int NUM_RECORD=0;
    public static long TABLE_DIGEST_DURATION=0;
    private static TranslatedQuery translated;
    private static final  Map<String, Object> scanresult = new HashMap<>();
    
    //this method returns a map with all scan values (record numbers , table digest,digestType, next autoincrement value, table name, tablespacename )
    //this data will be written to the transaction log so the follower nodes will do exactly what the master did
    public static Map<String,Object> createChecksum(DBManager manager,TranslatedQuery query, TableSpaceManager tableSpaceManager,String tableSpace,String tableName){
        
        AbstractTableManager tablemanager = tableSpaceManager.getTableManager(tableName);
        if(query == null){
            final Table table = tableSpaceManager.getTableManager(tableName).getTable();
            String columns = parseColumns(table);
            translated = manager.getPlanner().translate(tableSpace, "SELECT  "
                    + columns 
                    + " FROM "+ tableName 
                    + " order by " 
                    + parsePrimaryKeys(table) , Collections.emptyList(), true, false, false, -1);
         }
        translated = query;        
        ScanStatement statement =  translated.plan.mainStatement.unwrap(ScanStatement.class);
        LOGGER.log(Level.INFO,"creating digest for table {0}.{1} ", new Object[]{tableSpace,tableName});
        
        try ( DataScanner scan = manager.scan(statement, translated.context, TransactionContext.NO_TRANSACTION);){
            StreamingXXHash64 hash64 = factory.newStreamingHash64(SEED);
            byte[] serialize;
            long _start = System.currentTimeMillis(); 
            
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
            LOGGER.log(Level.FINER,"Number of processed records for table {0}.{1} = {2} ", new Object[]{tableSpace,tableName, NUM_RECORD});
            long _stop = System.currentTimeMillis();
            long nextAutoIncrementValue = tablemanager.getNextPrimaryKeyValue();  
            TABLE_DIGEST_DURATION = (_stop - _start);
             LOGGER.log(Level.INFO,"Creating digest for table {0}.{1} finished ", new Object[]{tableSpace,tableName});
             
            scanresult.put("digest", hash64.getValue());
            scanresult.put("digestType", HASH_TYPE);
            scanresult.put("numRecords", NUM_RECORD);
            scanresult.put("tableSpaceName", tableSpace);
            scanresult.put("tableName", tableName);
            scanresult.put("nextAutoIncrementValue", nextAutoIncrementValue);
            scanresult.put("ScanDuration", TABLE_DIGEST_DURATION);
            scanresult.put("query",translated.context.query);
            scanresult.put("DIGEST_NOT_AVAIBLE", false);
            
            
           return scanresult;
        } catch (DataScannerException ex) {
            LOGGER.log(Level.SEVERE,"Scan failled", ex);
            scanresult.put("DIGEST_NOT_AVAIBLE", DIGEST_NOT_AVAILABLE);
            return scanresult;
        } 
    }
    
    public  static String parsePrimaryKeys(Table table){
        return Arrays.asList(table.getPrimaryKey()).stream().collect(Collectors.joining(","));
    }
    public static String  parseColumns(Table table){
        return Stream.of(table.getColumns()).map(Column::getName).collect(Collectors.joining(","));
    }
}


