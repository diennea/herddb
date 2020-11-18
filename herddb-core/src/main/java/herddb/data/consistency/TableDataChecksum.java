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
package herddb.data.consistency;

import herddb.codec.RecordSerializer;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.model.Column;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import herddb.utils.SystemInstrumentation;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * digest creation by scanning the table
 *
 * @author Hamado.Dene
 */
public abstract class TableDataChecksum {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableDataChecksum.class.getName());
    private static final XXHashFactory FACTORY = XXHashFactory.fastestInstance();
    private static final int SEED = 0;
    public static final String HASH_TYPE = "StreamingXXHash64";

    public static TableChecksum createChecksum(DBManager manager, TranslatedQuery query, TableSpaceManager tableSpaceManager, String tableSpace, String tableName) throws DataScannerException {

        AbstractTableManager tablemanager = tableSpaceManager.getTableManager(tableName);
        String nodeID = tableSpaceManager.getDbmanager().getNodeId();
        TranslatedQuery translated = query;
        final Table table = manager.getTableSpaceManager(tableSpace).getTableManager(tableName).getTable();
        //Number of records
        long nrecords = 0;
        //If null value is passed as a query
        //For example, in leader node we may not know the query
        if (translated == null) {
            String columns = formatColumns(table);
            /*
                scan = true
                allowCache = false
                returnValues = false
                maxRows = -1
            */
            translated = manager.getPlanner().translate(tableSpace, "SELECT  "
                    + columns
                    + " FROM " + tableName
                    + " order by "
                    + formatPrimaryKeys(table), Collections.emptyList(), true, false, false, -1);
        }
        ScanStatement statement = translated.plan.mainStatement.unwrap(ScanStatement.class);
        statement.setAllowExecutionFromFollower(true);
        LOGGER.info("creating checksum for table {}.{} on node {}", new Object[]{ tableSpace, tableName, nodeID});
        try (DataScanner scan = manager.scan(statement, translated.context, TransactionContext.NO_TRANSACTION);) {
            StreamingXXHash64 hash64 = FACTORY.newStreamingHash64(SEED);
            long _start = System.currentTimeMillis();
            while (scan.hasNext()) {
                nrecords++;
                DataAccessor data = scan.next();
                data.forEach((String key, Object value) -> {
                    int type = table.getColumn(key).type;
                    byte[] serialize = RecordSerializer.serialize(value, type);
                    /*
                        Update need three parameters
                        update(byte[]buff, int off, int len)
                        buff is the input data
                        off is the start offset in buff
                        len is the number of bytes to hash
                     */
                    if (serialize != null) {
                        hash64.update(serialize, 0, serialize.length);
                    }
                });
            }
            LOGGER.info("Number of processed records for table {}.{} on node {} = {} ", new Object[]{tableSpace, tableName, nodeID, nrecords});
            long _stop = System.currentTimeMillis();
            long nextAutoIncrementValue = tablemanager.getNextPrimaryKeyValue();
            long scanduration = (_stop - _start);
            LOGGER.info("Creating checksum for table {}.{} on node {} finished in {} ms", new Object[]{tableSpace, tableName, nodeID, scanduration});

            SystemInstrumentation.instrumentationPoint("createChecksum", tableSpace, tableName);

            return new TableChecksum(tableSpace, tableName, hash64.getValue(), HASH_TYPE, nrecords, nextAutoIncrementValue, translated.context.query, scanduration);
        } catch (DataScannerException ex) {
            LOGGER.error("Scan failled", ex);
            throw new DataScannerException(ex);
    }
    }

    private static String formatPrimaryKeys(Table table) {
        return Arrays.asList(table.getPrimaryKey()).stream().collect(Collectors.joining(","));
    }

    private static String formatColumns(Table table) {
        return Stream.of(table.getColumns()).map(Column::getName).collect(Collectors.joining(","));
    }
}
