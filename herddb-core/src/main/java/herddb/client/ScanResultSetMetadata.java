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
package herddb.client;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Map;

/**
 * Metadta about a ResultSet
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ScanResultSetMetadata {

    private String[] columnNames;
    private Map<String, Integer> columnNameToPosition;

    public ScanResultSetMetadata(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * Returns the index (base 0) of the column. The search is not case
     * sensitive
     *
     * @param columnName
     * @return returns -1 if column not found
     */
    public int getColumnPosition(String columnName) {
        if (columnNameToPosition == null) {
            ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
            int i = 1;
            for (String _columnName : columnNames) {
                if (_columnName == null) {
                    throw new IllegalStateException("Invalid columnName null, in " + Arrays.toString(columnNames));
                }
                builder.put(_columnName, i++);

            }
            columnNameToPosition = builder.build();
        }
        Integer pos = columnNameToPosition.get(columnName);
        if (pos == null) {
            pos = columnNameToPosition.get(columnName.toLowerCase());
        }
        return (pos == null) ? 0 : pos;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

}
