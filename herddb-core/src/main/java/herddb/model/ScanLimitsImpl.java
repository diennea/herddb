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

package herddb.model;

/**
 * Limits on the scan, see the SQL LIMIT CLAUSE
 *
 * @author enrico.olivelli
 */
public class ScanLimitsImpl implements ScanLimits {

    private final int maxRows;
    private final int offset;
    private final int maxRowsJdbcParameterIndex;

    public ScanLimitsImpl(int maxRows, int offset) {
        this.maxRows = maxRows;
        this.offset = offset;
        this.maxRowsJdbcParameterIndex = 0;
    }

    public ScanLimitsImpl(int maxRows, int offset, int maxRowsJdbcParameterIndex) {
        this.maxRows = maxRows;
        this.offset = offset;
        this.maxRowsJdbcParameterIndex = maxRowsJdbcParameterIndex;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public int getOffset() {
        return offset;
    }

    public int getMaxRowsJdbcParameterIndex() {
        return maxRowsJdbcParameterIndex;
    }

    @Override
    public String toString() {
        return "ScanLimits{" + "maxRows=" + maxRows + ", offset=" + offset + '}';
    }

    @Override
    public int computeMaxRows(StatementEvaluationContext context) throws StatementExecutionException {
        if (maxRowsJdbcParameterIndex <= 0) {
            return maxRows;
        } else {
            try {
                Object limit = context.getJdbcParameter(maxRowsJdbcParameterIndex - 1);
                if (limit == null) {
                    throw new StatementExecutionException("Invalid LIMIT with NULL JDBC Parameter");
                } else if (limit instanceof Number) {
                    return ((Number) limit).intValue();
                } else {
                    try {
                        return Integer.parseInt(limit + "");
                    } catch (IllegalArgumentException ee) {
                        throw new StatementExecutionException("Invalid LIMIT JDBC Parameter: value is " + limit);
                    }
                }
            } catch (IndexOutOfBoundsException err) {
                throw new MissingJDBCParameterException(maxRowsJdbcParameterIndex);
            }
        }
    }

    @Override
    public int computeOffset(StatementEvaluationContext context) throws StatementExecutionException {
        return offset;
    }
}
