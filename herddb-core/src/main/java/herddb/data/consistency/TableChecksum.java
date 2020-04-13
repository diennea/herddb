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

/**
 * Table checksum info
 *
 * @author hamado
 */
public final class TableChecksum {

    private String tableSpaceName;
    private String tableName;
    private long digest;
    private String digestType;
    private long numRecords;
    private long nextAutoIncrementValue;
    private String query;
    private long scanDuration;

    //This is for JSON deserialization
    public TableChecksum() {
    }

    TableChecksum(String tableSpaceName, String tableName, long digest, String digestType, long numRecords, long nexAutoIncrementValue, String query, long scanDuration) {
        this.tableSpaceName = tableSpaceName;
        this.tableName = tableName;
        this.digest = digest;
        this.digestType = digestType;
        this.numRecords = numRecords;
        this.nextAutoIncrementValue = nexAutoIncrementValue;
        this.query = query;
        this.scanDuration = scanDuration;
    }

    public long getScanDuration() {
        return scanDuration;
    }

    public void setScanDuration(long scanDuration) {
        this.scanDuration = scanDuration;
    }

    public String getTableSpaceName() {
        return tableSpaceName;
    }

    public void setTableSpaceName(String tableSpaceName) {
        this.tableSpaceName = tableSpaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getDigest() {
        return digest;
    }

    public void setDigest(long digest) {
        this.digest = digest;
    }

    public String getDigestType() {
        return digestType;
    }

    public void setDigestType(String digestType) {
        this.digestType = digestType;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public long getNextAutoIncrementValue() {
        return nextAutoIncrementValue;
    }

    public void setNextAutoIncrementValue(long nextAutoIncrementValue) {
        this.nextAutoIncrementValue = nextAutoIncrementValue;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
