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

import herddb.sql.TranslatedQuery;
import java.util.HashMap;
import java.util.Map;

/**
 * Digest data manipulation
 * @author Hamado.Dene
 */
public  class DigestData {

    private  String tableSpaceName;
    private  String tableName;
    private  long digest;
    private  String digestType;
    private  int numRecords;
    private  long nextAutoIncrementValue;
    private String query;
    
    public void setDigest(long digest){
        this.digest = digest;
    }
    public void setDigestType(String digestType){
        this.digestType = digestType;
    }    
    public void SetNumRecords(int numRecords){
        this.numRecords = numRecords;
    }
    public void setTableName(String tableName){
        this.tableName = tableName;
    }
    public void setTableSpaceName(String tableSpaceName){
        this.tableSpaceName = tableSpaceName;
    }
    public void setNextAutoIncrementValue(long nextAutoIncrementValue){
        this.nextAutoIncrementValue = nextAutoIncrementValue;
    }
    public void setScanQuery(String query){
        this.query = query;
    }
    
    public long getDigest(){
        return this.digest;
    }
    public String getDigestType(){
        return this.digestType;
    }
    public int getNumRecords(){
        return this.numRecords;
    }
    public String getTableSpaceName(){
        return this.tableSpaceName;
    }
    public String getTableName(){
        return this.tableName;
    }
    public long getNextAutoIncrementValue(){
        return this.nextAutoIncrementValue;
    }
    public String getScanQuery(){
        return this.query;
    }
    
    public Map<String,Object> digestInfo(){
        Map<String, Object> map = new HashMap<>();
        map.put("digest", digest);
        map.put("digestType", digestType);
        map.put("numRecords", numRecords);
        map.put("tableSpaceName", tableSpaceName);
        map.put("tableName", tableName);
        map.put("nextAutoIncrementValue", nextAutoIncrementValue);
        map.put("query", query);
        return map;
    }
    
}
