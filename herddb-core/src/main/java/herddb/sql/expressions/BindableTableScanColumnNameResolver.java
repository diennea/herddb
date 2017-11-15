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
package herddb.sql.expressions;

import herddb.model.Column;
import herddb.model.Table;

/**
 * Helper which associates a given position to a column name.
 *
 * @author eolivelli
 */
public interface BindableTableScanColumnNameResolver {

    /**
     * Returns the namr of the column in a table. It is assumed that the
     * index is relative to the logical structure of the table
     * @param columnReference
     * @return 
     * @see Table#getColumns() 
     */
    public Column resolveColumName(int columnReference);
}
