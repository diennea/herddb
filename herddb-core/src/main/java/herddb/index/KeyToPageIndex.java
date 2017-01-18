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
package herddb.index;

import herddb.model.StatementEvaluationContext;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Index which maps every key of a table to the page which contains the key. Keys assigned to new pages are assigned to
 * a special NO_PAGE value
 *
 * @author enrico.olivelli
 */
public interface KeyToPageIndex extends AutoCloseable {

    public long size();

    public Long put(Bytes key, long currentPage);

    public Iterable<Bytes> getKeysMappedToPage(long page);

    public boolean containsKey(Bytes key);

    public Long get(Bytes key);

    public Long remove(Bytes key);

    public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation, StatementEvaluationContext context, TableContext tableContext, herddb.core.AbstractIndexManager index) throws DataStorageManagerException;

    @Override
    public void close();

    public void truncate();
}
