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
package herddb.index.blink;

import java.io.IOException;

import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;

/**
 * Node interface of {@link BLink}
 *
 * @author diego.salvi
 *
 * @param <K>
 */
interface BLinkNode<K extends Comparable<K>> {

    public BLinkPtr scanNode(K key);

    public boolean isLeaf();

    public boolean isSafe();
    public boolean isSafeDelete();

    public BLinkNode<K> insert(K key, long pointer);

    public BLinkNode<K>[] split(K key, long page, long newPage);

    public BLinkNode<K> delete(K key);

    public long getPageId();
    BLinkPage getPage();

    public K getHighKey();
    public K getLowKey();

    public BLinkPtr getFirstChild();
    public BLinkPtr getRight();

    public long keys();


    BLinkNodeMetadata<K> checkpoint() throws IOException;

    public void unload(boolean flush);


    /* K key key to delete during merging */
//    BLinkNode<K> merge(K key, BLinkNode<K> sibling);

}
