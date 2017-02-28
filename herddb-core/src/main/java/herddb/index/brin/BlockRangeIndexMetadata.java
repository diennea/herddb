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
package herddb.index.brin;

import java.util.List;

/**
 * Serializable metadata for the index
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexMetadata<K extends Comparable<K>> {

    private List<BlockMetadata<K>> blocksMetadata;

    public List<BlockMetadata<K>> getBlocksMetadata() {
        return blocksMetadata;
    }

    public BlockRangeIndexMetadata(List<BlockMetadata<K>> blocksMetadata) {
        this.blocksMetadata = blocksMetadata;
    }

    public static class BlockMetadata<K extends Comparable<K>> {

        final K firstKey;
        final K lastKey;
        final int blockId;
        final long size;
        final long pageId;

        public BlockMetadata(K firstKey, K lastKey, int blockId, long size, long pageId) {
            this.firstKey = firstKey;
            this.lastKey = lastKey;
            this.blockId = blockId;
            this.size = size;
            this.pageId = pageId;
        }

        public long getSize() {
            return size;
        }

        public long getPageId() {
            return pageId;
        }

        public K getFirstKey() {
            return firstKey;
        }

        public K getLastKey() {
            return lastKey;
        }

        public int getBlockId() {
            return blockId;
        }

    }
}
