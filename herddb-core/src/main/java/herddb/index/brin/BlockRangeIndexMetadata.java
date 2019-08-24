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

import java.util.Collections;
import java.util.List;

/**
 * Serializable metadata for the index
 *
 * @author enrico.olivelli
 */
public class BlockRangeIndexMetadata<K extends Comparable<K>> {

    @SuppressWarnings("rawtypes")
    private static final BlockRangeIndexMetadata EMPTY_METADATA =
            new BlockRangeIndexMetadata<>(Collections.emptyList());

    @SuppressWarnings("unchecked")
    public static final <X extends Comparable<X>> BlockRangeIndexMetadata<X> empty() {
        return EMPTY_METADATA;
    }

    private List<BlockMetadata<K>> blocksMetadata;

    public List<BlockMetadata<K>> getBlocksMetadata() {
        return blocksMetadata;
    }

    public BlockRangeIndexMetadata(List<BlockMetadata<K>> blocksMetadata) {
        this.blocksMetadata = blocksMetadata;
    }

    public static class BlockMetadata<K extends Comparable<K>> {

        public static final byte HEAD = 0b01;
        public static final byte TAIL = 0b10;

        final boolean headBlock;
        final K firstKey;
        final long blockId;
        final long size;
        final long pageId;
        final Long nextBlockId;

        BlockMetadata(K firstKey, long blockId, long size, long pageId, Long nextBlockId) {

            this.firstKey = firstKey;
            this.blockId = blockId;
            this.size = size;
            this.pageId = pageId;
            this.nextBlockId = nextBlockId;

            if (nextBlockId != null && nextBlockId == blockId) {
                throw new IllegalStateException("I cannot be the successor of myself! BlockId " + blockId);
            }

            headBlock = firstKey == null;
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

        public boolean isHeadBlock() {
            return headBlock;
        }

        public long getBlockId() {
            return blockId;
        }

        public Long getNextBlockId() {
            return nextBlockId;
        }

        @Override
        public String toString() {
            return "BlockMetadata [headBlock=" + headBlock + ", firstKey=" + firstKey + ", blockId=" + blockId
                    + ", size=" + size + ", pageId=" + pageId + ", nextBlockId=" + nextBlockId + "]";
        }

    }

    @Override
    public String toString() {
        return "BlockRangeIndexMetadata [blocksMetadata=" + blocksMetadata + "]";
    }

}
