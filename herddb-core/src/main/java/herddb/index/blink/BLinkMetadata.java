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

import java.util.List;

/**
 * Serializable metadata for BLink indexes.
 *
 * <p>
 * Doesn't contains any node max size because page byte size can change between reboots. Nodes need to be
 * able to adapt to page size changes.
 * </p>
 *
 * @author diego.salvi
 */
public final class BLinkMetadata<K> {

    final long root;
    final long nextNodeId;

    final List<BLinkNodeMetadata<K>> nodeMetadatas;

    public BLinkMetadata(long root, long nextNodeId, List<BLinkNodeMetadata<K>> nodeMetadatas) {
        super();
        this.root = root;
        this.nextNodeId = nextNodeId;
        this.nodeMetadatas = nodeMetadatas;
    }

    public static final class BLinkNodeMetadata<K> {

        public static final byte LEAF_TYPE = 0;
        public static final byte NODE_TYPE = 1;

        final byte type;

        final long nodeId;
        final long storeId;

        final K highKey;

        final long size;
        final long keys;
        final long right;

        public BLinkNodeMetadata(byte type, long nodeId, long storeId, K highKey, long size, long keys, long right) {
            super();

            this.type = type;

            this.nodeId = nodeId;
            this.storeId = storeId;

            this.highKey = highKey;

            this.size = size;
            this.keys = keys;

            this.right = right;
        }

        @Override
        public String toString() {
            return "BLinkNodeMetadata [type=" + type + ", nodeId=" + nodeId + ", storeId=" + storeId + ", highKey="
                    + highKey + ", size=" + size + ", keys=" + keys + ", right=" + right + "]";
        }

    }

    @Override
    public String toString() {
        return "BLinkMetadata [root=" + root + ", nodeMetadatas=" + nodeMetadatas + "]";
    }

}
