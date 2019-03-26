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

    final long nextID;

    /** Anchor <i>fast node</i> pointer */
    final long fast;

    /** Anchor <i>fast node</i> height */
    final int fastheight;

    /** Anchor real <i>root</i> pointer */
    final long top;

    /** Anchor real <i>root</i> height */
    final int topheight;

    /** Leftmost leaf */
    final long first;

    /** Stored values count */
    final long values;

    final List<BLinkNodeMetadata<K>> nodes;

    public BLinkMetadata(long nextID,
            long fast, int fastheight, long top, int topheight,
            long first, long values,
            List<BLinkNodeMetadata<K>> nodes) {
        super();
        this.nextID = nextID;
        this.fast = fast;
        this.fastheight = fastheight;
        this.top = top;
        this.topheight = topheight;
        this.first = first;
        this.values = values;
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "BLinkMetadata [nextID=" + nextID +
                ", fast=" + fast +
                ", fastheight=" + fastheight +
                ", top=" + top +
                ", topheight=" + topheight +
                ", first=" + first +
                ", size=" + values +
                ", nodes=" + nodes.size() +
                "]";
    }

    public String nodesToStrings() {
        return nodes+"";
    }

    public static final class BLinkNodeMetadata<K> {

        public static final long NO_LINK = -1;

        final boolean leaf;

        final long id;
        final long storeId;

        final int keys;
        final long bytes;

        final long outlink;
        final long rightlink;

        final K rightsep;

        public BLinkNodeMetadata(boolean leaf, long id, long storeId, int keys, long size, long outlink, long rightlink, K rightsep) {
            super();
            this.leaf = leaf;
            this.id = id;
            this.storeId = storeId;
            this.keys = keys;
            this.bytes = size;
            this.outlink = outlink;
            this.rightlink = rightlink;
            this.rightsep = rightsep;
        }

        @Override
        public String toString() {
            return "BLinkNodeMetadata [leaf=" + leaf +
                    ", id=" + id +
                    ", storeId=" + storeId +
                    ", keys=" + keys +
                    ", size=" + bytes +
                    ", outlink=" + outlink +
                    ", rightlink=" + rightlink +
                    ", rightsep=" + rightsep +
                    "]";
        }

    }

}
