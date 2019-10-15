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

import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A set of Tables. All tables in the same TableSpace are handled by the same server at once. All the operations on the
 * tables of a TableSet share the same transaction log, so a transaction can span only tables which belong to the same
 * TableSapce.
 *
 * @author enrico.olivelli
 */
public class TableSpace {

    public static final String DEFAULT = "herd";

    public final String uuid;

    public final String name;
    /**
     * Leader Node for the TableSpace
     */
    public final String leaderId;
    /**
     * Nodes which contain data for the TableSpace. the leaderId MUST be contained in this set
     */
    public final Set<String> replicas;

    /**
     * Expected number of replicas for this TableSpace
     */
    public final int expectedReplicaCount;

    /**
     * Maximum time (in ms) to wait for a leader to ping its presence before auto-healing
     */
    public final long maxLeaderInactivityTime;

    public final Object metadataStorageVersion;
    
    public final long metadataStorageCreationTime;

    private TableSpace(
            String uuid, String name, String leaderId, Set<String> replicas, int expectedReplicaCount,
            long maxLeaderInactivityTime, Object metadataStorageVersion, long metadataStorageCreationTime
    ) {
        this.name = name;
        this.uuid = uuid;
        this.leaderId = leaderId;
        this.replicas = replicas;
        this.expectedReplicaCount = expectedReplicaCount;
        this.metadataStorageVersion = metadataStorageVersion;
        this.maxLeaderInactivityTime = maxLeaderInactivityTime;
        this.metadataStorageCreationTime = metadataStorageCreationTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableSpace deserialize(byte[] data, Object metadataStorageVersion, long metadataStorageCreationTime) throws IOException {
        return deserialize(new ExtendedDataInputStream(new SimpleByteArrayInputStream(data)), metadataStorageVersion, metadataStorageCreationTime);
    }

    public static TableSpace deserialize(ExtendedDataInputStream in, Object metadataStorageVersion, long metadataStorageCreationTime) throws IOException {
        long version = in.readVLong(); // version
        long flags = in.readVLong(); // flags for future implementations
        if (version != 1 || flags != 0) {
            throw new IOException("corrupted tablespace file");
        }
        String uuid = in.readUTF();
        String name = in.readUTF();
        String leaderId = in.readUTF();
        int expectedReplicaCount = in.readVInt();
        int numreplicas = in.readVInt();
        Set<String> replicas = new HashSet<>();
        for (int i = 0; i < numreplicas; i++) {
            replicas.add(in.readUTF());
        }
        long maxLeaderInactivityTime = in.readVLong();
        return new TableSpace(uuid, name, leaderId, replicas, expectedReplicaCount, maxLeaderInactivityTime, metadataStorageVersion, metadataStorageCreationTime);
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ExtendedDataOutputStream doo = new ExtendedDataOutputStream(oo)) {
            serialize(doo);
        }
        return oo.toByteArray();
    }

    public void serialize(ExtendedDataOutputStream out) throws IOException {
        out.writeVLong(1); // version
        out.writeVLong(0); // flags for future implementations
        out.writeUTF(uuid);
        out.writeUTF(name);
        out.writeUTF(leaderId);
        out.writeVInt(expectedReplicaCount);
        out.writeVInt(replicas.size());
        for (String replica : replicas) {
            out.writeUTF(replica);
        }
        out.writeVLong(maxLeaderInactivityTime);
    }

    public static class Builder {

        private final Set<String> replicas = new HashSet<>();
        private String name;
        private String uuid;
        private String leaderId;
        private int expectedReplicaCount = 1;
        private long maxLeaderInactivityTime = 0;

        private Builder() {
        }

        public Builder cloning(TableSpace tableSpace) {
            this.uuid = tableSpace.uuid;
            this.name = tableSpace.name;
            this.replicas.clear();
            this.replicas.addAll(tableSpace.replicas);
            this.leaderId = tableSpace.leaderId;
            this.expectedReplicaCount = tableSpace.expectedReplicaCount;
            this.maxLeaderInactivityTime = tableSpace.maxLeaderInactivityTime;
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder expectedReplicaCount(int expectedReplicaCount) {
            this.expectedReplicaCount = expectedReplicaCount;
            return this;
        }

        public Builder maxLeaderInactivityTime(long maxLeaderInactivityTime) {
            this.maxLeaderInactivityTime = maxLeaderInactivityTime;
            return this;
        }

        public Builder replica(String id) {
            this.replicas.add(id);
            return this;
        }

        public Builder replicas(Set<String> replicas) {
            this.replicas.clear();
            this.replicas.addAll(replicas);
            return this;
        }

        public Builder leader(String id) {
            this.leaderId = id;
            return this;
        }

        public TableSpace build() {
            if (uuid == null) {
                uuid = UUID.randomUUID().toString().replace("-", "");
            }
            if (uuid.isEmpty()) {
                throw new IllegalArgumentException("uuid is not defined");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is not defined");
            }
            if (replicas.isEmpty()) {
                throw new IllegalArgumentException("not replica defined");
            }
            if (leaderId == null || leaderId.isEmpty()) {
                leaderId = replicas.iterator().next();
            }
            if (!replicas.contains(leaderId)) {
                throw new IllegalArgumentException("leader " + leaderId + " must be in replica list " + replicas);
            }
            if (expectedReplicaCount <= 0) {
                throw new IllegalArgumentException("expectedReplicaCount must be > 0");
            }
            if (maxLeaderInactivityTime > 0 && maxLeaderInactivityTime < 5000) {
                throw new IllegalArgumentException("maxLeaderInactivityTime must be >= 5000");
            }
            return new TableSpace(uuid, name, leaderId, Collections.unmodifiableSet(replicas), expectedReplicaCount, maxLeaderInactivityTime, null, 0);
        }

    }

    @Override
    public String toString() {
        return "TableSpace{" + "uuid=" + uuid + ", name=" + name + ", leaderId=" + leaderId + ", replicas=" + replicas + ", expectedReplicaCount=" + expectedReplicaCount + ", maxLeaderInactivityTime=" + maxLeaderInactivityTime + ", metadataStorageVersion=" + metadataStorageVersion + '}';
    }

}
