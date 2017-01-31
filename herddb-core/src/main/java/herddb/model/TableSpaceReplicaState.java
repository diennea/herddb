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

import herddb.utils.SimpleByteArrayInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * State of a replica
 *
 * @author enrico.olivelli
 */
public class TableSpaceReplicaState {

    public static final int MODE_LEADER = 1;
    public static final int MODE_FOLLOWER = 2;
    public static final int MODE_STOPPED = 3;

    public final String uuid;
    public final String nodeId;
    public final long timestamp;
    public final int mode;

    public TableSpaceReplicaState(String uuid, String nodeId, long timestamp, int mode) {
        this.uuid = uuid;
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.mode = mode;
    }

    public static TableSpaceReplicaState deserialize(byte[] data) throws IOException {
        return deserialize(new DataInputStream(new SimpleByteArrayInputStream(data)));
    }

    public static TableSpaceReplicaState deserialize(DataInputStream in) throws IOException {
        int flags = in.readInt(); // for future implementations        
        String uuid = in.readUTF();
        String nodeId = in.readUTF();
        long timestamp = in.readLong();
        int mode = in.readInt();
        return new TableSpaceReplicaState(uuid, nodeId, timestamp, mode);
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (DataOutputStream doo = new DataOutputStream(oo)) {
            serialize(doo);
        }
        return oo.toByteArray();
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(0); // for future implementations
        out.writeUTF(uuid);
        out.writeUTF(nodeId);
        out.writeLong(timestamp);
        out.writeInt(mode);
    }

    public static TableSpaceReplicaState.Builder builder() {
        return new TableSpaceReplicaState.Builder();
    }

    public static class Builder {

        private String nodeId;
        private String uuid;
        private long timestamp;
        private int mode;

        private Builder() {
        }

        public Builder cloning(TableSpaceReplicaState other) {
            this.uuid = other.uuid;
            this.nodeId = other.nodeId;
            this.timestamp = other.timestamp;
            this.mode = other.mode;
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder mode(int mode) {
            this.mode = mode;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public TableSpaceReplicaState build() {
            if (uuid == null) {
                throw new IllegalArgumentException("uuid is not defined");
            }
            if (nodeId == null) {
                throw new IllegalArgumentException("nodeId is not defined");
            }
            if (timestamp <= 0) {
                throw new IllegalArgumentException("timestamp is not defined");
            }
            if (mode <= 0) {
                throw new IllegalArgumentException("mode is not defined");
            }
            return new TableSpaceReplicaState(uuid, nodeId, timestamp, mode);
        }

    }

    @Override
    public String toString() {
        return "TableSpaceReplicaState{" + "uuid=" + uuid + ", nodeId=" + nodeId + ", timestamp=" + timestamp + ", mode=" + mode + '}';
    }

    public static String modeToSQLString(int mode) {
        switch (mode) {
            case MODE_LEADER:
                return "leader";
            case MODE_FOLLOWER:
                return "follower";
            case MODE_STOPPED:
                return "stopped";
            default:
                return "mode?" + mode;
        }
    }

}
