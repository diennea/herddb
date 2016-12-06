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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Description of a Node
 *
 * @author enrico.olivelli
 */
public class NodeMetadata {

    public final String nodeId;
    public final Object metadataStorageVersion;
    public final String host;
    public final int port;
    public final boolean ssl;

    private NodeMetadata(String nodeId, Object metadataStorageVersion, String host, int port, boolean ssl) {
        this.nodeId = nodeId;
        this.metadataStorageVersion = metadataStorageVersion;
        this.host = host;
        this.port = port;
        this.ssl = ssl;
    }

    @Override
    public String toString() {
        return "NodeMetadata{" + "nodeId=" + nodeId + ", metadataStorageVersion=" + metadataStorageVersion + ", host=" + host + ", port=" + port + ", ssl=" + ssl + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static NodeMetadata deserialize(byte[] data, Object metadataStorageVersion) throws IOException {
        return deserialize(new DataInputStream(new ByteArrayInputStream(data)), metadataStorageVersion);
    }

    public static NodeMetadata deserialize(DataInputStream in, Object metadataStorageVersion) throws IOException {
        String nodeId = in.readUTF();
        int flags = in.readInt(); // for future implementations
        String host = in.readUTF();
        int port = in.readInt();
        boolean ssl = in.readInt() == 1;
        return new NodeMetadata(nodeId, metadataStorageVersion, host, port, ssl);
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (DataOutputStream doo = new DataOutputStream(oo)) {
            serialize(doo);
        }
        return oo.toByteArray();
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeUTF(nodeId);
        out.writeInt(0); // flags for future implementations
        out.writeUTF(host);
        out.writeInt(port);
        out.writeInt(ssl ? 1 : 0);
    }

    public static class Builder {

        private String nodeId = "localhost";
        private String host = "localhost";
        private int port = 7000;
        private boolean ssl = false;

        private Builder() {
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public NodeMetadata build() {
            if (nodeId == null || nodeId.isEmpty()) {
                throw new IllegalArgumentException("nodeId is not defined");
            }

            return new NodeMetadata(nodeId, null, host, port, ssl);
        }

    }
}
