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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Description of a Node
 *
 * @author enrico.olivelli
 */
public class NodeMetadata {

    public final String nodeId;
    public final Object metadataStorageVersion;

    private NodeMetadata(String nodeId, Object metadataStorageVersion) {
        this.nodeId = nodeId;
        this.metadataStorageVersion = metadataStorageVersion;
    }

    @Override
    public String toString() {
        return "NodeMetadata{" + "nodeId=" + nodeId + ", metadataStorageVersion=" + metadataStorageVersion + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static NodeMetadata deserialize(byte[] data, Object metadataStorageVersion) throws IOException {
        return deserialize(new DataInputStream(new ByteArrayInputStream(data)), metadataStorageVersion);
    }

    public static NodeMetadata deserialize(DataInputStream in, Object metadataStorageVersion) throws IOException {
        String nodeId = in.readUTF();

        return new NodeMetadata(nodeId, metadataStorageVersion);
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
    }

    public static class Builder {

        private String nodeId;

        private Builder() {
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public NodeMetadata build() {
            if (nodeId == null || nodeId.isEmpty()) {
                throw new IllegalArgumentException("nodeId is not defined");
            }

            return new NodeMetadata(nodeId, null);
        }

    }
}
