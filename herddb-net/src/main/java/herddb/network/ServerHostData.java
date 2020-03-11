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

package herddb.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Information about a broker
 *
 * @author enrico.olivelli
 */
public class ServerHostData {

    private final int port;
    private final String host;
    private final String version;
    private final boolean ssl;
    private final Map<String, String> additionalData;

    public ServerHostData(String host, int port, String version, boolean ssl, Map<String, String> additionalData) {
        this.port = port;
        this.host = host;
        this.version = version;
        this.ssl = ssl;
        this.additionalData = additionalData;
    }

    @Override
    public String toString() {
        return "ServerHostData{" + "port=" + port + ", host=" + host + ", version=" + version + ", ssl=" + ssl + ", additionalData=" + additionalData + '}';
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public String getVersion() {
        return version;
    }

    public boolean isSsl() {
        return ssl;
    }

    public Map<String, String> getAdditionalData() {
        return additionalData;
    }

    public static byte[] formatHostdata(ServerHostData data) {
        try {
            Map<String, String> mdata = new HashMap<>();
            mdata.put("host", data.host);
            mdata.put("port", data.port + "");
            mdata.put("version", data.version);
            mdata.put("ssl", data.ssl + "");
            if (data.additionalData != null) {
                mdata.putAll(data.additionalData);
            }
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            new ObjectMapper().writeValue(oo, mdata);
            return oo.toByteArray();
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    public static ServerHostData parseHostdata(byte[] oo) {
        try {
            Map<String, Object> data = new ObjectMapper().readValue(new ByteArrayInputStream(oo), Map.class);
            String host = (String) data.get("host");
            String version = (String) data.get("version");
            int port = Integer.parseInt(data.get("port") + "");
            boolean ssl = "true".equals(data.get("ssl"));
            Map<String, String> additional = new HashMap<>();
            data.forEach((k, v) -> {
                if (v != null) {
                    additional.put(k, v.toString());
                }
            });
            return new ServerHostData(host, port, version, ssl, additional);
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
    }
}
