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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A tuple of values
 *
 * @author enrico.olivelli
 */
public class Tuple {

    /**
     * Effetctive values. This array shoould be threated as immutable
     */
    public final Object[] values;

    public final String[] fieldNames;

    private Map<String, Object> map;

    public Tuple(String[] fieldNames, Object[] values) {
        this.fieldNames = fieldNames;
        this.values = values;
        if (fieldNames.length != values.length) {
            throw new IllegalArgumentException();
        }
    }

    public Tuple(Map<String, Object> record) {
        int size = record.size();
        this.fieldNames = new String[size];
        this.values = new Object[size];
        this.map = record;
        int i = 0;
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            fieldNames[i] = entry.getKey();
            values[i++] = entry.getValue();
        }
    }

    public Tuple(Map<String, Object> record, Column[] columns) {
        int size = columns.length;
        this.fieldNames = new String[size];
        this.values = new Object[size];
        this.map = record;
        int i = 0;
        for (Column c : columns) {
            fieldNames[i] = c.name;
            values[i++] = record.get(c.name);
        }
    }

    public int size() {
        return values.length;
    }

    public Map<String, Object> toMap() {
        if (map != null) {
            return map;
        }
        HashMap _map = new HashMap<>();
        for (int i = 0; i < fieldNames.length; i++) {
            _map.put(fieldNames[i], values[i]);
        }
        this.map = _map;
        return _map;
    }

    @Override
    public String toString() {
        return "Tuple{" + "values=" + Arrays.toString(values) + ", fieldNames=" + Arrays.toString(fieldNames) + '}';
    }

    public Object get(int i) {
        return values[i];
    }

    public Object get(String name) {
        return toMap().get(name);
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(oo)) {
            oos.writeUnshared(fieldNames);
            oos.writeUnshared(values);
        }
        return oo.toByteArray();
    }

    public static Tuple deserialize(byte[] data) throws IOException {
        try {
            ObjectInputStream oo = new ObjectInputStream(new ByteArrayInputStream(data));
            String[] fieldNames = (String[]) oo.readUnshared();
            Object[] values = (Object[]) oo.readUnshared();
            return new Tuple(fieldNames, values);
        } catch (ClassNotFoundException err) {
            throw new IOException(err);
        }
    }
}
