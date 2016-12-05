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
package herddb.metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Utility for the generation of new node ids
 *
 * @author enrico.olivelli
 */
public class NodeIdGenerator {

    private final List<String> list;
    private final Random random = new Random();

    public NodeIdGenerator() {
        try (InputStream in = NodeIdGenerator.class.getResourceAsStream("nodeidslist.txt");
            InputStreamReader read = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader r = new BufferedReader(read)) {
            list = r
                .lines()
                .map(String::trim).filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(Collectors.toList());
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    public String nextId() {
        int r = random.nextInt(list.size());
        return list.get(r);
    }
}
