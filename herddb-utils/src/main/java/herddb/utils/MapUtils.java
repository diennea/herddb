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

package herddb.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility
 *
 * @author enrico.olivelli
 */
public class MapUtils {

    public static Map<String, Object> map(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new RuntimeException("bad argument list " + objects.length + ": " + Arrays.toString(objects));
        }

        HashMap<String, Object> m = new HashMap<>();
        for (int i = 0; i < objects.length; i += 2) {
            m.put((String) objects[i], objects[i + 1]);
        }

        return m;
    }
}
