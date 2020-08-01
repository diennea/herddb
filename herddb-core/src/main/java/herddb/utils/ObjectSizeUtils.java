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

/**
 * Simple utilities for dealing with Object sizes. <br>
 * Please note that this is only used as an estimate of the actual memory used
 * by a plan. We are not computing an exact size: <ul>
 * <li>there is no way to do it in Java currently
 * <li>even if it was possible we should not take into account references to
 * objects that are already retained by the system, like refs to Table
 * definitions or cached constants
 * </ul>
 */
public final class ObjectSizeUtils {

    public static final int DEFAULT_OBJECT_SIZE_OVERHEAD = 16;
    public static final int BOOLEAN_FIELD_SIZE = 4;

    public static int stringSize(String s) {
        return DEFAULT_OBJECT_SIZE_OVERHEAD + (s != null ? s.length() : 0);
    }

    private ObjectSizeUtils() {
    }
}
