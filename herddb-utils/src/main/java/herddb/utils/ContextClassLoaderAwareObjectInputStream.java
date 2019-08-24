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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Specialized ObjectInputStream that looks for class definitions first from the ContextClassLoader. The default
 * beheaviour of ObjectInputStream is weird, it uses something like jdk.internal.misc.VM.latestUserDefinedLoader().
 */
public final class ContextClassLoaderAwareObjectInputStream extends ObjectInputStream {

    public ContextClassLoaderAwareObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {
        try {
            return Class.forName(desc.getName(), false,
                    Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException ex) {
            return super.resolveClass(desc);
        }

    }
}
