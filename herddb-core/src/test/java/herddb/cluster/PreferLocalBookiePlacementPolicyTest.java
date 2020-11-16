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

package herddb.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

/**
 * @author francesco.caliumi
 */
public class PreferLocalBookiePlacementPolicyTest {

    // CHECKSTYLE.OFF: ConstantName
    public static final Method registerLocalBookieAddress;
    public static final Method unregisterLocalBookieAddress;
    // CHECKSTYLE.ON: ConstantName

    static {
        try {
            Class<?> localBookiesRegistry = Class.forName("org.apache.bookkeeper.proto.LocalBookiesRegistry");
            registerLocalBookieAddress = localBookiesRegistry.getDeclaredMethod("registerLocalBookieAddress", BookieId.class);
            unregisterLocalBookieAddress = localBookiesRegistry.getDeclaredMethod("unregisterLocalBookieAddress", BookieId.class);
            registerLocalBookieAddress.setAccessible(true);
            unregisterLocalBookieAddress.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testEnsamblePolicySingle() throws Exception {

        BookieId a = new BookieSocketAddress("a.localhost", 3181).toBookieId();

        Set<BookieId> writableBookies = new HashSet<>();
        writableBookies.add(a);

        registerLocalBookieAddress.invoke(null, a);

        try {
            Set<BookieId> readOnlyBookies = Collections.emptySet();

            PreferLocalBookiePlacementPolicy policy = new PreferLocalBookiePlacementPolicy();
            Set<BookieId> deadBookies = policy.onClusterChanged(writableBookies, readOnlyBookies);

            assertTrue(deadBookies.isEmpty());

            List<BookieId> ensemble =
                    policy.newEnsemble(1, 1, 1, Collections.emptyMap(), Collections.emptySet()).getResult();
            System.out.println(ensemble);
            assertEquals(1, ensemble.size());
            assertEquals(a, ensemble.get(0));
        } finally {
            unregisterLocalBookieAddress.invoke(null, a);
        }
    }

    @Test
    public void testEnsamblePolicyMultiple() throws Exception {

        BookieId a = new BookieSocketAddress("a.localhost", 3181).toBookieId();
        BookieId b = new BookieSocketAddress("b.localhost", 3181).toBookieId();
        BookieId c = new BookieSocketAddress("c.localhost", 3181).toBookieId();
        BookieId d = new BookieSocketAddress("d.localhost", 3181).toBookieId();
        BookieId e = new BookieSocketAddress("e.localhost", 3181).toBookieId();

        Set<BookieId> writableBookies = new HashSet<>();
        writableBookies.add(a);
        writableBookies.add(b);
        writableBookies.add(c);
        writableBookies.add(d);
        writableBookies.add(e);

        registerLocalBookieAddress.invoke(null, c);

        try {
            Set<BookieId> readOnlyBookies = Collections.emptySet();

            PreferLocalBookiePlacementPolicy policy = new PreferLocalBookiePlacementPolicy();
            Set<BookieId> deadBookies = policy.onClusterChanged(writableBookies, readOnlyBookies);

            assertTrue(deadBookies.isEmpty());

            List<BookieId> ensemble =
                    policy.newEnsemble(3, 2, 2, Collections.emptyMap(), Collections.emptySet()).getResult();
            System.out.println(ensemble);
            assertEquals(3, ensemble.size());
            assertEquals(c, ensemble.get(0));
        } finally {
            unregisterLocalBookieAddress.invoke(null, c);
        }
    }
}
