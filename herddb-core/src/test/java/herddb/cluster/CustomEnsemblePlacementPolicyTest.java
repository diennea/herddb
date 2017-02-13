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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.net.BookieSocketAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author francesco.caliumi
 */
public class CustomEnsemblePlacementPolicyTest {
    
    public static final Method registerLocalBookieAddress;
    public static final Method unregisterLocalBookieAddress;
    
    static {
        try {
            Class LocalBookiesRegistry = Class.forName("org.apache.bookkeeper.proto.LocalBookiesRegistry");
            registerLocalBookieAddress = LocalBookiesRegistry.getDeclaredMethod("registerLocalBookieAddress", BookieSocketAddress.class);
            unregisterLocalBookieAddress = LocalBookiesRegistry.getDeclaredMethod("unregisterLocalBookieAddress", BookieSocketAddress.class);
            registerLocalBookieAddress.setAccessible(true);
            unregisterLocalBookieAddress.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Test
    public void testEnsamblePolicySingle() throws Exception {
        
        BookieSocketAddress a = new BookieSocketAddress("a.diennea.com", 3181);
        
        Set<BookieSocketAddress> writableBookies = new HashSet();
        writableBookies.add(a);
        
        registerLocalBookieAddress.invoke(null, a);
        
        try {
            Set<BookieSocketAddress> readOnlyBookies = Collections.EMPTY_SET;

            CustomEnsemblePlacementPolicy policy = new CustomEnsemblePlacementPolicy();
            Set<BookieSocketAddress> deadBookies = policy.onClusterChanged(writableBookies, readOnlyBookies);

            assertTrue(deadBookies.isEmpty());

            ArrayList<BookieSocketAddress> ensemble = policy.newEnsemble(1, 1, Collections.EMPTY_SET);
            System.out.println(ensemble);
            assertEquals(1, ensemble.size());
            assertEquals(a, ensemble.get(0));
        } finally {
            unregisterLocalBookieAddress.invoke(null, a);
        }
    }
    
    @Test
    public void testEnsamblePolicyMultiple() throws Exception {
         
        BookieSocketAddress a = new BookieSocketAddress("a.diennea.com", 3181);
        BookieSocketAddress b = new BookieSocketAddress("b.diennea.com", 3181);
        BookieSocketAddress c = new BookieSocketAddress("c.diennea.com", 3181);
        BookieSocketAddress d = new BookieSocketAddress("d.diennea.com", 3181);
        BookieSocketAddress e = new BookieSocketAddress("e.diennea.com", 3181);
        
        Set<BookieSocketAddress> writableBookies = new HashSet();
        writableBookies.add(a);
        writableBookies.add(b);
        writableBookies.add(c);
        writableBookies.add(d);
        writableBookies.add(e);
        
        registerLocalBookieAddress.invoke(null, c);
        
        try {
            Set<BookieSocketAddress> readOnlyBookies = Collections.EMPTY_SET;
        
            CustomEnsemblePlacementPolicy policy = new CustomEnsemblePlacementPolicy();
            Set<BookieSocketAddress> deadBookies = policy.onClusterChanged(writableBookies, readOnlyBookies);

            assertTrue(deadBookies.isEmpty());

            ArrayList<BookieSocketAddress> ensemble = policy.newEnsemble(3, 2, Collections.EMPTY_SET);
            System.out.println(ensemble);
            assertEquals(3, ensemble.size());
            assertEquals(c, ensemble.get(0));
        } finally {
            unregisterLocalBookieAddress.invoke(null, c);
        }
    }
}
