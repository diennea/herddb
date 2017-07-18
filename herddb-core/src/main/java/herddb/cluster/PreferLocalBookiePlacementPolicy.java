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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.commons.configuration.Configuration;

/**
 * Copied from DefaultEnsemblePlacementPolicy
 * @author francesco.caliumi
 */
public class PreferLocalBookiePlacementPolicy implements EnsemblePlacementPolicy {

    private Set<BookieSocketAddress> knownBookies = new HashSet<>();

    private static final Method isLocalBookieMethod;
    static {
        try {
            Class<?> c = Class.forName("org.apache.bookkeeper.proto.LocalBookiesRegistry");
            isLocalBookieMethod = c.getDeclaredMethod("isLocalBookie", BookieSocketAddress.class);
            isLocalBookieMethod.setAccessible(true);

        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean isLocalBookie(BookieSocketAddress bookie) {
        // Will be public in Bookkeeper 4.5.0
        //return LocalBookiesRegistry.isLocalBookie(bookie);

        try {
            return (boolean) isLocalBookieMethod.invoke(null, bookie);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int quorumSize,
            Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {

        ArrayList<BookieSocketAddress> newBookies = new ArrayList<>(ensembleSize);
        if (ensembleSize <= 0) {
            return newBookies;
        }
        List<BookieSocketAddress> allBookies;
        synchronized (this) {
            allBookies = new ArrayList<>(knownBookies);
        }

        BookieSocketAddress localBookie = null;
        for (BookieSocketAddress bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            if (isLocalBookie(bookie)) {
                localBookie = bookie;
                break;
            }
        }
        if (localBookie != null) {
            boolean ret = allBookies.remove(localBookie);
            if (!ret) {
                throw new RuntimeException("localBookie not found in list");
            }
            newBookies.add(localBookie);
            --ensembleSize;
            if (ensembleSize == 0) {
                return newBookies;
            }
        }

        Collections.shuffle(allBookies);
        for (BookieSocketAddress bookie : allBookies) {
            if (excludeBookies.contains(bookie)) {
                continue;
            }
            newBookies.add(bookie);
            --ensembleSize;
            if (ensembleSize == 0) {
                return newBookies;
            }
        }

        throw new BKException.BKNotEnoughBookiesException();
    }

    @Override
    public BookieSocketAddress replaceBookie(BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        ArrayList<BookieSocketAddress> addresses = newEnsemble(1, 1, excludeBookies);
        return addresses.get(0);
    }

    @Override
    public synchronized Set<BookieSocketAddress> onClusterChanged(Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies) {
        HashSet<BookieSocketAddress> deadBookies;
        deadBookies = new HashSet<>(knownBookies);
        deadBookies.removeAll(writableBookies);
        // readonly bookies should not be treated as dead bookies
        deadBookies.removeAll(readOnlyBookies);
        knownBookies = writableBookies;
        return deadBookies;
    }

    @Override
    public EnsemblePlacementPolicy initialize(Configuration conf) {
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

}
