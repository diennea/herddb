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

import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookiesHealthInfo;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.LocalBookiesRegistry;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Copied from DefaultEnsemblePlacementPolicy
 *
 * @author francesco.caliumi
 */
public class PreferLocalBookiePlacementPolicy implements EnsemblePlacementPolicy {

    private Set<BookieSocketAddress> knownBookies = new HashSet<>();

    @Override
    public EnsemblePlacementPolicy initialize(
            ClientConfiguration conf,
            Optional<DNSToSwitchMapping> optionalDnsResolver,
            HashedWheelTimer hashedWheelTimer,
            FeatureProvider featureProvider, StatsLogger statsLogger
    ) {
        return this;
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    @Override
    public synchronized Set<BookieSocketAddress> onClusterChanged(
            Set<BookieSocketAddress> writableBookies,
            Set<BookieSocketAddress> readOnlyBookies
    ) {
        HashSet<BookieSocketAddress> deadBookies;
        deadBookies = new HashSet<>(knownBookies);
        deadBookies.removeAll(writableBookies);
        // readonly bookies should not be treated as dead bookies
        deadBookies.removeAll(readOnlyBookies);
        knownBookies = writableBookies;
        return deadBookies;
    }

    @Override
    public PlacementResult<BookieSocketAddress> replaceBookie(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Map<String, byte[]> customMetadata,
            List<BookieSocketAddress> currentEnsemble,
            BookieSocketAddress bookieToReplace,
            Set<BookieSocketAddress> excludeBookies
    ) throws BKNotEnoughBookiesException {
        excludeBookies.addAll(currentEnsemble);
        PlacementResult<List<BookieSocketAddress>> list = newEnsemble(1, 1, 1, customMetadata, excludeBookies);
        return PlacementResult.of(list.getResult().get(0), PlacementPolicyAdherence.MEETS_STRICT);
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(List<BookieSocketAddress> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(List<BookieSocketAddress> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return writeSet;
    }

    @Override
    public void registerSlowBookie(BookieSocketAddress bookieSocketAddress, long entryId) {
    }

    @Override
    public PlacementResult<List<BookieSocketAddress>> newEnsemble(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Map<String, byte[]> customMetadata,
            Set<BookieSocketAddress> excludeBookies
    )
            throws BKNotEnoughBookiesException {

        ArrayList<BookieSocketAddress> newBookies = new ArrayList<>(ensembleSize);
        if (ensembleSize <= 0) {
            return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
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
            if (LocalBookiesRegistry.isLocalBookie(bookie)) {
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
                return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
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
                return PlacementResult.of(newBookies, PlacementPolicyAdherence.MEETS_STRICT);
            }
        }

        throw new BKException.BKNotEnoughBookiesException();
    }

    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieSocketAddress> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        return PlacementPolicyAdherence.MEETS_STRICT;
    }

}
