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

package herddb.core;

/**
 * Runs the {@link MemoryManager} page pool sizing checks on the CLOCK-Pro page replacement policy, the default choice.
 *
 * @author diego.salvi
 */
public class MemoryManagerTest extends MemoryManagerSuite {

    static {
        System.setProperty(PAGE_REPLACEMENT_POLICY_PROPERTY, "cp");
    }

    @Override
    protected Class<? extends PageReplacementPolicy> expectedPolicyImplementation() {
        return ClockProPolicy.class;
    }

}
