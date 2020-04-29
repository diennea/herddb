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
 * Handle fake locks by key in order to avoid useless concurrent access management.
 *
 * @author paolo.venturi
 */
public class NullLockManager implements ILocalLockManager {

    private static final LockHandle NULL_HANDLE = new LockHandle(0, null, true);

    @Override
    public LockHandle acquireWriteLockForKey(Bytes key) {
        return NULL_HANDLE;
    }

    @Override
    public void releaseWriteLock(LockHandle handle) {
    }

    @Override
    public LockHandle acquireReadLockForKey(Bytes key) {
        return NULL_HANDLE;
    }

    @Override
    public void releaseReadLock(LockHandle handle) {
    }

    @Override
    public void releaseLock(LockHandle handle) {
    }

    @Override
    public void clear() {
    }

    @Override
    public int getNumKeys() {
        return 0;
    }

}
