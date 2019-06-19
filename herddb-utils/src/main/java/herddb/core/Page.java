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
 * A generic page loaded in memory
 *
 * @author diego.salvi
 */
public abstract class Page<O extends Page.Owner> {

    /**
     * The page owner: responsible for real page unload (data dereference)
     *
     * @author diego.salvi
     */
    public static interface Owner {

        /**
         * Unload given page from memory.
         *
         * @param pageId id of page to be unloaded
         */
        public void unload(long pageId);

    }

    /**
     * Page metadata for {@link PageReplacementPolicy} use.
     *
     * @author diego.salvi
     */
    public static class Metadata {

        public final Page.Owner owner;
        public final long pageId;

        public Metadata(Page.Owner owner, long pageId) {
            super();

            this.owner = owner;
            this.pageId = pageId;
        }

    }

    /** Owner of the page */
    public final O owner;

    /** Page id absolute on the owner */
    public final long pageId;

    /** Page metadata for {@link PageReplacementPolicy} use */
    Metadata metadata;

    public Page(O owner, long pageId) {
        super();
        this.owner = owner;
        this.pageId = pageId;
    }

}
