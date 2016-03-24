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
package herddb.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of Tables. All tables in the same TableSpace are handled by the same
 * server at once. All the operations on the tables of a TableSet share the same
 * transaction log, so a transaction can span only tables which belong to the
 * same TableSapce.
 *
 * @author enrico.olivelli
 */
public class TableSpace {

    public static final String DEFAULT = "default";
    public final String name;
    /**
     * Leader Node for the TableSpace
     */
    public final String leaderId;
    /**
     * Nodes which contain data for the TableSpace. the leaderId MUST be
     * contained in this set
     */
    public final Set<String> replicas;

    private TableSpace(String name, String leaderId, Set<String> replicas) {
        this.name = name;
        this.leaderId = leaderId;
        this.replicas = replicas;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Set<String> replicas = new HashSet<>();
        private String name;
        private String leaderId;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder replica(String id) {
            this.replicas.add(id);
            return this;
        }
        public Builder replicas(Set<String> replicas) {
            this.replicas.addAll(replicas);
            return this;
        }

        public Builder leader(String id) {
            this.leaderId = id;
            return this;
        }

        public TableSpace build() {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name is not defined");
            }
            if (replicas.isEmpty()) {
                throw new IllegalArgumentException("not replica defined");
            }
            if (leaderId == null || leaderId.isEmpty()) {
                leaderId = replicas.iterator().next();
            }
            if (!replicas.contains(leaderId)) {
                throw new IllegalArgumentException("leader " + leaderId + " must be in replica list " + replicas);
            }
            return new TableSpace(name, leaderId, Collections.unmodifiableSet(replicas));
        }

    }
}
