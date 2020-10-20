/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Definition of a ForeignKey constaint.
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public final class ForeignKeyDef {

    public static final int ACTION_NO_ACTION = 0; // NO_ACTION means to reject the operation
    public final String name;
    public final String parentTableId; // uuid, not name
    public final String[] columns;
    public final String[] parentTableColumns;
    public final int onUpdateCascadeAction;
    public final int onDeleteCascadeAction;

    public static Builder builder() {
        return new Builder();
    }

    private ForeignKeyDef(String name, String parentTableId, String[] columns, String[] parentTableColumns, int onUpdateCascadeAction, int onDeleteCascadeAction) {
        this.name = name;
        this.parentTableId = parentTableId;
        this.columns = columns;
        this.parentTableColumns = parentTableColumns;
        this.onUpdateCascadeAction = onUpdateCascadeAction;
        this.onDeleteCascadeAction = onDeleteCascadeAction;
    }

    public static class Builder {

        private String name = UUID.randomUUID().toString();
        private String parentTableId; // uuid, not name
        private final List<String> columns = new ArrayList<>();
        private final List<String> parentTableColumns = new ArrayList<>();
        private int onUpdateCascadeAction;
        private int onDeleteCascadeAction;


        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder parentTableId(String parentTableId) {
            this.parentTableId = parentTableId;
            return this;
        }

        public Builder column(String column) {
            this.columns.add(column);
            return this;
        }

        public Builder parentTableColumn(String parentTableColumn) {
            this.parentTableColumns.add(parentTableColumn);
            return this;
        }

        public Builder onUpdateCascadeAction(int onUpdateCascadeAction) {
            this.onUpdateCascadeAction = onUpdateCascadeAction;
            return this;
        }

        public Builder onDeleteCascadeAction(int onDeleteCascadeAction) {
            this.onDeleteCascadeAction = onDeleteCascadeAction;
            return this;
        }

        public ForeignKeyDef build() {
            if (parentTableId == null || parentTableId.isEmpty()) {
                throw new IllegalArgumentException("parentTableId must be set");
            }
            if (onUpdateCascadeAction != ACTION_NO_ACTION) {
                throw new IllegalArgumentException("invalid onUpdateCascadeAction " + onUpdateCascadeAction);
            }
            if (onDeleteCascadeAction != ACTION_NO_ACTION) {
                throw new IllegalArgumentException("invalid onDeleteCascadeAction " + onDeleteCascadeAction);
            }
            if (parentTableColumns.size() != columns.size()) {
                throw new IllegalArgumentException("the number of columns in child and parent table must be the same");
            }
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("a foreign key constaint must refer to at least one column");
            }
            return new ForeignKeyDef(name, parentTableId,
                    columns.toArray(new String[0]),
                    parentTableColumns.toArray(new String[0]),
                    onUpdateCascadeAction, onDeleteCascadeAction);
        }

    }
}
