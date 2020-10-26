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
    public static final int ACTION_CASCADE = 1;
    public static final int ACTION_SETNULL = 2;
    public final String name;
    public final String parentTableId; // uuid, not name
    public final String[] columns;
    public final String[] parentTableColumns;
    public final int onUpdateAction;
    public final int onDeleteAction;

    public static Builder builder() {
        return new Builder();
    }

    private ForeignKeyDef(String name, String parentTableId, String[] columns, String[] parentTableColumns, int onUpdateAction, int onDeleteAction) {
        this.name = name;
        this.parentTableId = parentTableId;
        this.columns = columns;
        this.parentTableColumns = parentTableColumns;
        this.onUpdateAction = onUpdateAction;
        this.onDeleteAction = onDeleteAction;
    }

    public static class Builder {

        private String name = UUID.randomUUID().toString();
        private String parentTableId; // uuid, not name
        private final List<String> columns = new ArrayList<>();
        private final List<String> parentTableColumns = new ArrayList<>();
        private int onUpdateAction;
        private int onDeleteAction;


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

        public Builder onUpdateAction(int onUpdateAction) {
            this.onUpdateAction = onUpdateAction;
            return this;
        }

        public Builder onDeleteAction(int onDeleteCascadeAction) {
            this.onDeleteAction = onDeleteCascadeAction;
            return this;
        }

        public ForeignKeyDef build() {
            if (parentTableId == null || parentTableId.isEmpty()) {
                throw new IllegalArgumentException("parentTableId must be set");
            }
            if (onUpdateAction != ACTION_NO_ACTION
                    && onUpdateAction != ACTION_SETNULL) {
                throw new IllegalArgumentException("invalid onUpdateAction " + onDeleteAction);
            }
            if (onDeleteAction != ACTION_NO_ACTION
                    && onDeleteAction != ACTION_CASCADE
                    && onDeleteAction != ACTION_SETNULL) {
                throw new IllegalArgumentException("invalid onDeleteAction " + onDeleteAction);
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
                    onUpdateAction, onDeleteAction);
        }

    }
}
