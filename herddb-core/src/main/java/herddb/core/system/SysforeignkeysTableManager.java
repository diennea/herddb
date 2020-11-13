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

package herddb.core.system;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.ForeignKeyDef;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.ArrayList;
import java.util.List;

/**
 * Table Manager for the sysforeignkeys virtual table
 *
 * @author enrico.olivelli
 */
public class SysforeignkeysTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("sysforeignkeys")
            .column("child_table_name", ColumnTypes.STRING)
            .column("child_table_column_name", ColumnTypes.STRING)
            .column("child_table_cons_name", ColumnTypes.STRING)
            .column("parent_table_name", ColumnTypes.STRING)
            .column("parent_table_column_name", ColumnTypes.STRING)
            .column("on_delete_action", ColumnTypes.STRING)
            .column("on_update_action", ColumnTypes.STRING)
            .column("ordinal_position", ColumnTypes.INTEGER)
            .column("deferred", ColumnTypes.STRING)
            .primaryKey("child_table_name", false)
            .primaryKey("child_table_column_name", false)
            .primaryKey("child_table_cons_name", false)
            .build();

    public SysforeignkeysTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList(Transaction transaction) {
        List<Table> tables = tableSpaceManager.getAllVisibleTables(transaction);
        List<Record> result = new ArrayList<>();
        for (Table child : tables) {
            if (child.foreignKeys == null) {
                continue;
            }
            String child_table_name = child.name;
            for (ForeignKeyDef fk : child.foreignKeys) {
                Table parent =
                        tables.stream()
                                .filter(ta -> ta.uuid.equals(fk.parentTableId))
                                .findAny()
                                .orElse(null);
                if (parent == null) {
                    continue;
                }
                for (int i = 0; i < fk.columns.length; i++) {
                String child_column_name = fk.columns[i];
                String parent_column_name = fk.parentTableColumns[i];
                String parent_table_name = parent.name;
                String on_delete_action;
                switch (fk.onDeleteAction) {
                    case ForeignKeyDef.ACTION_CASCADE:
                        on_delete_action = "importedKeyCascade";
                        break;
                    case ForeignKeyDef.ACTION_NO_ACTION:
                        on_delete_action = "importedNoAction";
                        break;
                    case ForeignKeyDef.ACTION_SETNULL:
                        on_delete_action = "importedKeySetNull";
                        break;
                    default:
                        on_delete_action = "importedKeyCascade";
                        break;
                }
                String on_update_action;
                switch (fk.onUpdateAction) {
                    case ForeignKeyDef.ACTION_CASCADE:
                        on_update_action = "importedKeyCascade";
                        break;
                    case ForeignKeyDef.ACTION_NO_ACTION:
                        on_update_action = "importedNoAction";
                        break;
                    case ForeignKeyDef.ACTION_SETNULL:
                        on_update_action = "importedKeySetNull";
                        break;
                    default:
                        on_update_action = "importedKeyCascade";
                        break;
                }
                result.add(RecordSerializer.makeRecord(
                        table,
                        "child_table_name", child_table_name,
                        "child_table_column_name", child_column_name,
                        "child_table_cons_name", fk.name,
                        "parent_table_name", parent_table_name,
                        "parent_table_column_name", parent_column_name,
                        "on_delete_action", on_delete_action,
                        "on_update_action", on_update_action,
                        "ordinal_position", (i + 1),
                        "deferred", "importedKeyNotDeferrable"
                ));
                }
            }
        }
        return result;
    }

}
