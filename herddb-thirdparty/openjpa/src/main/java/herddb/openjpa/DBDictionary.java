/*
 * Copyright 2020 eolivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.openjpa;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Set;
import java.util.TreeSet;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.identifier.DBIdentifierRule;

/**
 * OpenJPA DBDictionary for HerdDB.
 */
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class DBDictionary extends org.apache.openjpa.jdbc.sql.DBDictionary {

    private static final String DELIMITER_BACK_TICK = "`";

    private Set<String> nonCaseSensitiveReservedWords;

    public DBDictionary() {
        platform = "HerdDB";
        databaseProductName = "HerdDB";
        supportsForeignKeys = false;
        supportsUniqueConstraints = false;
        supportsCascadeDeleteAction = false;
        reservedWordSet.add("USER");
        reservedWordSet.add("VALUE");
        setSupportsDelimitedIdentifiers(true);
        setLeadingDelimiter(DELIMITER_BACK_TICK);
        setTrailingDelimiter(DELIMITER_BACK_TICK);
    }

    @Override
    protected void configureNamingRules() {
        super.configureNamingRules();
        // the DBIdentifierRule must receive a non case-sensitive set of reserved words
        // because the database uses lowercase identifiers
        nonCaseSensitiveReservedWords = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        nonCaseSensitiveReservedWords.addAll(reservedWordSet);
        DBIdentifierRule defRule = new DBIdentifierRule(DBIdentifier.DBIdentifierType.DEFAULT, nonCaseSensitiveReservedWords);
        defRule.setDelimitReservedWords(true);
        getIdentifierRules().put(defRule.getName(), defRule);
    }

    @Override
    public void endConfiguration() {
        super.endConfiguration();
        // sync nonCaseSensitiveReservedWords with selectWordSet, that is filled in in super.endConfiguration()
        nonCaseSensitiveReservedWords.addAll(selectWordSet);
    }

    @Override
    public String toDBName(DBIdentifier name, boolean delimit) {
        String result = super.toDBName(name, delimit);
        // force delimiter on reservedWords
        if (result != null && nonCaseSensitiveReservedWords.contains(result)) {
            return getNamingUtil().delimit(name.getType().name(), result);
        }
        return result;
    }

    @Override
    public String toDBName(DBIdentifier name) {
        String result = super.toDBName(name);
        if (result != null && nonCaseSensitiveReservedWords.contains(result)) {
            return getNamingUtil().delimit(name.getType().name(), result);
        }
        return result;
    }

}
