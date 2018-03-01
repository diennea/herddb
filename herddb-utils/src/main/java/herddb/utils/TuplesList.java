/*
 * Copyright 2018 enrico.olivelli.
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
package herddb.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A list of tuples
 *
 * @author enrico.olivelli
 */
public class TuplesList {

    public final String[] columnNames;
    public final List<DataAccessor> tuples;

    public TuplesList(String[] columnNames, List<DataAccessor> tuples) {
        this.columnNames = columnNames;
        this.tuples = tuples;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TuplesList other = (TuplesList) obj;
        if (!Arrays.deepEquals(this.columnNames, other.columnNames)) {
            return false;
        }
        if (!Objects.equals(this.tuples, other.tuples)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TuplesList{" + "columnNames=" + Arrays.toString(columnNames) + ", tuples=" + tuples + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Arrays.deepHashCode(this.columnNames);
        hash = 73 * hash + Objects.hashCode(this.tuples);
        return hash;
    }

}
