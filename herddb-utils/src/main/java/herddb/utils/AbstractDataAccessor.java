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

import java.util.Objects;

/**
 * Base class for DataAccessors, provide default implementations
 */
public abstract class AbstractDataAccessor implements DataAccessor {

//    @Override
//    public boolean equals(Object obj) {
//        if (obj instanceof DataAccessor) {
//            DataAccessor da = (DataAccessor) obj;
//            int size = this.getNumFields();
//            int size2 = da.getNumFields();
//            if (size != size2) {
//                return false;
//            }
//            // leverage zero-copy and to not create temporary arrays
//            for (int i = 0; i < size; i++) {
//                if (!Objects.equals(get(i), da.get(i))) {
//                    return false;
//                }
//            }
//            return true;
//        }
//        return false;
//    }
//    
//    public String toString(){
//        return this.getClass().getSimpleName();
//    }
//
//    private int _hashcode = Integer.MIN_VALUE;
//
//    @Override
//    public int hashCode() {
//        if (_hashcode == Integer.MIN_VALUE) {
//            int size = this.getNumFields();
//            int res = 0;
//            // leverage zero-copy and to not create temporary arrays
//            for (int i = 0; i < size; i++) {
//                res += Objects.hashCode(get(i));
//            }
//            _hashcode = res;
//        }
//        return _hashcode;
//    }

}
