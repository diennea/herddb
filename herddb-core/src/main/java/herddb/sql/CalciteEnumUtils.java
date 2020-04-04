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

package herddb.sql;

import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.rel.core.JoinRelType;

public class CalciteEnumUtils {

    public static JoinType toLinq4jJoinType(JoinRelType joinRelType) {
        switch (joinRelType) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT;
            case RIGHT:
                return JoinType.RIGHT;
            case FULL:
                return JoinType.FULL;
            case SEMI:
                return JoinType.SEMI;
            case ANTI:
                return JoinType.ANTI;
            default:
                throw new IllegalStateException(
                        "Unable to convert " + joinRelType + " to Linq4j JoinType");
        }
    }
}
