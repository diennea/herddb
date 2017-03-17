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
package herddb.cli;

import java.util.ArrayList;
import java.util.List;
import static org.codehaus.groovy.ast.tools.GeneralUtils.params;

/**
 * Rewrites MySQL dump INSERT statements in order to leverage PreparedStatement feature, and so "cache" the execution
 * plan
 *
 * @author enrico.olivelli
 */
public class MySqlDumpInsertStatementRewriter {

    static final int STATE_START_RECORD = 0;
    static final int STATE_IN_VALUE = 1;
    static final int STATE_IN_STRING_VALUE = 2;
    static final int STATE_IN_NUMERIC_VALUE = 3;
    static final int STATE_IN_NULL_VALUE = 4;
    static final int STATE_IN_NULL_VALUE_1 = 40;
    static final int STATE_IN_NULL_VALUE_2 = 41;
    static final int STATE_IN_NULL_VALUE_3 = 42;
    static final int STATE_END_VALUE = 5;

    public static QueryWithParameters rewriteSimpleInsertStatement(String query) {
        if (!query.startsWith("INSERT INTO ")) {
            return null;
        }
        int startValues = query.indexOf("VALUES ");
        if (startValues <= 0) {
            return null;
        }
        List<Object> jdbcParameters = new ArrayList<>();
        int pos = startValues + "VALUES ".length();
        StringBuilder rewritten = new StringBuilder(query.substring(0, pos));
        StringBuilder currentValue = new StringBuilder();
        int end = query.length();

        int state = STATE_START_RECORD;
        while (pos < end) {
            char c = query.charAt(pos++);
            switch (state) {
                case STATE_START_RECORD: {
                    if (c == '(') {
                        rewritten.append(c);
                        state = STATE_IN_VALUE;
                    } else if (c == ',') {
                        rewritten.append(c);
                        state = STATE_START_RECORD;
                    } else {
                        return null;
                    }
                    break;
                }
                case STATE_IN_VALUE: {
                    if (c == '\'') {
                        state = STATE_IN_STRING_VALUE;
                        currentValue.setLength(0);
                    } else if (Character.isDigit(c)) {
                        state = STATE_IN_NUMERIC_VALUE;
                        currentValue.setLength(0);
                        currentValue.append(c);
                    } else if (c == 'N' || c == 'n') {
                        state = STATE_IN_NULL_VALUE;
                        currentValue.setLength(0);
                    } else {
                        return null;
                    }
                    break;
                }
                case STATE_IN_NULL_VALUE: {
                    if (c == 'U' || c == 'u') {
                        state = STATE_IN_NULL_VALUE_1;
                    } else {
                        return null;
                    }
                    break;
                }
                case STATE_IN_NULL_VALUE_1: {
                    if (c == 'L' || c == 'l') {
                        state = STATE_IN_NULL_VALUE_2;
                    } else {
                        return null;
                    }
                    break;
                }
                case STATE_IN_NULL_VALUE_2: {
                    if (c == 'L' || c == 'l') {
                        state = STATE_IN_NULL_VALUE_3;
                    } else {
                        return null;
                    }
                    break;
                }
                case STATE_IN_NULL_VALUE_3: {
                    if (c == ',') {
                        jdbcParameters.add(null);
                        rewritten.append("?,");
                        currentValue.setLength(0);
                        state = STATE_IN_VALUE;
                        break;
                    } else if (c == ')') {
                        jdbcParameters.add(null);
                        rewritten.append("?)");
                        currentValue.setLength(0);
                        state = STATE_START_RECORD;
                        break;
                    } else {
                        currentValue.append(c);
                    }
                    break;
                }
                case STATE_IN_STRING_VALUE: {
                    if (c == '\'') {
                        if (query.charAt(pos) == '\'') {
                            // sql escape of '' syntax
                            currentValue.append('\'');
                            pos++;
                        } else {
                            jdbcParameters.add(currentValue.toString());
                            rewritten.append('?');
                            currentValue.setLength(0);
                            state = STATE_END_VALUE;
                        }
                        break;
                    } else {
                        currentValue.append(c);
                    }
                    break;
                }
                case STATE_IN_NUMERIC_VALUE: {
                    if (c == ',') {
                        Number value;
                        try {
                            value = Long.parseLong(currentValue.toString());
                        } catch (NumberFormatException bigNumber) {
                            try {
                                value = Double.parseDouble(currentValue.toString());
                            } catch (NumberFormatException noAgain) {
                                return null;
                            }
                        }
                        jdbcParameters.add(value);
                        rewritten.append("?,");
                        currentValue.setLength(0);
                        state = STATE_IN_VALUE;
                        break;
                    } else if (c == ')') {
                        Number value;
                        try {
                            value = Long.parseLong(currentValue.toString());
                        } catch (NumberFormatException bigNumber) {
                            try {
                                value = Double.parseDouble(currentValue.toString());
                            } catch (NumberFormatException noAgain) {
                                return null;
                            }
                        }
                        jdbcParameters.add(value);
                        rewritten.append("?)");
                        currentValue.setLength(0);
                        state = STATE_START_RECORD;
                        break;
                    } else {
                        currentValue.append(c);
                    }
                    break;
                }
                case STATE_END_VALUE:
                    if (c == ',') {
                        rewritten.append(',');
                        state = STATE_IN_VALUE;
                    } else if (c == ')') {
                        rewritten.append(')');
                        state = STATE_START_RECORD;
                    } else {
                        return null;
                    }
                    break;
                default:
                    throw new IllegalStateException(state + " at pos:" + pos);

            }
        }
        if (state != STATE_START_RECORD) {
            return null;
        }
        return new QueryWithParameters(rewritten.toString(), jdbcParameters, null);
    }
}
