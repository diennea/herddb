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

import java.io.IOException;
import java.io.Reader;
import java.util.function.Consumer;

/**
 * Breaks an SQL file into statements
 *
 * @author enrico.olivelli
 */
public class SQLFileParser {

    private static enum State {
        OUT,
        INSQL,
        INSTRING,
        INSTRING_ESCAPE,
        INSTRING_MYSQLESCAPE,
        SINGLELINECOMMENT_FIRSTCHAR,
        SINGLELINECOMMENT,
        MULTILINECOMMENT_FIRSTCHAR,
        MULTILINECOMMENT,
        MULTILINECOMMENT_STARTEND
    }

    public static interface StatementAcceptor {

        public void accept(Statement statement) throws Exception;
    }

    public static final class Statement {

        public final String content;
        public boolean comment;

        public Statement(String content, boolean comment) {
            this.content = content;
            this.comment = comment;
        }

    }

    private static final int EOF = -1;

    public static void parseSQLFile(Reader stream, StatementAcceptor statements) throws Exception {
        StringBuilder current = new StringBuilder();
        State state = State.OUT;
        while (true) {
            int _c = stream.read();
            char c = (char) _c;
            switch (state) {
                case OUT: {
                    if (c == '\n' || c == '\r' || c == ';') {
                        // swallow
                    } else if (c == '-') {
                        state = State.SINGLELINECOMMENT_FIRSTCHAR;
                        current.append(c);
                    } else if (c == '/') {
                        state = State.MULTILINECOMMENT_FIRSTCHAR;
                        current.append(c);
                    } else {
                        if (_c == EOF) {
                            return;
                        }
                        state = State.INSQL;
                        current.append(c);
                    }
                    break;
                }
                case SINGLELINECOMMENT_FIRSTCHAR: {
                    if (c == '-') {
                        state = State.SINGLELINECOMMENT;
                        current.append(c);
                    } else {
                        throw new IOException("Unparsable SQL, found '" + c + "' after a '-'");
                    }
                    break;
                }
                case MULTILINECOMMENT_FIRSTCHAR: {
                    if (c == '*') {
                        state = State.MULTILINECOMMENT;
                    } else {
                        throw new IOException("Unparsable SQL, found '" + c + "' after a '/'");
                    }
                    break;
                }
                case SINGLELINECOMMENT: {
                    if (c == '\n' || _c == EOF) {
                        state = State.OUT;
                        statements.accept(new Statement(current.toString(), true));
                        current.setLength(0);
                        if (_c == EOF) {
                            return;
                        }
                    } else {
                        current.append(c);
                    }
                    break;
                }
                case MULTILINECOMMENT: {
                    if (_c == EOF) {
                        state = State.OUT;
                        statements.accept(new Statement(current.toString(), true));
                        current.setLength(0);
                        if (_c == EOF) {
                            return;
                        }
                    } else if (c == '*') {
                        state = State.MULTILINECOMMENT_STARTEND;
                        current.append(c);
                    } else {
                        current.append(c);
                    }
                    break;
                }
                case MULTILINECOMMENT_STARTEND: {
                    if (c == '/' || _c == EOF) {
                        if (_c != EOF) {
                            current.append(c);
                        }
                        state = State.OUT;
                        statements.accept(new Statement(current.toString(), true));
                        current.setLength(0);
                    } else {
                        state = State.MULTILINECOMMENT;
                        current.append(c);
                    }
                    break;
                }
                case INSQL: {
                    if (_c == EOF) {
                        state = State.OUT;
                        statements.accept(new Statement(current.toString(), false));
                        current.setLength(0);
                        return;
                    } else if (c == ';') {
                        state = State.OUT;
                        statements.accept(new Statement(current.toString(), false));
                        current.setLength(0);
                    } else if (c == '\'') {
                        current.append(c);
                        state = State.INSTRING;
                    } else {
                        current.append(c);
                    }
                    break;
                }
                case INSTRING: {
                    if (_c == EOF) {
                        throw new IllegalStateException(state + ", buffer is " + current + ". found an EOF");
                    } else if (c == '\'') {
                        state = State.INSTRING_ESCAPE;
                        current.append(c);
                    } else if (c == '\\') {
                        state = State.INSTRING_MYSQLESCAPE;
                        // we are rewriting \' to ''
                    } else {
                        current.append(c);
                    }
                    break;
                }
                case INSTRING_ESCAPE: {
                    if (_c == EOF) {
                        throw new IllegalStateException(state + ", buffer is " + current + ". found an EOF");
                    } else if (c == ',' || c == ')') {
                        current.append(c);
                        state = State.INSQL;
                    } else {
                        current.append(c);
                        state = State.INSTRING;
                    }
                    break;
                }
                case INSTRING_MYSQLESCAPE: {
                    if (_c == EOF) {
                        throw new IllegalStateException(state + ", buffer is " + current + ". found an EOF");
                    } else if (c == '\'') {
                        current.append('\'');
                        // we are rewriting \' to ''
                        current.append(c);
                        state = State.INSTRING;
                    } else {
                        current.append(c);
                        state = State.INSTRING;
                    }
                    break;
                }
                default:
                    throw new IllegalStateException(state + ", buffer is " + current);

            }
        }
    }
}
