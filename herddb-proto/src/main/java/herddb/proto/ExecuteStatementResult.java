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
package herddb.proto;

/**
 * Result of the execution of a statement
 *
 * @author enrico.olivelli
 */
public class ExecuteStatementResult extends Response {

    static int OWN_SIZE = 8 + 8;

    public long updateCount;
    public long tx;
}
