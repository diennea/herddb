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
package herddb.client;

import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.proto.flatbuf.MessageType;
import herddb.proto.flatbuf.Response;

/**
 * Generic client side exception
 *
 * @author enrico.olivelli
 */
public class HDBException extends Exception {

    public HDBException(String message) {
        super(message);
    }

    public HDBException(Response reply) {
        super(reply.type() == MessageType.TYPE_ERROR
                ? reply.error() + "" : reply + "",
                new Exception("server-side-error:" + reply));

    }

    public HDBException(Pdu reply) {
        super(reply.type == Pdu.TYPE_ERROR
                ? PduCodec.ErrorResponse.readError(reply) : reply + "");

    }

    public HDBException(Throwable cause) {
        super(cause);
    }

    public HDBException(String message, Throwable cause) {
        super(message, cause);
    }

}
