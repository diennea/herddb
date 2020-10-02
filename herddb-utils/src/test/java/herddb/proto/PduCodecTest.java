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
package herddb.proto;

import static herddb.proto.PduCodec.ObjectListReader.isDontKeepReadLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.utils.RawString;
import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests about PduCodec
 */
public class PduCodecTest {

    @Test
    public void readObjectsTrailer() throws Exception {
        long msgId = 1234;
        String ts = "dsfs";
        String query = "q";
        long scannerId = 2332;
        long tx = 353;
        List<Object> params = Arrays.asList("1", 12L, 3d);
        long statementId = 2342;
        int fetchSize = 12313;
        int maxRows = 1239;
        boolean keepReadLocks = false; // we need a TRAILER, since 0.20.0
        ByteBuf write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, keepReadLocks);
        try (Pdu pdu = PduCodec.decodePdu(write);) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_OPENSCANNER, pdu.type);
            assertEquals(ts, PduCodec.OpenScanner.readTablespace(pdu));
            assertEquals(query, PduCodec.OpenScanner.readQuery(pdu));
            assertEquals(scannerId, PduCodec.OpenScanner.readScannerId(pdu));
            assertEquals(tx, PduCodec.OpenScanner.readTx(pdu));
            assertEquals(statementId, PduCodec.OpenScanner.readStatementId(pdu));
            assertEquals(fetchSize, PduCodec.OpenScanner.readFetchSize(pdu));
            assertEquals(maxRows, PduCodec.OpenScanner.readMaxRows(pdu));
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            assertEquals(params.size(), paramsReader.getNumParams());
            assertEquals(RawString.of((String) params.get(0)), paramsReader.nextObject());
            assertEquals(params.get(1), paramsReader.nextObject());
            assertEquals(params.get(2), paramsReader.nextObject());
            byte trailer = paramsReader.readTrailer();
            assertTrue(isDontKeepReadLocks(trailer));
        }

        keepReadLocks = true; // NO TRAILER, this is what 0.19.0 clients did
        write = PduCodec.OpenScanner.write(msgId, ts, query, scannerId, tx, params, statementId, fetchSize, maxRows, keepReadLocks);
        try (Pdu pdu = PduCodec.decodePdu(write);) {
            assertEquals(msgId, pdu.messageId);
            assertEquals(Pdu.TYPE_OPENSCANNER, pdu.type);
            assertEquals(ts, PduCodec.OpenScanner.readTablespace(pdu));
            assertEquals(query, PduCodec.OpenScanner.readQuery(pdu));
            assertEquals(scannerId, PduCodec.OpenScanner.readScannerId(pdu));
            assertEquals(tx, PduCodec.OpenScanner.readTx(pdu));
            assertEquals(statementId, PduCodec.OpenScanner.readStatementId(pdu));
            assertEquals(fetchSize, PduCodec.OpenScanner.readFetchSize(pdu));
            assertEquals(maxRows, PduCodec.OpenScanner.readMaxRows(pdu));
            PduCodec.ObjectListReader paramsReader = PduCodec.OpenScanner.startReadParameters(pdu);
            assertEquals(params.size(), paramsReader.getNumParams());
            assertEquals(RawString.of((String) params.get(0)), paramsReader.nextObject());
            assertEquals(params.get(1), paramsReader.nextObject());
            assertEquals(params.get(2), paramsReader.nextObject());
            byte trailer = paramsReader.readTrailer();
            assertEquals(0, trailer);
            assertFalse(isDontKeepReadLocks(trailer));
        }

    }
}
