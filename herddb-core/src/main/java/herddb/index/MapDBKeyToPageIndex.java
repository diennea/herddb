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
package herddb.index;

import herddb.utils.Bytes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * Use MapDB to store the index
 *
 * @author enrico.olivelli
 */
public class MapDBKeyToPageIndex extends ConcurrentMapKeyToPageIndex {

    private static HTreeMap<Bytes, Long> create(Path tmpDirectory, String tmpName) throws IOException {
        File temporarySwapFile = File.createTempFile("keysswap." + tmpName, ".bin", tmpDirectory.toFile());
        DB db = DBMaker
                .newFileDB(temporarySwapFile)
                .cacheLRUEnable()
                .asyncWriteEnable()
                .transactionDisable()
                .commitFileSyncDisable()
                .deleteFilesAfterClose()
                .make();
        return db.createHashMap("keys")
                .keySerializer(new BytesSerializer())
                .make();
    }

    private static final class BytesSerializer implements Serializable, Serializer<Bytes> {

        private static final long serialVersionUID = 0;

        @Override
        public void serialize(DataOutput arg0, Bytes arg1) throws IOException {
            arg0.writeInt(arg1.data.length);
            arg0.write(arg1.data);
        }

        @Override
        public Bytes deserialize(DataInput arg0, int arg1) throws IOException {
            int len = arg0.readInt();
            byte[] data = new byte[len];
            arg0.readFully(data);
            return Bytes.from_array(data);
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    }

    public MapDBKeyToPageIndex(Path tmpDirectory, String tmpName) throws IOException {
        super(create(tmpDirectory, tmpName));
    }

    @Override
    public void close() {
        HTreeMap treeMap = (HTreeMap) getMap();
        treeMap.close();
    }

}
