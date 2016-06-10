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
package herddb.file;

import herddb.core.MaterializedRecordSet;
import herddb.core.RecordSetFactory;
import herddb.model.Column;
import java.nio.file.Path;

/**
 * Factory which holds RecordSet in Memory only
 *
 * @author enrico.olivelli
 */
public class FileRecordSetFactory extends RecordSetFactory {

    Path tmpDirectory;
    private final int swapThreshold;

    public FileRecordSetFactory(Path tmpDirectory, int swapThreshold) {
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
    }

    @Override
    public MaterializedRecordSet createRecordSet(Column[] columns) {
        return new FileRecordSet(-1, swapThreshold, columns, this);
    }

    @Override
    public MaterializedRecordSet createFixedSizeRecordSet(int size, Column[] columns) {
        return new FileRecordSet(size, swapThreshold, columns, this);
    }
}
