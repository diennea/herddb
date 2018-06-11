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
package herddb.benchs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.Callable;
import javax.sql.DataSource;

/**
 *
 * @author enrico.olivelli
 */
public class UpdateByPKOperation extends Operation {

    @Override
    public Callable<Void> newInstance(int seed, int batchSize, DataSource dataSource) throws Exception {
        if (batchSize <= 0) {
            throw new IllegalArgumentException();
        }
        return () -> {
            try (Connection con = dataSource.getConnection()) {
                if (batchSize > 1) {
                    con.setAutoCommit(false);
                }
                try (PreparedStatement ps = con.prepareStatement(BaseTableDefinition.UPDATE_BY_PK)) {
                    ps.setString(1, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                        + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                        + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                        + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                        + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + seed);
                    for (int i = 0; i < batchSize; i++) {
                        String pk = "pk" + seed;
                        ps.setString(2, pk);
                        int ucount = ps.executeUpdate();
                        if (ucount <= 0) {
                            throw new RuntimeException("row " + pk + " not updated");
                        }
                    }
                }
                if (batchSize > 1) {
                    con.commit();
                }
            }
            return null;
        };
    }

}
