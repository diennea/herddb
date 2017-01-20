/*
 * Copyright 2017 enrico.olivelli.
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
package herddb.benchs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import javax.sql.DataSource;

/**
 *
 * @author enrico.olivelli
 */
public class SelectByPKOperation extends Operation {

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
                try (PreparedStatement ps = con.prepareStatement(BaseTableDefinition.SELECT_PK)) {
                    for (int i = 0; i < batchSize; i++) {
                        String pk = "pk" + seed;
                        ps.setString(1, pk);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) {
                                throw new RuntimeException("record " + pk + " not found");
                            }
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
