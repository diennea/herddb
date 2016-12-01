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
package herddb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.Projection;
import herddb.model.ScanLimits;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Tuple;
import herddb.model.TupleComparator;

/**
 * Suite of tests on RecordSet
 *
 * @author enrico.olivelli
 */
public abstract class RecordSetSuite {

    protected abstract RecordSetFactory buildRecordSetFactory(int swapSize);

    @Test
    public void testSimpleNoSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(Integer.MAX_VALUE);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {
            Set<String> expected_s1 = new HashSet<>();
            Set<Integer> expected_n1 = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                expected_s1.add(s1);
                expected_n1.add(i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();
            for (Tuple t : rs) {
                expected_s1.remove((String) t.get("s1"));
                expected_n1.remove((Integer) t.get("n1"));
            }
            assertTrue(expected_n1.isEmpty());
            assertTrue(expected_s1.isEmpty());
        }

    }

    @Test
    public void testSimpleSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(1);
        Column[] columns = new Column[6];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);
        columns[2] = Column.column("t1", ColumnTypes.TIMESTAMP);
        columns[3] = Column.column("i1", ColumnTypes.INTEGER);
        columns[4] = Column.column("b1", ColumnTypes.BYTEARRAY);
        columns[5] = Column.column("null1", ColumnTypes.STRING);

        java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {
            Set<String> expected_s1 = new HashSet<>();
            Set<Long> expected_n1 = new HashSet<>();
            Set<Integer> expected_i1 = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", Long.valueOf(i));
                record.put("i1", Integer.valueOf(i));
                record.put("t1", ts);
                record.put("null1", null);
                record.put("b1", s1.getBytes(StandardCharsets.UTF_8));
                expected_s1.add(s1);
                expected_n1.add(Long.valueOf(i));
                expected_i1.add(i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();
            for (Tuple t : rs) {
                expected_s1.remove((String) t.get("s1"));
                expected_n1.remove((Long) t.get("n1"));
                expected_i1.remove((Integer) t.get("i1"));
                assertEquals(ts, t.get("t1"));
                assertNull(t.get("null1"));
            }
            assertTrue(expected_n1.isEmpty());
            assertTrue(expected_s1.isEmpty());
        }

    }

    @Test
    public void testLimitsSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(1);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {
            Set<String> expected_s1 = new HashSet<>();
            Set<Integer> expected_n1 = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                if (i >= 10 && i < 30) {
                    expected_s1.add(s1);
                    expected_n1.add(i);
                }
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            rs.applyLimits(new ScanLimits(20, 10));

            for (Tuple t : rs) {
                expected_s1.remove((String) t.get("s1"));
                expected_n1.remove((Integer) t.get("n1"));
            }
            assertTrue(expected_n1.isEmpty());
            assertTrue(expected_s1.isEmpty());
        }

    }

    @Test
    public void testLimitsNoSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(Integer.MAX_VALUE);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {
            Set<String> expected_s1 = new HashSet<>();
            Set<Integer> expected_n1 = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                if (i >= 10 && i < 30) {
                    expected_s1.add(s1);
                    expected_n1.add(i);
                }
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            rs.applyLimits(new ScanLimits(20, 10));

            for (Tuple t : rs) {
                expected_s1.remove((String) t.get("s1"));
                expected_n1.remove((Integer) t.get("n1"));
            }
            assertTrue(expected_n1.isEmpty());
            assertTrue(expected_s1.isEmpty());
        }

    }

    @Test
    public void testLimitsAfterEndSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(1);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {
            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            rs.applyLimits(new ScanLimits(20, 100000));

            for (Tuple t : rs) {
                fail();
            }

        }

    }

    @Test
    public void testSortNoSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(Integer.MAX_VALUE);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {

            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            // sort descending
            rs.sort(new TupleComparator() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    return ((Integer) o2.get((1))).compareTo((Integer) o1.get(1));
                }
            });

            int last = Integer.MAX_VALUE;
            for (Tuple t : rs) {
                int n1 = (Integer) t.get("n1");
                assertTrue(last > n1);
                last = n1;
            }

        }

    }

    @Test
    public void testSortSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(1);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);

        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {

            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            // sort descending
            rs.sort(new TupleComparator() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    return ((Integer) o2.get((1))).compareTo((Integer) o1.get(1));
                }
            });

            int last = Integer.MAX_VALUE;
            for (Tuple t : rs) {
                int n1 = (Integer) t.get("n1");
                assertTrue(last > n1);
                last = n1;
            }

        }

    }

    @Test
    public void testApplyProjectionSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(1);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);
        Set<String> expected_s2 = new HashSet<>();
        Set<Integer> expected_n2 = new HashSet<>();
        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {

            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                expected_s2.add(s1);
                expected_n2.add(i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            Column[] columns_projected = new Column[2];
            columns_projected[0] = Column.column("n2", ColumnTypes.LONG);
            columns_projected[1] = Column.column("s2", ColumnTypes.STRING);
            String[] fieldNames_projected = new String[]{"n2", "s2"};

            rs.applyProjection(new Projection() {
                @Override
                public Column[] getColumns() {
                    return columns_projected;
                }

                @Override
                public Tuple map(Tuple tuple, StatementEvaluationContext context) throws StatementExecutionException {
                    Object[] projected_values = new Object[2];
                    projected_values[0] = tuple.get("n1");
                    projected_values[1] = tuple.get("s1");
                    return new Tuple(fieldNames_projected, projected_values);
                }
            }, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT());
            for (Tuple t : rs) {
                System.out.println("t:" + t.toMap());
                expected_s2.remove((String) t.get("s2"));
                expected_n2.remove((Integer) t.get("n2"));
            }
            assertTrue(expected_s2.isEmpty());
            assertTrue(expected_n2.isEmpty());
            assertEquals("n2", rs.getColumns()[0].name);
            assertEquals("s2", rs.getColumns()[1].name);
        }

    }

    @Test
    public void testApplyProjectionNoSwap() throws Exception {
        RecordSetFactory factory = buildRecordSetFactory(Integer.MAX_VALUE);
        Column[] columns = new Column[2];
        columns[0] = Column.column("s1", ColumnTypes.STRING);
        columns[1] = Column.column("n1", ColumnTypes.LONG);
        Set<String> expected_s2 = new HashSet<>();
        Set<Integer> expected_n2 = new HashSet<>();
        try (MaterializedRecordSet rs = factory.createRecordSet(columns);) {

            for (int i = 0; i < 100; i++) {
                Map<String, Object> record = new HashMap<>();
                String s1 = "test_" + i;
                record.put("s1", s1);
                record.put("n1", i);
                expected_s2.add(s1);
                expected_n2.add(i);
                rs.add(new Tuple(record, columns));
            }
            rs.writeFinished();

            Column[] columns_projected = new Column[2];
            columns_projected[0] = Column.column("n2", ColumnTypes.LONG);
            columns_projected[1] = Column.column("s2", ColumnTypes.STRING);
            String[] fieldNames_projected = new String[]{"n2", "s2"};

            rs.applyProjection(new Projection() {
                @Override
                public Column[] getColumns() {
                    return columns_projected;
                }

                @Override
                public Tuple map(Tuple tuple, StatementEvaluationContext context) throws StatementExecutionException {
                    Object[] projected_values = new Object[2];
                    projected_values[0] = tuple.get("n1");
                    projected_values[1] = tuple.get("s1");
                    return new Tuple(fieldNames_projected, projected_values);
                }
            }, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT());
            for (Tuple t : rs) {
                expected_s2.remove((String) t.get("s2"));
                expected_n2.remove((Integer) t.get("n2"));
            }
            assertTrue(expected_s2.isEmpty());
            assertTrue(expected_n2.isEmpty());
            assertEquals("n2", rs.getColumns()[0].name);
            assertEquals("s2", rs.getColumns()[1].name);
        }

    }

}
