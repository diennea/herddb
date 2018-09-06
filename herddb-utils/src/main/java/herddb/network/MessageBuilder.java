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
package herddb.network;

import com.google.flatbuffers.ByteBufFlatBufferBuilder;
import static com.google.flatbuffers.ByteBufFlatBufferBuilder.newFlatBufferBuilder;
import com.google.flatbuffers.FlatBufferBuilder;
import herddb.utils.KeyValue;
import herddb.utils.TuplesList;
import herddb.proto.flatbuf.*;
import static herddb.proto.flatbuf.MessageType.TYPE_ACK;
import static herddb.proto.flatbuf.MessageType.TYPE_CLIENT_SHUTDOWN;
import static herddb.proto.flatbuf.MessageType.TYPE_CLOSESCANNER;
import static herddb.proto.flatbuf.MessageType.TYPE_ERROR;
import static herddb.proto.flatbuf.MessageType.TYPE_EXECUTE_STATEMENT;
import static herddb.proto.flatbuf.MessageType.TYPE_EXECUTE_STATEMENTS;
import static herddb.proto.flatbuf.MessageType.TYPE_EXECUTE_STATEMENTS_RESULT;
import static herddb.proto.flatbuf.MessageType.TYPE_EXECUTE_STATEMENT_RESULT;
import static herddb.proto.flatbuf.MessageType.TYPE_FETCHSCANNERDATA;
import static herddb.proto.flatbuf.MessageType.TYPE_OPENSCANNER;
import static herddb.proto.flatbuf.MessageType.TYPE_REQUEST_TABLESPACE_DUMP;
import static herddb.proto.flatbuf.MessageType.TYPE_RESULTSET_CHUNK;
import static herddb.proto.flatbuf.MessageType.TYPE_SASL_TOKEN_MESSAGE_REQUEST;
import static herddb.proto.flatbuf.MessageType.TYPE_SASL_TOKEN_MESSAGE_TOKEN;
import static herddb.proto.flatbuf.MessageType.TYPE_SASL_TOKEN_SERVER_RESPONSE;
import static herddb.proto.flatbuf.MessageType.TYPE_TABLESPACE_DUMP_DATA;
import static herddb.proto.flatbuf.MessageType.TYPE_TX_COMMAND;

import herddb.utils.DataAccessor;
import herddb.utils.IntHolder;
import herddb.utils.RawString;
import io.netty.buffer.ByteBuf;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public abstract class MessageBuilder {

    public static ByteBuf ACK(long reply) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        Response.startResponse(builder);
        Response.addReplyMessageId(builder, reply);
        Response.addType(builder, MessageType.TYPE_ACK);
        return finishAsResponse(builder);
    }

    public static ByteBuf ERROR(long replyId, Throwable error) {
        return ERROR(replyId, error, null, false);
    }

    private static int addString(FlatBufferBuilder builder, String s) {
        return builder.createByteVector(s.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf ERROR(long replyId, Throwable error, String additionalInfo, boolean notLeader) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();

        int errorOffset;
        if (additionalInfo != null) {
            errorOffset = addString(builder, error + " " + additionalInfo);
        } else {
            errorOffset = addString(builder, error + "");
        }

        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));

        int stackTrace = addString(builder, writer.toString());

        Response.startResponse(builder);
        Response.addReplyMessageId(builder, replyId);
        Response.addType(builder, MessageType.TYPE_ERROR);
        Response.addNotLeader(builder, notLeader);

        Response.addError(builder, errorOffset);

        Response.addStackTrace(builder, stackTrace);

        return finishAsResponse(builder);
    }

    public static ByteBuf finishAsResponse(ByteBufFlatBufferBuilder builder) {
        int posResponse = Response.endResponse(builder);
        Message.startMessage(builder);
        Message.addResponse(builder, posResponse);
        int pos = Message.endMessage(builder);
        Message.finishMessageBuffer(builder, pos);
        return builder.toByteBuf();
    }

    public static ByteBuf finishAsRequest(ByteBufFlatBufferBuilder builder) {
        int posRequest = Request.endRequest(builder);
        Message.startMessage(builder);
        Message.addRequest(builder, posRequest);
        int pos = Message.endMessage(builder);
        Message.finishMessageBuffer(builder, pos);
        return builder.toByteBuf();
    }

    public static ByteBuf EXECUTE_STATEMENT(long id, String tableSpace, String query, long tx,
            boolean returnValues,
            List<Object> params) {

        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int queryOffset = addString(builder, query);

        int paramsOffset = encodeAnyValueList(params, false, builder);

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addTx(builder, tx);
        Request.addReturnValues(builder, returnValues);
        Request.addQuery(builder, queryOffset);
        Request.addType(builder, MessageType.TYPE_EXECUTE_STATEMENT);

        if (paramsOffset >= 0) {
            Request.addParams(builder, paramsOffset);
        }
        return finishAsRequest(builder);

    }

    private static final Logger LOG = Logger.getLogger(MessageBuilder.class.getName());

    private static int encodeBatchParams(List<List<Object>> params, FlatBufferBuilder builder) throws IllegalArgumentException {
        if (params == null || params.isEmpty()) {
            return -1;
        }
        int[] listOffsets = new int[params.size()];
        int i = 0;
        for (List<Object> paramList : params) {
            int listOffset = encodeAnyValueList(paramList, true /*we must encode the list even if it is empty*/, builder);
            listOffsets[i++] = listOffset;
        }
        return Request.createBatchParamsVector(builder, listOffsets);
    }

    private static int encodeAnyValueList(List<Object> params, boolean encodeEmpty, FlatBufferBuilder builder) throws IllegalArgumentException {
        int paramsOffset = -1;
        if (params != null && (encodeEmpty || !params.isEmpty())) {
            int[] paramsOffsets = new int[params.size()];
            int pIndex = 0;

            for (Object param : params) {
                int wEnd = encodeObject(builder, param);
                paramsOffsets[pIndex++] = wEnd;

            }
            int itemsOffset = AnyValueList.createItemsVector(builder, paramsOffsets);
            AnyValueList.startAnyValueList(builder);
            AnyValueList.addItems(builder, itemsOffset);
            paramsOffset = AnyValueList.endAnyValueList(builder);
        }
        return paramsOffset;
    }

    private static int encodeMap(FlatBufferBuilder builder, Map<String, Object> map) throws IllegalArgumentException {
        if (map == null || map.isEmpty()) {
            return -1;
        }
        int[] entriesOffsets = new int[map.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            int keyOffset = addString(builder, entry.getKey());
            int valueOffset = encodeObject(builder, entry.getValue());
            MapEntry.startMapEntry(builder);
            MapEntry.addKey(builder, keyOffset);
            MapEntry.addValue(builder, valueOffset);
            entriesOffsets[i++] = MapEntry.endMapEntry(builder);
        }
        int entriesOffset = herddb.proto.flatbuf.Map.createEntriesVector(builder, entriesOffsets);
        herddb.proto.flatbuf.Map.startMap(builder);
        herddb.proto.flatbuf.Map.addEntries(builder, entriesOffset);
        return herddb.proto.flatbuf.Map.endMap(builder);
    }

    private static int encodeObject(FlatBufferBuilder builder, Object param) throws IllegalArgumentException {
        int wEnd;
        if (param == null) {
            NullValue.startNullValue(builder);
            int end = NullValue.endNullValue(builder);

            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.NullValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);
        } else if (param instanceof RawString) {
            int stringOffset = builder.createByteVector(((RawString) param).data);
            StringValue.startStringValue(builder);
            StringValue.addValue(builder, stringOffset);
            int end = StringValue.endStringValue(builder);

            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.StringValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof String) {
            int stringOffset = addString(builder, (String) param);
            StringValue.startStringValue(builder);
            StringValue.addValue(builder, stringOffset);
            int end = StringValue.endStringValue(builder);

            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.StringValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof Integer
                || param instanceof Byte
                || param instanceof Short) {
            IntValue.startIntValue(builder);
            IntValue.addValue(builder, ((Number) param).intValue());
            int end = IntValue.endIntValue(builder);
            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.IntValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof Long) {
            LongValue.startLongValue(builder);
            LongValue.addValue(builder, ((Number) param).intValue());
            int end = LongValue.endLongValue(builder);
            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.LongValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof java.sql.Timestamp) {
            TimestampValue.startTimestampValue(builder);
            TimestampValue.addValue(builder, ((java.sql.Timestamp) param).getTime());
            int end = TimestampValue.endTimestampValue(builder);
            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.TimestampValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof Double
                || param instanceof Float) {
            DoubleValue.startDoubleValue(builder);
            DoubleValue.addValue(builder, Double.doubleToLongBits(((Number) param).doubleValue()));
            int end = DoubleValue.endDoubleValue(builder);
            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.DoubleValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else if (param instanceof java.lang.Boolean) {
            BooleanValue.startBooleanValue(builder);
            BooleanValue.addValue(builder, ((java.lang.Boolean) param).booleanValue());
            int end = BooleanValue.endBooleanValue(builder);
            AnyValueWrapper.startAnyValueWrapper(builder);
            AnyValueWrapper.addValueType(builder, AnyValue.BooleanValue);
            AnyValueWrapper.addValue(builder, end);
            wEnd = AnyValueWrapper.endAnyValueWrapper(builder);

        } else {
            throw new IllegalArgumentException("Unsupported JDBC Value type " + param.getClass() + " - " + param.toString());
        }
        return wEnd;
    }

    private static int encodeColumDefinitionsList(String[] columnNames, FlatBufferBuilder builder) throws IllegalArgumentException {
        int[] columnsOffsets = new int[columnNames.length];
        int pIndex = 0;

        for (String colName : columnNames) {
            int stringOffset = addString(builder, (String) colName);
            StringValue.startStringValue(builder);
            StringValue.addValue(builder, stringOffset);
            int end = StringValue.endStringValue(builder);
            columnsOffsets[pIndex++] = end;
        }
        return Response.createColumnNamesVector(builder, columnsOffsets);
    }

    public static ByteBuf TX_COMMAND(long id, String tableSpace, int type, long tx) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addType(builder, MessageType.TYPE_TX_COMMAND);
        Request.addTx(builder, tx);
        Request.addTxCommand(builder, (byte) type);
        return finishAsRequest(builder);
    }

    public static ByteBuf EXECUTE_STATEMENTS(long id, String tableSpace, String query,
            long tx, boolean returnValues, List<List<Object>> params) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int queryOffset = addString(builder, query);

        int batchParamsOffset = encodeBatchParams(params, builder);

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addTx(builder, tx);
        Request.addReturnValues(builder, returnValues);
        Request.addQuery(builder, queryOffset);
        Request.addType(builder, MessageType.TYPE_EXECUTE_STATEMENTS);

        if (batchParamsOffset >= 0) {
            Request.addBatchParams(builder, batchParamsOffset);
        }
        return finishAsRequest(builder);
    }

    public static ByteBuf OPEN_SCANNER(long id, String tableSpace, String query,
            String scannerId, long tx, List<Object> params, int fetchSize, int maxRows) {

        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int queryOffset = addString(builder, query);
        int scannerIdOffset = addString(builder, scannerId);

        int paramsOffset = encodeAnyValueList(params, false, builder);

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addScannerId(builder, scannerIdOffset);
        Request.addTx(builder, tx);
        Request.addFetchSize(builder, fetchSize);
        Request.addMaxRows(builder, maxRows);
        Request.addQuery(builder, queryOffset);
        Request.addType(builder, MessageType.TYPE_OPENSCANNER);

        if (paramsOffset >= 0) {
            Request.addParams(builder, paramsOffset);
        }
        return finishAsRequest(builder);
    }

    public static ByteBuf REQUEST_TABLESPACE_DUMP(long id, String tableSpace, String dumpId, int fetchSize,
            boolean includeTransactionLog) {

        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int dumpIdOffset = addString(builder, dumpId);

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addDumpId(builder, dumpIdOffset);

        Request.addFetchSize(builder, fetchSize);
        Request.addIncludeTransactionLog(builder, includeTransactionLog);
        Request.addType(builder, MessageType.TYPE_REQUEST_TABLESPACE_DUMP);
        return finishAsRequest(builder);

    }

    public static ByteBuf TABLESPACE_DUMP_DATA(long id, String tableSpace, String dumpId,
            String command, byte[] tableDefinition, long estimatedSize,
            long dumpLedgerid, long dumpOffset, List<byte[]> indexesDefinition,
            List<KeyValue> records) {

        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int dumpIdOffset = addString(builder, dumpId);
        int commandOffset = builder.createString(command);

        int tableDefinitionOffset = -1;
        if (tableDefinition != null) {
            int schemaDefinitionOffset = TableDefinition.createSchemaVector(builder, tableDefinition);
            TableDefinition.startTableDefinition(builder);
            TableDefinition.addSchema(builder, schemaDefinitionOffset);
            tableDefinitionOffset = TableDefinition.endTableDefinition(builder);
        }
        int indexesOffset = -1;
        if (indexesDefinition != null && !indexesDefinition.isEmpty()) {
            int[] indexesOffsets = new int[indexesDefinition.size()];
            int i = 0;
            for (byte[] indexDefinition : indexesDefinition) {
                int schemaDefinitionOffset = TableDefinition.createSchemaVector(builder, indexDefinition);
                IndexDefinition.startIndexDefinition(builder);
                IndexDefinition.addSchema(builder, schemaDefinitionOffset);
                indexesOffsets[i++] = IndexDefinition.endIndexDefinition(builder);
            }
            indexesOffset = Request.createIndexesDefinitionVector(builder, indexesOffsets);
        }
        int rawDataChunkOffset = -1;
        if (records != null && !records.isEmpty()) {
            int[] entriesOffsets = new int[records.size()];
            int i = 0;
            for (KeyValue kekValue : records) {
                int keyOffset = builder.createByteVector(kekValue.key);
                int valueOffset = builder.createByteVector(kekValue.value);
                herddb.proto.flatbuf.KeyValue.startKeyValue(builder);
                herddb.proto.flatbuf.KeyValue.addKey(builder, keyOffset);
                herddb.proto.flatbuf.KeyValue.addValue(builder, valueOffset);
                entriesOffsets[i++] = herddb.proto.flatbuf.KeyValue.endKeyValue(builder);
            }
            rawDataChunkOffset = Request.createRawDataChunkVector(builder, entriesOffsets);
        }

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addDumpId(builder, dumpIdOffset);
        Request.addDumpLedgerId(builder, dumpLedgerid);
        Request.addDumpOffset(builder, dumpOffset);
        Request.addCommand(builder, commandOffset);
        if (tableDefinitionOffset >= 0) {
            Request.addTableDefinition(builder, tableDefinitionOffset);
        }
        if (indexesOffset >= 0) {
            Request.addIndexesDefinition(builder, indexesOffset);
        }
        if (rawDataChunkOffset >= 0) {
            Request.addRawDataChunk(builder, rawDataChunkOffset);
        }

        Request.addType(builder, MessageType.TYPE_TABLESPACE_DUMP_DATA);
        return finishAsRequest(builder);
    }

    public static ByteBuf RESULTSET_CHUNK(long replyId, TuplesList tuplesList, boolean last, long tx) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();

        int columnsOffset = encodeColumDefinitionsList(tuplesList.columnNames, builder);
        int rowsOffset = -1;
        if (!tuplesList.tuples.isEmpty()) {
            int numColumns = tuplesList.columnNames.length;
            int[] rowsOffsets = new int[tuplesList.tuples.size()];
            int rowIndex = 0;
            for (DataAccessor da : tuplesList.tuples) {

                int[] cellsOffsets = new int[numColumns];
                IntHolder currentColumn = new IntHolder();
                da.forEach((String key, Object value) -> {
                    String expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    while (!key.equals(expectedColumnName)) {
                        // nulls are not returned for some special accessors, lie DataAccessorForFullRecord
                        int valueOffset = encodeObject(builder, null);
                        cellsOffsets[currentColumn.value++] = valueOffset;
                        expectedColumnName = tuplesList.columnNames[currentColumn.value];
                    }
                    int valueOffset = encodeObject(builder, value);
                    cellsOffsets[currentColumn.value++] = valueOffset;
                });

                // fill with nulls
                while (currentColumn.value < numColumns) {
                    int valueOffset = encodeObject(builder, null);
                    cellsOffsets[currentColumn.value++] = valueOffset;
                }
                if (currentColumn.value > numColumns) {
                    throw new RuntimeException("unexpected number of columns " + currentColumn.value + " > " + numColumns);
                }
                int cellsOffset = Row.createCellsVector(builder, cellsOffsets);

                Row.startRow(builder);
                Row.addCells(builder, cellsOffset);
                int rowOffset = Row.endRow(builder);
                rowsOffsets[rowIndex++] = rowOffset;
            }
            rowsOffset = Response.createRowsVector(builder, rowsOffsets);
        }

        Response.startResponse(builder);
        Response.addReplyMessageId(builder, replyId);
        Response.addLast(builder, last);
        Response.addTx(builder, tx);
        Response.addType(builder, MessageType.TYPE_RESULTSET_CHUNK);
        Response.addColumnNames(builder, columnsOffset);
        if (rowsOffset >= 0) {
            Response.addRows(builder, rowsOffset);
        }
        return finishAsResponse(builder);

    }

    public static ByteBuf CLOSE_SCANNER(long id, String scannerId) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int scannerIdOffset = addString(builder, scannerId);
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addScannerId(builder, scannerIdOffset);
        Request.addType(builder, MessageType.TYPE_CLOSESCANNER);
        return finishAsRequest(builder);
    }

    public static ByteBuf FETCH_SCANNER_DATA(long id, String scannerId, int fetchSize) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int scannerIdOffset = addString(builder, scannerId);
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addScannerId(builder, scannerIdOffset);
        Request.addType(builder, MessageType.TYPE_FETCHSCANNERDATA);
        Request.addFetchSize(builder, fetchSize);
        return finishAsRequest(builder);

    }

    public static ByteBuf EXECUTE_STATEMENT_RESULT(long replyId, long updateCount,
            Map<String, Object> record, long tx) {

        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();

        int recordOffset = -1;
        if (record != null) {
            recordOffset = encodeMap(builder, record);
        }
        Response.startResponse(builder);
        Response.addReplyMessageId(builder, replyId);
        Response.addUpdateCount(builder, updateCount);
        Response.addTx(builder, tx);
        if (recordOffset >= 0) {
            Response.addNewValue(builder, recordOffset);
        }
        Response.addType(builder, MessageType.TYPE_SASL_TOKEN_SERVER_RESPONSE);
        return finishAsResponse(builder);
    }

    public static ByteBuf EXECUTE_STATEMENT_RESULTS(long replyId, List<Long> updateCounts, List<Map<String, Object>> otherdata, long tx) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int numResults = updateCounts.size();
        int[] resultsOffsets = new int[numResults];
        for (int i = 0; i < numResults; i++) {
            Map<String, Object> record = otherdata.get(i);
            int recordOffset = -1;
            if (record != null) {
                recordOffset = encodeMap(builder, record);
            }
            DMLResult.startDMLResult(builder);
            DMLResult.addUpdateCount(builder, updateCounts.get(i));
            if (recordOffset >= 0) {
                DMLResult.addNewValue(builder, recordOffset);
            }
            resultsOffsets[i] = DMLResult.endDMLResult(builder);
        }

        int batchResultsOffset = Response.createBatchResultsVector(builder, resultsOffsets);

        Response.startResponse(builder);
        Response.addReplyMessageId(builder, replyId);
        Response.addTx(builder, tx);
        Response.addBatchResults(builder, batchResultsOffset);
        Response.addType(builder, MessageType.TYPE_EXECUTE_STATEMENTS_RESULT);
        return finishAsResponse(builder);

    }

    public static ByteBuf SASL_TOKEN_MESSAGE_REQUEST(long id, String saslMech, byte[] firstToken) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int mechOffset = addString(builder, saslMech);
        int tokenOffset = builder.createByteVector(firstToken);
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addMech(builder, mechOffset);
        Request.addToken(builder, tokenOffset);
        Request.addType(builder, MessageType.TYPE_SASL_TOKEN_MESSAGE_REQUEST);
        return finishAsRequest(builder);
    }

    public static ByteBuf SASL_TOKEN_SERVER_RESPONSE(long replyId, byte[] saslTokenChallenge) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tokenOffset = -1;
        if (saslTokenChallenge != null) {
            tokenOffset = builder.createByteVector(saslTokenChallenge);
        }
        Response.startResponse(builder);
        Response.addReplyMessageId(builder, replyId);
        if (tokenOffset >= 0) {
            Response.addToken(builder, tokenOffset);
        }
        Response.addType(builder, MessageType.TYPE_SASL_TOKEN_SERVER_RESPONSE);
        return finishAsResponse(builder);
    }

    public static ByteBuf SASL_TOKEN_MESSAGE_TOKEN(long id, byte[] token) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tokenOffset = 0;
        if (token != null) {
            tokenOffset = builder.createByteVector(token);
        }
        Request.startRequest(builder);
        Request.addId(builder, id);
        if (token != null) {
            Request.addToken(builder, tokenOffset);
        }
        Request.addType(builder, MessageType.TYPE_SASL_TOKEN_MESSAGE_TOKEN);
        return finishAsRequest(builder);
    }

    public static ByteBuf REQUEST_TABLE_RESTORE(long id, String tableSpace, byte[] tableDefinition,
            long dumpLedgerId, long dumpOffset) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);

        int schemaDefinitionOffset = TableDefinition.createSchemaVector(builder, tableDefinition);
        TableDefinition.startTableDefinition(builder);
        TableDefinition.addSchema(builder, schemaDefinitionOffset);
        int tableDefinitionOffset = TableDefinition.endTableDefinition(builder);

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addDumpLedgerId(builder, dumpLedgerId);
        Request.addDumpOffset(builder, dumpOffset);
        Request.addTableDefinition(builder, tableDefinitionOffset);

        Request.addType(builder, MessageType.TYPE_REQUEST_TABLE_RESTORE);
        return finishAsRequest(builder);
    }

    public static ByteBuf TABLE_RESTORE_FINISHED(long id, String tableSpace, String tableName,
            List<byte[]> indexesDefinition) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int tableNameOffset = addString(builder, tableName);

        int indexesOffset = -1;
        if (indexesDefinition != null && !indexesDefinition.isEmpty()) {
            int[] indexesOffsets = new int[indexesDefinition.size()];
            int i = 0;
            for (byte[] indexDefinition : indexesDefinition) {
                int schemaDefinitionOffset = TableDefinition.createSchemaVector(builder, indexDefinition);
                IndexDefinition.startIndexDefinition(builder);
                IndexDefinition.addSchema(builder, schemaDefinitionOffset);
                indexesOffsets[i++] = IndexDefinition.endIndexDefinition(builder);
            }
            indexesOffset = Request.createIndexesDefinitionVector(builder, indexesOffsets);
        }
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addTableName(builder, tableNameOffset);
        if (indexesOffset >= 0) {
            Request.addIndexesDefinition(builder, indexesOffset);
        }
        Request.addType(builder, MessageType.TYPE_TABLE_RESTORE_FINISHED);
        return finishAsRequest(builder);

    }

    public static ByteBuf RESTORE_FINISHED(long id, String tableSpace) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addType(builder, MessageType.TYPE_RESTORE_FINISHED);
        return finishAsRequest(builder);

    }

    public static ByteBuf PUSH_TABLE_DATA(long id, String tableSpace, String tableName, List<KeyValue> chunk) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);
        int tableNameOffset = addString(builder, tableName);

        int rawDataChunkOffset = -1;
        if (chunk != null && !chunk.isEmpty()) {
            int[] entriesOffsets = new int[chunk.size()];
            int i = 0;
            for (KeyValue kekValue : chunk) {
                int keyOffset = builder.createByteVector(kekValue.key);
                int valueOffset = builder.createByteVector(kekValue.value);
                herddb.proto.flatbuf.KeyValue.startKeyValue(builder);
                herddb.proto.flatbuf.KeyValue.addKey(builder, keyOffset);
                herddb.proto.flatbuf.KeyValue.addValue(builder, valueOffset);
                entriesOffsets[i++] = herddb.proto.flatbuf.KeyValue.endKeyValue(builder);
            }
            rawDataChunkOffset = Request.createRawDataChunkVector(builder, entriesOffsets);
        }

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addTableName(builder, tableNameOffset);
        Request.addType(builder, MessageType.TYPE_PUSH_TABLE_DATA);
        if (rawDataChunkOffset >= 0) {
            Request.addRawDataChunk(builder, rawDataChunkOffset);
        }

        return finishAsRequest(builder);
    }

    public static ByteBuf PUSH_TXLOGCHUNK(long id, String tableSpace, List<KeyValue> chunk) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);

        int rawDataChunkOffset = -1;
        if (chunk != null && !chunk.isEmpty()) {
            int[] entriesOffsets = new int[chunk.size()];
            int i = 0;
            for (KeyValue kekValue : chunk) {
                int keyOffset = builder.createByteVector(kekValue.key);
                int valueOffset = builder.createByteVector(kekValue.value);
                herddb.proto.flatbuf.KeyValue.startKeyValue(builder);
                herddb.proto.flatbuf.KeyValue.addKey(builder, keyOffset);
                herddb.proto.flatbuf.KeyValue.addValue(builder, valueOffset);
                entriesOffsets[i++] = herddb.proto.flatbuf.KeyValue.endKeyValue(builder);
            }
            rawDataChunkOffset = Request.createRawDataChunkVector(builder, entriesOffsets);
        }

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addType(builder, MessageType.TYPE_PUSH_TXLOGCHUNK);
        if (rawDataChunkOffset >= 0) {
            Request.addRawDataChunk(builder, rawDataChunkOffset);
        }

        return finishAsRequest(builder);
    }

    public static ByteBuf PUSH_TRANSACTIONSBLOCK(long id, String tableSpace, List<byte[]> chunk) {
        ByteBufFlatBufferBuilder builder = newFlatBufferBuilder();
        int tableSpaceOffset = addString(builder, tableSpace);

        int dumpTxLogEntriesVectorOffset = -1;
        if (chunk != null && !chunk.isEmpty()) {
            int[] entriesOffsets = new int[chunk.size()];
            int i = 0;
            for (byte[] entry : chunk) {
                int itemOffset = builder.createByteVector(entry);

                herddb.proto.flatbuf.TxLogEntry.startTxLogEntry(builder);
                herddb.proto.flatbuf.TxLogEntry.addEntry(builder, itemOffset);
                entriesOffsets[i++] = herddb.proto.flatbuf.TxLogEntry.endTxLogEntry(builder);
            }
            dumpTxLogEntriesVectorOffset = Request.createDumpTxLogEntriesVector(builder, entriesOffsets);
        }

        Request.startRequest(builder);
        Request.addId(builder, id);
        Request.addTableSpace(builder, tableSpaceOffset);
        Request.addType(builder, MessageType.TYPE_PUSH_TRANSACTIONSBLOCK);
        if (dumpTxLogEntriesVectorOffset >= 0) {
            Request.addDumpTxLogEntries(builder, dumpTxLogEntriesVectorOffset);
        }

        return finishAsRequest(builder);
    }

    public static final int TX_COMMAND_ROLLBACK_TRANSACTION = 1;
    public static final int TX_COMMAND_COMMIT_TRANSACTION = 2;
    public static final int TX_COMMAND_BEGIN_TRANSACTION = 3;

    public static String typeToString(int type) {
        switch (type) {
            case TYPE_ACK:
                return "ACK";
            case TYPE_ERROR:
                return "ERROR";
            case TYPE_CLIENT_SHUTDOWN:
                return "CLIENT_SHUTDOWN";
            case TYPE_EXECUTE_STATEMENT:
                return "EXECUTE_STATEMENT";
            case TYPE_TX_COMMAND:
                return "TX_COMMAND";
            case TYPE_EXECUTE_STATEMENT_RESULT:
                return "EXECUTE_STATEMENT_RESULT";
            case TYPE_EXECUTE_STATEMENTS:
                return "EXECUTE_STATEMENTS";
            case TYPE_EXECUTE_STATEMENTS_RESULT:
                return "EXECUTE_STATEMENTS_RESULT";
            case TYPE_OPENSCANNER:
                return "OPENSCANNER";
            case TYPE_RESULTSET_CHUNK:
                return "RESULTSET_CHUNK";
            case TYPE_CLOSESCANNER:
                return "CLOSESCANNER";
            case TYPE_FETCHSCANNERDATA:
                return "FETCHSCANNERDATA";
            case TYPE_REQUEST_TABLESPACE_DUMP:
                return "REQUEST_TABLESPACE_DUMP";
            case TYPE_TABLESPACE_DUMP_DATA:
                return "TABLESPACE_DUMP_DATA";
            case TYPE_SASL_TOKEN_MESSAGE_REQUEST:
                return "SASL_TOKEN_MESSAGE_REQUEST";
            case TYPE_SASL_TOKEN_SERVER_RESPONSE:
                return "SASL_TOKEN_SERVER_RESPONSE";
            case TYPE_SASL_TOKEN_MESSAGE_TOKEN:
                return "SASL_TOKEN_MESSAGE_TOKEN";
            default:
                return "?" + type;
        }
    }

}
