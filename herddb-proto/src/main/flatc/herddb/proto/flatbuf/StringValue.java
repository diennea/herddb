// automatically generated by the FlatBuffers compiler, do not modify

package herddb.proto.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class StringValue extends Table {
  public static StringValue getRootAsStringValue(ByteBuffer _bb) { return getRootAsStringValue(_bb, new StringValue()); }
  public static StringValue getRootAsStringValue(ByteBuffer _bb, StringValue obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public StringValue __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String value() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer valueAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer valueInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }

  public static int createStringValue(FlatBufferBuilder builder,
      int valueOffset) {
    builder.startObject(1);
    StringValue.addValue(builder, valueOffset);
    return StringValue.endStringValue(builder);
  }

  public static void startStringValue(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addValue(FlatBufferBuilder builder, int valueOffset) { builder.addOffset(0, valueOffset, 0); }
  public static int endStringValue(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
