// automatically generated by the FlatBuffers compiler, do not modify

package herddb.proto.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ColumnDefinition extends Table {
  public static ColumnDefinition getRootAsColumnDefinition(ByteBuffer _bb) { return getRootAsColumnDefinition(_bb, new ColumnDefinition()); }
  public static ColumnDefinition getRootAsColumnDefinition(ByteBuffer _bb, ColumnDefinition obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public ColumnDefinition __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte name(int j) { int o = __offset(4); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int nameLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }

  public static int createColumnDefinition(FlatBufferBuilder builder,
      int nameOffset) {
    builder.startObject(1);
    ColumnDefinition.addName(builder, nameOffset);
    return ColumnDefinition.endColumnDefinition(builder);
  }

  public static void startColumnDefinition(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(0, nameOffset, 0); }
  public static int createNameVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startNameVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endColumnDefinition(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}
