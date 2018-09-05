package com.google.flatbuffers;

import static com.google.flatbuffers.FlatBufferBuilder.growByteBuffer;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * A special FlatBufferBuilder able to write to Netty ByteBufs
 *
 * @author Enrico Olivelli
 */
public class EnhancedFlatBufferBuilder extends FlatBufferBuilder {

    private Consumer<ByteBuffer> releaser;

    public EnhancedFlatBufferBuilder(ByteBuffer existing_bb,
            FlatBufferBuilder.ByteBufferFactory bb_factory, Consumer<ByteBuffer> releaser) {
        super(existing_bb, bb_factory);
        this.releaser = releaser;
    }

    @Override
    public void prep(int size, int additional_bytes) {
        // Track the biggest thing we've ever aligned to.
        if (size > minalign) {
            minalign = size;
        }
        // Find the amount of alignment needed such that `size` is properly
        // aligned after `additional_bytes`
        int align_size = ((~(bb.capacity() - space + additional_bytes)) + 1) & (size - 1);
        // Reallocate the buffer if needed.
        while (space < align_size + size + additional_bytes) {
            int old_buf_size = bb.capacity();
            ByteBuffer prev = bb;
            bb = growByteBuffer(prev, bb_factory);
            if (bb != prev) {
                // RELEASE PREV MEMORY
                releaser.accept(prev);
            }
            space += bb.capacity() - old_buf_size;
        }
        pad(align_size);
    }

}
