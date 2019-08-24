package herddb.utils;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ExtendedDataInputStream} tests
 *
 * @author diego.salvi
 */
public class ExtendedDataOutputStreamTest {

    /**
     * Test write folded and unfolded vint version for compatibility
     *
     * @throws IOException
     */
    @Test
    public void testWriteVInt() throws IOException {

        checkAndCompareVInt(new byte[]{0}, 0);
        checkAndCompareVInt(new byte[]{1}, 1);
        checkAndCompareVInt(new byte[]{-1, -1, -1, -1, 15}, -1);
        checkAndCompareVInt(new byte[]{-128, -128, -128, -128, 8}, Integer.MIN_VALUE);
        checkAndCompareVInt(new byte[]{-1, -1, -1, -1, 7}, Integer.MAX_VALUE);

    }


    protected final void writeVIntFolded(ExtendedDataOutputStream oo, int i) throws IOException {
        while ((i & ~0x7F) != 0) {
            oo.writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        oo.writeByte((byte) i);
    }


    private void checkAndCompareVInt(byte[] expected, int data) throws IOException {

        byte[] folded;

        byte[] unfolded;

        try (VisibleByteArrayOutputStream vos = new VisibleByteArrayOutputStream(expected.length);
             ExtendedDataOutputStream os = new ExtendedDataOutputStream(vos)) {

            writeVIntFolded(os, data);

            folded = vos.buf;
        }

        try (VisibleByteArrayOutputStream vos = new VisibleByteArrayOutputStream(expected.length);
             ExtendedDataOutputStream os = new ExtendedDataOutputStream(vos)) {

            os.writeVInt(data);

            unfolded = vos.buf;
        }

        Assert.assertArrayEquals(expected, unfolded);
        Assert.assertArrayEquals(expected, folded);
    }

}
