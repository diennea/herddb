package herddb.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link ExtendedDataInputStream} tests
 *
 * @author diego.salvi
 */
public class ExtendedDataInputStreamTest {

    /**
     * Test read folded and unfolded vint version for compatibility
     *
     * @throws IOException
     */
    @Test
    public void testReadVInt() throws IOException {

        checkAndCompareVInt(0, new byte[]{0});
        checkAndCompareVInt(1, new byte[]{1});
        checkAndCompareVInt(-1, new byte[]{-1, -1, -1, -1, 15});
        checkAndCompareVInt(Integer.MIN_VALUE, new byte[]{-128, -128, -128, -128, 8});
        checkAndCompareVInt(Integer.MAX_VALUE, new byte[]{-1, -1, -1, -1, 7});

    }

    protected int readVIntFolded(byte first, ExtendedDataInputStream ii) throws IOException {
        byte b = first;
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = ii.readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    private void checkAndCompareVInt(int expected, byte[] data) throws IOException {

        try (ExtendedDataInputStream is = new ExtendedDataInputStream(new ByteArrayInputStream(data))) {
            is.mark(5);
            int folded = readVIntFolded(is.readByte(), is);

            is.reset();
            int unfolded = is.readVInt();

            Assert.assertEquals(expected, unfolded);
            Assert.assertEquals(expected, folded);
        }

    }

}
