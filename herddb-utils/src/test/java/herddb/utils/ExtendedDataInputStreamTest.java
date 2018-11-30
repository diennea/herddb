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

        checkAndCompareVInt(0, new byte[] { 0 });
        checkAndCompareVInt(1, new byte[] { 1 });
        checkAndCompareVInt(-1, new byte[] { -1, -1, -1, -1, 15 });
        checkAndCompareVInt(Integer.MIN_VALUE, new byte[] { -128, -128, -128, -128, 8 });
        checkAndCompareVInt(Integer.MAX_VALUE, new byte[] { -1, -1, -1, -1, 7 });

    }

    private void checkAndCompareVInt(int expected, byte[] data) throws IOException {

        try (ExtendedDataInputStream is = new ExtendedDataInputStream(new ByteArrayInputStream(data))) {
            is.mark(5);
            int folded = is.readVIntFolded();

            is.reset();
            int unfolded = is.readVIntUnfolded();

            Assert.assertEquals(expected,unfolded);
            Assert.assertEquals(expected,folded);
        }

    }

}
