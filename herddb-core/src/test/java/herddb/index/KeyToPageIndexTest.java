package herddb.index;

import org.junit.Assert;
import org.junit.Test;

import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;

/**
 * Base test suite for {@link KeyToPageIndex}
 *
 * @author diego.salvi
 */
public abstract class KeyToPageIndexTest {

    abstract KeyToPageIndex createIndex();

    @Test
    public void putIf() {

        int entries = 100;

        try (KeyToPageIndex index = createIndex()) {

            index.start(LogSequenceNumber.START_OF_TIME);

            for(int i = 0; i < entries; ++i) {
                index.put(Bytes.from_int(i), 1L);
            }

            for(int i = 0; i < entries; ++i) {
                Assert.assertTrue(index.put(Bytes.from_int(i), 2L, 1L));
            }

            for(int i = 0; i < entries; ++i) {
                Assert.assertFalse(index.put(Bytes.from_int(i), 3L, 1L));
                index.put(Bytes.from_int(i), 3L, 1L);
            }
        }
    }

}
