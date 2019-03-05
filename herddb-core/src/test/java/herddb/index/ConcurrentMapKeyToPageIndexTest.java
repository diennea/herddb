package herddb.index;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Base test suite for {@link  ConcurrentMapKeyToPageIndex}
 *
 * @author diego.salvi
 */
public class ConcurrentMapKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {

        return new ConcurrentMapKeyToPageIndex(new ConcurrentHashMap<>());

    }

}
