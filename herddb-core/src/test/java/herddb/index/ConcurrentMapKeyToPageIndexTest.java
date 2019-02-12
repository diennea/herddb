package herddb.index;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Suite di test base per {@link  ConcurrentMapKeyToPageIndex}
 *
 * @author diego.salvi
 */
public class ConcurrentMapKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {

        return new ConcurrentMapKeyToPageIndex(new ConcurrentHashMap<>());

    }

}
