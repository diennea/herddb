package herddb.index.blink;

import java.io.IOException;

public interface BLinkIndexDataStorage<K> {

    public static final long NEW_PAGE = -1;

    public Element<K> loadPage(long pageId) throws IOException;

    public long createDataPage(Element<K> root) throws IOException;

    public long createMetadataPage(BLinkMetadata<K> metadata) throws IOException;

}
