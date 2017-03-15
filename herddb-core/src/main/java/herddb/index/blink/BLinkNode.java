package herddb.index.blink;

import java.io.IOException;

import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;

public interface BLinkNode<K extends Comparable<K>> {

    public BLinkPtr scanNode(K key);

    public boolean isLeaf();

    public boolean isSafe();
    public boolean isSafeDelete();

    public BLinkNode<K> insert(K key, long pointer);

    public BLinkNode<K>[] split(K key, long page, long newPage);

    public BLinkNode<K> delete(K key);

    public long getPageId();
    BLinkPage getPage();

    public K getHighKey();
    public K getLowKey();

    public BLinkPtr getFirstChild();
    public BLinkPtr getRight();

    public long keys();


    BLinkNodeMetadata<K> checkpoint() throws IOException;

    public void unload(boolean flush);


    /* K key key to delete during merging */
//    BLinkNode<K> merge(K key, BLinkNode<K> sibling);

}
