package herddb.index.blink;

public interface BLinkNode<K extends Comparable<K>> {


    public BlinkPtr scanNode(K key);

    public boolean isLeaf();

    public boolean isSafe();

    public BLinkNode<K> insert(K key, long pointer);

    public BLinkNode<K>[] split(K key, long page, long newPage);

    public long getPage();

    public K getHighKey();
    public K getLowKey();

}
