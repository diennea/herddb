package herddb.index.blink;

public final class Element<K> {
//  private static final class Element<K> {
//  private K key;
//  private long page;

  final K key;
  final long page;
//  private Element<K> next;
  Element<K> next;

  public Element(K key, long page) {
      this(key,page,null);
  }

  public Element(K key, long page, Element<K> next) {
      super();
      this.key = key;
      this.page = page;
      this.next = next;
  }

  @Override
  public String toString() {
      return "Element [key=" + key + ", page=" + page + "]";
  }
}