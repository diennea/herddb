package herddb.core;

import java.util.Collection;

public interface PageReplacementPolicy {

    public DataPage add(DataPage page);

    public boolean remove(DataPage page);

    public void remove(Collection<DataPage> pages);

    public int size();

    public int capacity();

    public void clear();

}
