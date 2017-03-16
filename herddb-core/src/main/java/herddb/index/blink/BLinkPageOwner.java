package herddb.index.blink;

import herddb.core.Page;

public final class BLinkPageOwner implements Page.Owner {

    private volatile BLinkNode<?> node;

    public BLinkPageOwner() {
        super();
        this.node = null;
    }

    public BLinkPageOwner(BLinkNode<?> node) {
        super();
        this.node = node;
    }

    public void setOwner(BLinkNode<?> node) {
        this.node = node;
    }

    @Override
    public void unload(long pageId) {
        node.unload(true);
    }

    @Override
    public String toString() {
        return "BLinkPageOwner [nodeId=" + node.getPageId() + "]";
    }
}
