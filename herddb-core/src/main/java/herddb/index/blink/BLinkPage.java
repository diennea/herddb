package herddb.index.blink;

import herddb.core.Page;
import herddb.core.PageReplacementPolicy;

/**
 * A <i>virtual</i> page of BLink nodes data for {@link PageReplacementPolicy} use.
 *
 * @author diego.salvi
 */
public class BLinkPage extends Page<BLinkPageOwner> {

    public BLinkPage(long page) {
        super(new BLinkPageOwner(), page);
    }

    public BLinkPage(long page, BLinkNode<?> node) {
        super(new BLinkPageOwner(node), page);
    }

    @Override
    public String toString() {
        return "BLinkPage [owner=" + owner + ", pageId=" + pageId + "]";
    }

    public void setOwner(BLinkNode<?> node) {
        this.owner.setOwner(node);
    }
}
