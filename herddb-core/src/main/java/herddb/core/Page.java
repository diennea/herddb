package herddb.core;

/**
 * A generic page loaded in memory
 *
 * @author diego.salvi
 */
public abstract class Page<O extends Page.Owner> {

    /**
     * The page owner: responsible for real page unload (data dereference)
     *
     * @author diego.salvi
     */
    public static interface Owner {

        /**
         * Unload given page from memory.
         *
         * @param pageId id of page to be unloaded
         */
        public void unload(long pageId);

    }

    /**
     * Page metadata for {@link PageReplacementPolicy} use.
     *
     * @author diego.salvi
     */
    public static class Metadata {

        public final Page.Owner owner;
        public final long pageId;

        public Metadata(Page.Owner owner, long pageId) {
            super();

            this.owner = owner;
            this.pageId = pageId;
        }

    }

    /** Owner of the page */
    public final O owner;

    /** Page id absolute on the owner */
    public final long pageId;

    /** Page metadata for {@link PageReplacementPolicy} use */
    Metadata metadata;

    public Page(O owner, long pageId) {
        super();
        this.owner = owner;
        this.pageId = pageId;
    };

}
