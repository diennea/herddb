package herddb.index.blink;

public class BLinkPtr {

    public static final byte LINK  = -1;
    public static final byte PAGE  =  1;
    public static final byte EMPTY  = 0;

    private static final BLinkPtr EMPTY_INSTANCE = new BLinkPtr(BLink.NO_PAGE, EMPTY);

    long value;
    byte type;

    BLinkPtr(long value, byte type) {
        super();
        this.value = value;
        this.type = type;
    }

    public static final BLinkPtr link(long value) {
        return  (value == BLink.NO_PAGE) ? EMPTY_INSTANCE : new BLinkPtr(value, LINK);
    }

    public static final BLinkPtr page(long value) {
        return  (value == BLink.NO_PAGE) ? EMPTY_INSTANCE : new BLinkPtr(value, PAGE);
    }

    public static final BLinkPtr empty() {
        return EMPTY_INSTANCE;
    }

    public boolean isLink() {
        return type == LINK;
    }

    public boolean isPage() {
        return type == PAGE;
    }

    public boolean isEmpty() {
        return type == EMPTY;
    }

    @Override
    public String toString() {
        switch (type) {
            case LINK:
                return "BlinkPtr [value=" + value + ", type=LINK]";
            case PAGE:
                return "BlinkPtr [value=" + value + ", type=PAGE]";
            case EMPTY:
                return "BlinkPtr [value=" + value + ", type=EMPTY]";
            default:
                return "BlinkPtr [value=" + value + ", type=" + type + "]";
        }
    }

}
