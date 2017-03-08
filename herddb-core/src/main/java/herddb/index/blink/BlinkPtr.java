package herddb.index.blink;

public class BlinkPtr {

    public static final byte LINK  = -1;
    public static final byte PAGE  =  1;
    public static final byte EMPTY  = 0;

    private static final BlinkPtr EMPTY_INSTANCE = new BlinkPtr(Long.MIN_VALUE, EMPTY);

    long value;
    byte type;

    BlinkPtr(long value, byte type) {
        super();
        this.value = value;
        this.type = type;
    }

    public static final BlinkPtr link(long value) {
        return new BlinkPtr(value, LINK);
    }

    public static final BlinkPtr page(long value) {
        return new BlinkPtr(value, PAGE);
    }

    public static final BlinkPtr empty() {
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
