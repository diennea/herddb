package herddb.core;

import java.util.Arrays;
import java.util.Comparator;

public class RecordRange2<X> {

    private final int size;
    private final Comparator<X> comparator;
    
//    private X min;
//    private X max;
    
    private final X[] records;
    private int count;
    
    public RecordRange2(int size, Comparator<X> comparator) {
        super();
        this.size = size;
        this.comparator = comparator;
        
//        this.records = (X[]) new Object[size];
        this.records = (X[]) new Integer[size];
    }
    
        
    public void add(X record) {

        boolean full = (count >= size);
        
        if (full) {
            final int cmp = comparator.compare(records[count -1], record);

            if (cmp < 0) {
//                System.out.println("max " + records[count -1] + " " + record + " cmp " + cmp);
                return;
            }
        }
        
        int idx = Arrays.binarySearch(records, 0, count, record, comparator);

        if (idx < 0) {
            idx = -idx - 1;
        }
        
        if (!full) {
            ++count;
        }
        
//        System.out.println("src " + idx + " dst " + (idx + 1) + " count " + count + " len " + (count - (idx + 1)));
        System.arraycopy(records, idx, records, idx + 1, count - (idx + 1));
        
        records[idx] = record;
        
        System.out.println(idx);

    }
    
    public static void main(String[] args) {
        
        RecordRange2<Integer> r = new RecordRange2<>(3, (x,y) -> Integer.compare(x, y) );
        
//        put(4, r);
//        put(4, r);
//        put(4, r);
//        put(3, r);
//        put(3, r);
//        put(3, r);
//        put(2, r);
//        put(1, r);
//        put(2, r);
//        put(1, r);
//        put(4, r);
        
        put(4, r);
        put(3, r);
        put(2, r);
        put(1, r);
        put(-1, r);
    }
    
    private static final void put(int i, RecordRange2<Integer> r) {
        
        System.out.println( "-------------------------------------");
        System.out.println( "Previous Add " + i );
        System.out.println( "count: " + r.count );
//        System.out.println( "min:   " + r.min );
        if (r.count == 0) {
            System.out.println( "max:   null" );
        } else {
            System.out.println( "max:   " + r.records[r.count-1] );
        }
        
        System.out.println( "array: " + Arrays.toString(r.records) );
        
        r.add(i);
        
        System.out.println( "After Add " + i );
        System.out.println( "count: " + r.count );
//        System.out.println( "min:   " + r.min );
        System.out.println( "max:   " + r.records[r.count -1] );
        
        System.out.println( "array: " + Arrays.toString(r.records) );
        
    }
    
    
}
