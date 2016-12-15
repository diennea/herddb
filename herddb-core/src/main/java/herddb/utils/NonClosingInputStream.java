package herddb.utils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link FilterInputStream} implementation which doesn't propagate {@link #close()} invocations.
 * <p>
 * Usefull when wrapped stream musn't be close but you want to wrap it in a try-with-resources manner
 * </p>
 * 
 * @author diego.salvi
 */
public class NonClosingInputStream extends FilterInputStream {

    public NonClosingInputStream(InputStream in) {
        super(in);
    }

    @Override
    public void close() throws IOException {}
    
}
