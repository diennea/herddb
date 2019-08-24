package herddb.utils;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link FilterOutputStream} implementation which doesn't propagate {@link #close()} invocations.
 * <p>
 * Usefull when wrapped stream musn't be close but you want to wrap it in a try-with-resources manner
 * </p>
 *
 * @author diego.salvi
 */
public class NonClosingOutputStream extends FilterOutputStream {

    public NonClosingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void close() throws IOException {
        /* Keep expected close implicit flush semantic */
        out.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

}
