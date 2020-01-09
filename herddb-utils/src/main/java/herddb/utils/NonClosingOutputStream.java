/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

*/

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
