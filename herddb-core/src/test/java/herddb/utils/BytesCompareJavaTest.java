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

import org.junit.Assert;
import org.junit.Test;

/**
 * Check if bytes generated with Bytes.next are then compared the <i>correct</i>. Byte comparison
 * should be unsigned nut it wasn't (issue #276).
 *
 * <p>
 * This test is placed here and not in herd-utils package due to a Surefire issue. Surefire will not
 * use specialized versions placed under java9/... if run on the same project (src). When correctly
 * packed in a jar it will use them depending on Java version.
 * </p>
 *
 * @author diego.salvi
 */
public class BytesCompareJavaTest {

    /**
     * Check Bytes comparison on byte values greater than 127.
     *
     * <p>
     * Checked Bytes are:
     * <ul>
     * <li>08000000000008d4<b>7f</b></li>
     * <li>08000000000008d4<b>80</b></li>
     * </ul>
     * Last bytes switch from positive +127 to negative -128 but the should checked unsigned.
     * </p>
     */
    @Test
    public void checkCompare() {

        /* Builds 08000000000008d47f */

        Bytes suffix = Bytes.from_long(578687L);

        byte[] array = new byte[suffix.getLength() + 1];
        array[0] = 8;

        System.arraycopy(suffix.getBuffer(), suffix.getOffset(), array, 1, suffix.getLength());

        Bytes low = Bytes.from_array(array);

        Assert.assertEquals("08000000000008d47f", low.toString());

        /* Builds 08000000000008d480 */
        Bytes hi = low.next();

        Assert.assertEquals("08000000000008d480", hi.toString());


        Assert.assertTrue(low.compareTo(hi) < 0);

    }

}
