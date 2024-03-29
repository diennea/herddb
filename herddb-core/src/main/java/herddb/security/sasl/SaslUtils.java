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

package herddb.security.sasl;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.security.sasl.Sasl;

/**
 * Sasl Utility
 *
 * @author enrico.olivelli
 */
public class SaslUtils {

    public static final String AUTH_DIGEST_MD5 = "DIGEST-MD5";

    public static final String AUTH_PLAIN = "PLAIN";


    public static final String DEFAULT_REALM = "default";


    static Map<String, String> getSaslProps(boolean allowPlain) {
        Map<String, String> props = new HashMap<String, String>();
        props.put(Sasl.POLICY_NOPLAINTEXT, !allowPlain + "");
        return props;
    }

    /**
     * Encode a password as a base64-encoded char[] array.
     *
     * @param password as a byte array.
     * @return password as a char array.
     */
    static char[] encodePassword(byte[] password) {
        return Base64.getEncoder().encodeToString(password).toCharArray();
    }


}
