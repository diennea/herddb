/*
 * Copyright 2016 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package herddb.security.sasl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Sasl Client
 *
 * @author enrico.olivelli
 */
public class SaslNettyClient {

    private static final Logger LOG = Logger
            .getLogger(SaslNettyClient.class.getName());

    /**
     * Used to respond to server's counterpart, SaslServer with SASL tokens
     * represented as byte arrays.
     */
    private SaslClient saslClient;

    /**
     * Create a SaslNettyClient for authentication with servers.
     */
    public SaslNettyClient(String username, String password) throws Exception {

        saslClient = Sasl.createSaslClient(
                new String[]{SaslUtils.AUTH_DIGEST_MD5}, null, null,
                SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(),
                new SaslClientCallbackHandler(username, password.toCharArray()));
        if (saslClient == null) {
            throw new IOException("Cannot create JVM SASL Client");
        }

    }

    public boolean isComplete() {
        return saslClient.isComplete();
    }

    /**
     * Respond to server's SASL token.
     *
     * @param saslTokenMessage contains server's SASL token
     * @return client's response SASL token
     */
    public byte[] saslResponse(byte[] saslTokenMessage) {
        try {
            byte[] retval = saslClient.evaluateChallenge(saslTokenMessage);
            return retval;
        } catch (SaslException e) {
            LOG.log(Level.SEVERE,
                    "saslResponse: Failed to respond to SASL server's token:",
                    e);
            return null;
        }
    }

    
    private static class SaslClientCallbackHandler implements CallbackHandler {

        
        private final String userName;
        
        private final char[] userPassword;
        
        public SaslClientCallbackHandler(String username, char[] token) {
            this.userName = username;
            this.userPassword = token;
        }

        /**
         * Implementation used to respond to SASL tokens from server.
         *
         * @param callbacks objects that indicate what credential information
         * the server's SaslServer requires from the client.
         * @throws UnsupportedCallbackException
         */
        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            RealmCallback rc = null;
            for (Callback callback : callbacks) {                
                if (callback instanceof RealmChoiceCallback) {
                    continue;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    rc = (RealmCallback) callback;
                } else {
                    throw new UnsupportedCallbackException(callback,
                            "handle: Unrecognized SASL client callback");
                }
            }
            if (nc != null) {               
                nc.setName(userName);
            }
            if (pc != null) {               
                pc.setPassword(userPassword);
            }
            if (rc != null) {                
                rc.setText(rc.getDefaultText());
            }
        }

    }
}
