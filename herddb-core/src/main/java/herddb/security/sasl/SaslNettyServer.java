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

import herddb.client.ClientConfiguration;
import herddb.server.Server;
import java.io.IOException;
import java.util.logging.Logger;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.jute.compiler.generated.Rcc;

/**
 * Server side Sasl implementation
 *
 * @author enrico.olivelli
 */
public class SaslNettyServer {

    private static final Logger LOG = Logger
            .getLogger(SaslNettyServer.class.getName());

    private SaslServer saslServer;
    private Server server;

    public SaslNettyServer(Server server) throws IOException {
        this.server = server;
        try {
            SaslDigestCallbackHandler ch = new SaslNettyServer.SaslDigestCallbackHandler();
            saslServer = Sasl.createSaslServer(SaslUtils.AUTH_DIGEST_MD5, null,
                    SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(), ch);
            if (saslServer == null) {
                throw new IOException("Cannot create JVM SASL Server");
            }
        } catch (SaslException e) {
            LOG.severe("SaslNettyServer: Could not create SaslServer: " + e);
            throw new IOException(e);
        }

    }

    public boolean isComplete() {
        return saslServer.isComplete();
    }

    public String getUserName() {
        return saslServer.getAuthorizationID();
    }

    /**
     * CallbackHandler for SASL DIGEST-MD5 mechanism
     */
    private class SaslDigestCallbackHandler implements CallbackHandler {

        public SaslDigestCallbackHandler() {
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException,
                UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            AuthorizeCallback ac = null;

            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    ac = (AuthorizeCallback) callback;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    continue; // realm is ignored
                } else {
                    throw new UnsupportedCallbackException(callback,
                            "handle: Unrecognized SASL DIGEST-MD5 Callback");
                }
            }

            String authenticatingUser = null;
            if (nc != null) {
                authenticatingUser = nc.getDefaultName();
                LOG.finest("SASL server auth user " + authenticatingUser);
                nc.setName(nc.getDefaultName());
            }
            if (pc != null) {
                String expectedPassword = server.getUserManager().getExpectedPassword(authenticatingUser);
                if (expectedPassword != null) {
                    pc.setPassword(expectedPassword.toCharArray());
                }
            }

            if (ac != null) {
                String authid = ac.getAuthenticationID();
                String authzid = ac.getAuthorizationID();
                if (authid.equals(authzid)) {
                    ac.setAuthorized(true);
                } else {
                    ac.setAuthorized(false);
                }
                if (ac.isAuthorized()) {
                    ac.setAuthorizedID(authzid);
                }
            }
        }
    }

    /**
     * Used by SaslTokenMessage::processToken() to respond to server SASL
     * tokens.
     *
     * @param token Server's SASL token
     * @return token to send back to the server.
     */
    public byte[] response(byte[] token) throws SaslException {
        try {            
            byte[] retval = saslServer.evaluateResponse(token);            
            return retval;
        } catch (SaslException e) {
            LOG.severe("response: Failed to evaluate client token of length: "
                    + token.length + " : " + e);
            throw e;
        }
    }
}
