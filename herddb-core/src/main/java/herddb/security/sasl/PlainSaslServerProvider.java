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

import herddb.client.ClientConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

public class PlainSaslServerProvider extends Provider {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = Logger
            .getLogger(PlainSaslServerProvider.class.getName());

    protected PlainSaslServerProvider() {
        super("SASL/PLAIN Server Provider", 1.0, "SASL/PLAIN Server Provider for HerdDB");
        put("SaslServerFactory." + SaslUtils.AUTH_PLAIN,
                PlainSaslServerFactory.class.getName());
    }


    public static class PlainSaslServerFactory implements SaslServerFactory {
        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props,
                                           CallbackHandler callbackHandler) {
            String[] mechanismNamesCompatibleWithPolicy = getMechanismNames(props);
            for (int i = 0; i < mechanismNamesCompatibleWithPolicy.length; i++) {
                if (mechanismNamesCompatibleWithPolicy[i].equals(mechanism)) {
                    return new PlainSaslServer(callbackHandler);
                }
            }
            return null;
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return new String[]{SaslUtils.AUTH_PLAIN};
        }
    }

    public static class PlainSaslServer implements SaslServer {


        private boolean complete;
        private final CallbackHandler callbackHandler;

        private String username;

        public PlainSaslServer(CallbackHandler callbackHandler) {
            this.callbackHandler = callbackHandler;
        }

        @Override
        public byte[] evaluateResponse(byte[] response) throws SaslException {
            // see https://www.rfc-editor.org/rfc/rfc4616.html
            if (response.length == 0 || response[0] != 0) {
                throw new AuthenticationException("Invalid auth");
            }
            int endusername = -1;
            for (int i = 1; i < response.length; i++) {
                if (response[i] == 0) {
                    endusername = i;
                    break;
                }
            }
            if (endusername < 0) {
                throw new AuthenticationException("Invalid auth");
            }
            this.username = new String(response, 1, endusername - 1,
                    StandardCharsets.UTF_8);
            String password = new String(response, endusername + 1, response.length - endusername - 1,
                    StandardCharsets.UTF_8);

            NameCallback nameCallback = new NameCallback("username", ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT);
            nameCallback.setName(username);
            PasswordCallback passwordCallback = new PasswordCallback("password", false);
            passwordCallback.setPassword(password.toCharArray());
            Callback[] callbacks = new Callback[] { nameCallback, passwordCallback };


            try {
                callbackHandler.handle(callbacks);
            } catch (IOException | UnsupportedCallbackException error) {
                LOG.log(Level.SEVERE, "Error", error);
                throw new AuthenticationException("Error while performing SASL PLAIN authentication " + error, error);
            }

            complete = true;

            return new byte[0];
        }

        @Override
        public String getAuthorizationID() {
            if (!complete) {
                throw new IllegalStateException("Authentication exchange has not completed");
            }
            return username;
        }

        @Override
        public String getMechanismName() {
            return SaslUtils.AUTH_PLAIN;
        }

        @Override
        public Object getNegotiatedProperty(String propName) {
            if (!complete) {
                throw new IllegalStateException("Authentication exchange has not completed");
            }
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public byte[] unwrap(byte[] incoming, int offset, int len) {
            if (!complete) {
                throw new IllegalStateException("Authentication exchange has not completed");
            }
            return Arrays.copyOfRange(incoming, offset, offset + len);
        }

        @Override
        public byte[] wrap(byte[] outgoing, int offset, int len) {
            if (!complete) {
                throw new IllegalStateException("Authentication exchange has not completed");
            }
            return Arrays.copyOfRange(outgoing, offset, offset + len);
        }

        @Override
        public void dispose() {
            complete = false;
            username = null;
        }

    }

}
