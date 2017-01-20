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

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.zookeeper.server.auth.KerberosName;

/**
 * Sasl Client
 *
 * @author enrico.olivelli
 */
public class SaslNettyClient {

    private static final Logger LOG = Logger
        .getLogger(SaslNettyClient.class.getName());

    /**
     * Used to respond to server's counterpart, SaslServer with SASL tokens represented as byte arrays.
     */
    private SaslClient saslClient;
    private Subject clientSubject;

    /**
     * Create a SaslNettyClient for authentication with servers.
     */
    public SaslNettyClient(String username, String password, String serverHostname) throws Exception {
        String serverPrincipal = "herddb/" + serverHostname;
        clientSubject = loginClient();

        if (clientSubject == null) {
            LOG.log(Level.FINEST, "Using plain SASL/DIGEST-MD5 auth to connect to " + serverHostname);
            saslClient = Sasl.createSaslClient(
                new String[]{SaslUtils.AUTH_DIGEST_MD5}, null, null,
                SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(),
                new SaslClientCallbackHandler(username, password.toCharArray()));
        } else if (clientSubject.getPrincipals().isEmpty()) {
            LOG.log(Level.FINEST, "Using JAAS/SASL/DIGEST-MD5 auth to connect to " + serverPrincipal);
            String[] mechs = {"DIGEST-MD5"};
            username = (String) (clientSubject.getPublicCredentials().toArray()[0]);
            password = (String) (clientSubject.getPrivateCredentials().toArray()[0]);
            saslClient = Sasl.createSaslClient(mechs, username, "herddb", "herddb", null, new ClientCallbackHandler(password));
        } else { // GSSAPI.
            final Object[] principals = clientSubject.getPrincipals().toArray();
            // determine client principal from subject.
            final Principal clientPrincipal = (Principal) principals[0];
            final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
            KerberosName serviceKerberosName = new KerberosName(serverPrincipal + "@" + clientKerberosName.getRealm());
            final String serviceName = serviceKerberosName.getServiceName();
            final String serviceHostname = serviceKerberosName.getHostName();
            final String clientPrincipalName = clientKerberosName.toString();
            LOG.log(Level.FINEST, "Using JAAS/SASL/GSSAPI auth to connect to server Principal " + serverPrincipal);
            saslClient = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SaslClient>() {
                @Override
                public SaslClient run() throws SaslException {
                    String[] mechs = {"GSSAPI"};
                    return Sasl.createSaslClient(mechs, clientPrincipalName, serviceName, serviceHostname, null, new ClientCallbackHandler(null));
                }
            });
        }
        if (saslClient == null) {
            throw new IOException("Cannot create JVM SASL Client");
        }

    }

    public byte[] evaluateChallenge(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
            throw new SaslException("saslToken is null.");
        }

        if (clientSubject != null) {            
            try {
                final byte[] retval
                    = Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws SaslException {
                            return saslClient.evaluateChallenge(saslToken);
                        }
                    });
                return retval;
            } catch (PrivilegedActionException e) {
                e.printStackTrace();
                throw new SaslException("SASL/JAAS error", e);
            }
        } else {
            return saslClient.evaluateChallenge(saslToken);
        }
    }

    private Subject loginClient() throws SaslException, PrivilegedActionException, LoginException {
        String clientSection = "HerdDBClient";
        AppConfigurationEntry[] entries = Configuration.getConfiguration().getAppConfigurationEntry(clientSection);
        if (entries == null) {
            LOG.log(Level.FINEST, "No JAAS Configuration found with section HerdDBClient");
            return null;
        }
        try {
            LoginContext loginContext = new LoginContext(clientSection, new ClientCallbackHandler(null));
            loginContext.login();
            LOG.log(Level.SEVERE, "Using JAAS Configuration subject: " + loginContext.getSubject());
            return loginContext.getSubject();
        } catch (LoginException error) {
            LOG.log(Level.SEVERE, "Error JAAS Configuration subject: " + error, error);
            return null;
        }
    }

    public boolean hasInitialResponse() {
        return saslClient.hasInitialResponse();
    }

    private static class ClientCallbackHandler implements CallbackHandler {

        private String password = null;

        public ClientCallbackHandler(String password) {
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws
            UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else {
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback pc = (PasswordCallback) callback;
                        if (password != null) {
                            pc.setPassword(this.password.toCharArray());
                        }
                    } else {
                        if (callback instanceof RealmCallback) {
                            RealmCallback rc = (RealmCallback) callback;
                            rc.setText(rc.getDefaultText());
                        } else {
                            if (callback instanceof AuthorizeCallback) {
                                AuthorizeCallback ac = (AuthorizeCallback) callback;
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
                            } else {
                                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
                            }
                        }
                    }
                }
            }
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
         * @param callbacks objects that indicate what credential information the server's SaslServer requires from the
         * client.
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
