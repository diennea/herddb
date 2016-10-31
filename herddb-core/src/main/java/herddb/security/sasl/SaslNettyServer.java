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

import herddb.server.Server;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
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
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.zookeeper.server.auth.KerberosName;

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

    public SaslNettyServer(Server server, String mech) throws IOException {
        this.server = server;
        try {
            Subject subject = loginServer();

            saslServer = createSaslServer(mech, subject);
            if (saslServer == null) {
                throw new IOException("Cannot create JVM SASL Server");
            }
        } catch (Exception e) {
            LOG.severe("SaslNettyServer: Could not create SaslServer: " + e);
            throw new IOException(e);
        }

    }

    private SaslServer createSaslServer(final String mech, final Subject subject) throws SaslException, IOException {
        if (subject == null) {
            SaslDigestCallbackHandler ch = new SaslNettyServer.SaslDigestCallbackHandler();
            return Sasl.createSaslServer(mech, null,
                SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(), ch);
        } else {
            SaslServerCallbackHandler callbackHandler = new SaslServerCallbackHandler(Configuration.getConfiguration());
            // server is using a JAAS-authenticated subject: determine service principal name and hostname from zk server's subject.
            if (subject.getPrincipals().size() > 0) {
                try {
                    final Object[] principals = subject.getPrincipals().toArray();
                    final Principal servicePrincipal = (Principal) principals[0];

                    final String servicePrincipalNameAndHostname = servicePrincipal.getName();
                    int indexOf = servicePrincipalNameAndHostname.indexOf("/");
                    final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf + 1, servicePrincipalNameAndHostname.length());
                    int indexOfAt = serviceHostnameAndKerbDomain.indexOf("@");

                    final String servicePrincipalName, serviceHostname;
                    if (indexOf > 0) {
                        // e.g. servicePrincipalName := "zookeeper"
                        servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);
                        // e.g. serviceHostname := "myhost.foo.com"
                        serviceHostname = serviceHostnameAndKerbDomain.substring(0, indexOfAt);
                    } else {
                        servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOfAt);
                        serviceHostname = null;
                    }

                    final String _mech = "GSSAPI";   // TODO: should depend on zoo.cfg specified mechs, but if subject is non-null, it can be assumed to be GSSAPI.

                    LOG.log(Level.INFO,"serviceHostname is ''{0}'', servicePrincipalName is ''{1}'', SASL mechanism(mech) is ''" + _mech + "'', Subject is ''{2}''", new Object[]{serviceHostname, servicePrincipalName, subject});

                    try {
                        return Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                            public SaslServer run() {
                                try {
                                    SaslServer saslServer;
                                    saslServer = Sasl.createSaslServer(_mech, servicePrincipalName, serviceHostname, null, callbackHandler);
                                    return saslServer;
                                } catch (SaslException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                        );
                    } catch (PrivilegedActionException e) {
                        // TODO: exit server at this point(?)                        
                        e.printStackTrace();
                    }
                } catch (IndexOutOfBoundsException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    SaslServer saslServer = Sasl.createSaslServer("DIGEST-MD5", "herddb", "herddb", null, callbackHandler);
                    return saslServer;
                } catch (SaslException e) {
                    e.printStackTrace();
                }
            }
        }

        LOG.severe("failed to create saslServer object.");

        return null;
    }

    private Subject loginServer() throws SaslException, PrivilegedActionException, LoginException {
        AppConfigurationEntry[] entries = Configuration.getConfiguration().getAppConfigurationEntry("HerdDBServer");
        if (entries == null) {
            return null;
        }
        String clientSection = "HerdDBServer";
        LoginContext loginContext = new LoginContext(clientSection, new ClientCallbackHandler(null));
        loginContext.login();
        return loginContext.getSubject();

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
     * Used by SaslTokenMessage::processToken() to respond to server SASL tokens.
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

    private static class SaslServerCallbackHandler implements CallbackHandler {

        private static final String USER_PREFIX = "user_";
        final String serverSection = "HerdDBServer";

        private String userName;
        private final Map<String, String> credentials = new HashMap<String, String>();

        public SaslServerCallbackHandler(Configuration configuration) throws IOException {

            AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(serverSection);

            if (configurationEntries == null) {
                String errorMessage = "Could not find a '" + serverSection + "' entry in this configuration: Server cannot start.";

                throw new IOException(errorMessage);
            }
            credentials.clear();
            for (AppConfigurationEntry entry : configurationEntries) {
                Map<String, ?> options = entry.getOptions();
                // Populate DIGEST-MD5 user -> password map with JAAS configuration entries from the "Server" section.
                // Usernames are distinguished from other options by prefixing the username with a "user_" prefix.
                for (Map.Entry<String, ?> pair : options.entrySet()) {
                    String key = pair.getKey();
                    if (key.startsWith(USER_PREFIX)) {
                        String userName = key.substring(USER_PREFIX.length());
                        credentials.put(userName, (String) pair.getValue());
                    }
                }
            }
        }

        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    handleNameCallback((NameCallback) callback);
                } else if (callback instanceof PasswordCallback) {
                    handlePasswordCallback((PasswordCallback) callback);
                } else if (callback instanceof RealmCallback) {
                    handleRealmCallback((RealmCallback) callback);
                } else if (callback instanceof AuthorizeCallback) {
                    handleAuthorizeCallback((AuthorizeCallback) callback);
                }
            }
        }

        private void handleNameCallback(NameCallback nc) {
            // check to see if this user is in the user password database.
            if (credentials.get(nc.getDefaultName()) == null) {
                LOG.severe("User '" + nc.getDefaultName() + "' not found in list of JAAS DIGEST-MD5 users.");
                return;
            }
            nc.setName(nc.getDefaultName());
            userName = nc.getDefaultName();
        }

        private void handlePasswordCallback(PasswordCallback pc) {
            if (credentials.containsKey(userName)) {
                pc.setPassword(credentials.get(userName).toCharArray());
            } else {
                LOG.severe("No password found for user: " + userName);
            }
        }

        private void handleRealmCallback(RealmCallback rc) {
            LOG.severe("client supplied realm: " + rc.getDefaultText());
            rc.setText(rc.getDefaultText());
        }

        private void handleAuthorizeCallback(AuthorizeCallback ac) {
            String authenticationID = ac.getAuthenticationID();
            String authorizationID = ac.getAuthorizationID();

            LOG.severe("Successfully authenticated client: authenticationID=" + authenticationID
                + ";  authorizationID=" + authorizationID + ".");
            ac.setAuthorized(true);

            KerberosName kerberosName = new KerberosName(authenticationID);
            try {
                StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
                userNameBuilder.append("/").append(kerberosName.getHostName());
                userNameBuilder.append("@").append(kerberosName.getRealm());
                LOG.severe("Setting authorizedID: " + userNameBuilder);
                ac.setAuthorizedID(userNameBuilder.toString());
            } catch (IOException e) {
                LOG.severe("Failed to set name based on Kerberos authentication rules.");
            }
        }

    }
}
