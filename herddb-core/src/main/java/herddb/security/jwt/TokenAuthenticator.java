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
package herddb.security.jwt;

import herddb.security.UserManager;
import herddb.server.ServerConfiguration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.RequiredTypeException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.SignatureException;
import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

public class TokenAuthenticator extends UserManager {

    private static final Logger LOG = Logger.getLogger(TokenAuthenticator.class.getName());
    static final String CONF_TOKEN_SETTING_PREFIX = "server.tokenSettingPrefix";
    static final String CONF_TOKEN_SETTING_PREFIX_DEFAULT = "server.token.";

    // When symmetric key is configured
    static final String CONF_TOKEN_SECRET_KEY = "tokenSecretKey";

    // When public/private key pair is configured
    static final String CONF_TOKEN_PUBLIC_KEY = "tokenPublicKey";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";

    // When using public key's, the algorithm of the key
    static final String CONF_TOKEN_PUBLIC_ALG = "tokenPublicAlg";

    // The token audience "claim" name, e.g. "aud", that will be used to get the audience from token.
    static final String CONF_TOKEN_AUDIENCE_CLAIM = "tokenAudienceClaim";

    // The token audience stands for this server. The field `tokenAudienceClaim` of a valid token, need contains this.
    static final String CONF_TOKEN_AUDIENCE = "tokenAudience";

    static final String TOKEN = "token";
    private String confTokenSecretKeySettingName;
    private String confTokenPublicKeySettingName;
    private String confTokenAuthClaimSettingName;
    private String confTokenPublicAlgSettingName;
    private String confTokenAudienceClaimSettingName;
    private String confTokenAudienceSettingName;
    private Key validationKey;
    private String roleClaim;
    private SignatureAlgorithm publicKeyAlg;
    private String audienceClaim;
    private String audience;
    private JwtParser parser;

    public TokenAuthenticator(ServerConfiguration config) throws Exception {
        String prefix = config.getString(CONF_TOKEN_SETTING_PREFIX, CONF_TOKEN_SETTING_PREFIX_DEFAULT);
        this.confTokenSecretKeySettingName = prefix + CONF_TOKEN_SECRET_KEY;
        this.confTokenPublicKeySettingName = prefix + CONF_TOKEN_PUBLIC_KEY;
        this.confTokenAuthClaimSettingName = prefix + CONF_TOKEN_AUTH_CLAIM;
        this.confTokenPublicAlgSettingName = prefix + CONF_TOKEN_PUBLIC_ALG;
        this.confTokenAudienceClaimSettingName = prefix + CONF_TOKEN_AUDIENCE_CLAIM;
        this.confTokenAudienceSettingName = prefix + CONF_TOKEN_AUDIENCE;
        this.publicKeyAlg = getPublicKeyAlgType(config);
        this.validationKey = getValidationKey(config);
        this.roleClaim = getTokenRoleClaim(config);
        this.audienceClaim = getTokenAudienceClaim(config);
        this.audience = getTokenAudience(config);

        this.parser = Jwts.parserBuilder().setSigningKey(this.validationKey).build();

        if (audienceClaim != null && audience == null) {
            throw new IllegalArgumentException("Token Audience Claim [" + audienceClaim
                    + "] configured, but Audience stands for this server not.");
        }
    }

    @Override
    public String getExpectedPassword(String username) throws IOException {
        throw new IOException("Unsupported with JWT authentication");
    }

    @Override
    public void authenticate(String username, char[] pwd) throws IOException {
        Jwt<?, Claims> jwt = parser.parseClaimsJws(new String(pwd));
        if (audienceClaim != null) {
            Object object = jwt.getBody().get(audienceClaim);
            if (object == null) {
                throw new JwtException("Found null Audience in token, for claimed field: " + audienceClaim);
            }

            if (object instanceof List) {
                List<String> audiences = (List<String>) object;
                // audience not contains this server, throw exception.
                if (audiences.stream().noneMatch(audienceInToken -> audienceInToken.equals(audience))) {
                    throw new IOException("Audiences in token: [" + String.join(", ", audiences)
                            + "] not contains this server: " + audience);
                }
            } else if (object instanceof String) {
                if (!object.equals(audience)) {
                    throw new IOException("Audiences in token: [" + object
                            + "] not contains this server: " + audience);
                }
            } else {
                // should not reach here.
                throw new IOException("Audiences in token is not in expected format: " + object);
            }
        }

        String role = getPrincipal(jwt);
        if (role == null) {
            throw new IOException("Found null role in token, for claimed field: " + roleClaim);
        }
        LOG.log(Level.INFO, "Authenticated user {0} with role {1}", new Object[]{username, role});

    }

    private String getPrincipal(Jwt<?, Claims> jwt) {
        try {
            return jwt.getBody().get(roleClaim, String.class);
        } catch (RequiredTypeException requiredTypeException) {
            List list = jwt.getBody().get(roleClaim, List.class);
            if (list != null && !list.isEmpty() && list.get(0) instanceof String) {
                return (String) list.get(0);
            }
            return null;
        }
    }

    private String getTokenRoleClaim(ServerConfiguration conf) throws IOException {
        String tokenAuthClaim = conf.getString(confTokenAuthClaimSettingName, "");
        if (StringUtils.isNotBlank(tokenAuthClaim)) {
            return tokenAuthClaim;
        } else {
            return Claims.SUBJECT;
        }
    }

    /**
     * Try to get the validation key for tokens from several possible config options.
     */
    private Key getValidationKey(ServerConfiguration conf) throws IOException {
        LOG.log(Level.INFO, "Trying to get validation key for token authentication from {0} and {1}",
                new Object[] {confTokenSecretKeySettingName, confTokenPublicKeySettingName});
        String tokenSecretKey = conf.getString(confTokenSecretKeySettingName, "");
        String tokenPublicKey = conf.getString(confTokenPublicKeySettingName, "");
        if (StringUtils.isNotBlank(tokenSecretKey)) {
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(tokenSecretKey);
            return AuthTokenUtils.decodeSecretKey(validationKey);
        } else if (StringUtils.isNotBlank(tokenPublicKey)) {
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(tokenPublicKey);
            return AuthTokenUtils.decodePublicKey(validationKey, publicKeyAlg);
        } else {
            throw new IOException("No secret key was provided for token authentication");
        }
    }

    private SignatureAlgorithm getPublicKeyAlgType(ServerConfiguration conf) throws IllegalArgumentException {
        String tokenPublicAlg = conf.getString(confTokenPublicAlgSettingName, "");
        if (StringUtils.isNotBlank(tokenPublicAlg)) {
            try {
                return SignatureAlgorithm.forName(tokenPublicAlg);
            } catch (SignatureException ex) {
                throw new IllegalArgumentException("invalid algorithm provided " + tokenPublicAlg, ex);
            }
        } else {
            return SignatureAlgorithm.RS256;
        }
    }

    // get Token Audience Claim from configuration, if not configured return null.
    private String getTokenAudienceClaim(ServerConfiguration conf) throws IllegalArgumentException {
        String tokenAudienceClaim = conf.getString(confTokenAudienceClaimSettingName, "");
        if (StringUtils.isNotBlank(tokenAudienceClaim)) {
            return tokenAudienceClaim;
        } else {
            return null;
        }
    }

    // get Token Audience that stands for this server from configuration, if not configured return null.
    private String getTokenAudience(ServerConfiguration conf) throws IllegalArgumentException {
        String tokenAudience = conf.getString(confTokenAudienceSettingName, "");
        if (StringUtils.isNotBlank(tokenAudience)) {
            return tokenAudience;
        } else {
            return null;
        }
    }
}
