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
package org.herddb.ui;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

/**
 * Web application lifecycle listener.
 *
 * @author nicolo.boschi
 */
@WebListener
public class SessionListener implements HttpSessionListener {

    private static final Logger LOG = Logger.getLogger(SessionListener.class.getName());

    @Override
    public void sessionDestroyed(HttpSessionEvent se) {
        LOG.info("Destroyed session from " + se.getSource());
        HttpSession session = se.getSession();
        AutoCloseable ds = (AutoCloseable) session.getAttribute("datasource");
        if (ds != null) {
            try {
                ds.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void sessionCreated(HttpSessionEvent se) {
        LOG.info("Created session from " + se.getSource());
    }
}
