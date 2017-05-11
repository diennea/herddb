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

import herddb.jdbc.HerdDBDataSource;
import herddb.jdbc.HerdDBEmbeddedDataSource;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.sql.DataSource;

@WebListener
public class Initializer implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        HerdDBDataSource datasource = new HerdDBDataSource();
        System.out.println("Initializing datasource " + datasource.getUrl());
        sce.getServletContext().setAttribute("datasource", datasource);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        AutoCloseable datasource = (AutoCloseable) sce.getServletContext().getAttribute("datasource");
        System.out.println("Closing datasource " + datasource);
        if (datasource != null) {
            try {
                datasource.close();
            } catch (Exception err) {
                err.printStackTrace();
            }
        }
    }
}
