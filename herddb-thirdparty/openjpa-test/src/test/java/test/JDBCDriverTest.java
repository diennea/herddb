/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package test;

import static org.junit.Assert.assertEquals;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import org.junit.Test;
import test.entity.Address;
import test.entity.User;

/**
 *
 * @author enrico.olivelli
 */
public class JDBCDriverTest {

    @Test
    public void hello() throws Exception {


        final EntityManagerFactory factory = Persistence.createEntityManagerFactory("hdb_jdbc");

        {
            final EntityManager em = factory.createEntityManager();
            final EntityTransaction transaction = em.getTransaction();
            transaction.begin();
            em.persist(new User(0, "First", 10, "Something", new Address(1, "Localhost")));
            transaction.commit();
            em.close();
        }
        {
            final EntityManager em = factory.createEntityManager();
            assertEquals(1, em.createQuery("select e from User e").getResultList().size());
            em.close();
        }
        factory.close();

        // clean up, unregistring the driver will shutdown every datasource
        for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements();) {
            Driver d = e.nextElement();
            DriverManager.deregisterDriver(d);
        }

    }
}
