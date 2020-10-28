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
import herddb.jdbc.Driver;
import java.util.Map;
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
public class DataSourceTest {

    @Test
    public void hello() throws Exception {

        // create an manage your own datasource
        // with autoClose=true the server will be shutdown when you shutdown the datasource
        org.apache.commons.dbcp2.BasicDataSource ds2 = new org.apache.commons.dbcp2.BasicDataSource();
        ds2.setUrl("jdbc:herddb:local?autoClose=true");
        ds2.setUsername("sa");
        ds2.setPassword("hdb");
        ds2.setMinIdle(1);
        ds2.setDriverClassName(Driver.class.getName());
        ds2.setInitialSize(1);

        final EntityManagerFactory factory = Persistence.createEntityManagerFactory("hdb_ds",
                Map.of("openjpa.ConnectionFactory", ds2));

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

        ds2.close();

    }
}
