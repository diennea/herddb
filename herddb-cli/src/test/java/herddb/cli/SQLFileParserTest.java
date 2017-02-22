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
package herddb.cli;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class SQLFileParserTest {

    @Test
    public void testParseSQLFile() throws Exception {
        String sql = "-- MySQL dump 10.13  Distrib 5.6.35, for Linux (x86_64)\n"
            + "--\n"
            + "-- Host: localhost    Database: seleniummonitor\n"
            + "-- ------------------------------------------------------\n"
            + "-- Server version	5.6.35\n"
            + "\n"
            + "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n"
            + "/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n"
            + "/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n"
            + "/*!40101 SET NAMES utf8 */;\n"
            + "/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n"
            + "/*!40103 SET TIME_ZONE='+00:00' */;\n"
            + "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n"
            + "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n"
            + "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n"
            + "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n"
            + "\n"
            + "--\n"
            + "-- Table structure for table `sm_general_log`\n"
            + "--\n"
            + "\n"
            + "DROP TABLE IF EXISTS `sm_general_log`;\n"
            + "/*!40101 SET @saved_cs_client     = @@character_set_client */;\n"
            + "/*!40101 SET character_set_client = utf8 */;\n"
            + "CREATE TABLE `sm_general_log` (\n"
            + "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `log` varchar(2000) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;\n"
            + "/*!40101 SET character_set_client = @saved_cs_client */;\n"
            + "\n"
            + "--\n"
            + "-- Dumping data for table `sm_general_log`\n"
            + "--\n"
            + "\n"
            + "LOCK TABLES `sm_general_log` WRITE;\n"
            + "/*!40000 ALTER TABLE `sm_general_log` DISABLE KEYS */;\n"
            + "INSERT INTO `sm_general_log` VALUES (2,'my string with accent '' and a semicolon;');\n"
            + "INSERT INTO `sm_general_log` VALUES (2,'my string with accent '' and a semicolon;');\n"
            + "INSERT INTO `sm_general_log` VALUES (2,'my string with accent \\' and a semicolon;');\n"
            + "/*!40000 ALTER TABLE `sm_general_log` ENABLE KEYS */;\n"
            + "UNLOCK TABLES;\n"
            + "\n"
            + "--";
        List<SQLFileParser.Statement> res = new ArrayList<>();
        SQLFileParser.parseSQLFile(new StringReader(sql), (s -> {
            if (s.comment) {
                System.out.println("comment: '" + s.content + "'");
            } else {
                System.out.println("statement:'" + s.content + "'");
            }
            res.add(s);
        }));
    }

}
