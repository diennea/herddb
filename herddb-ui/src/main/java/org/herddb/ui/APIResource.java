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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.jdbc.HerdDBDataSource;
import herddb.model.TableSpaceDoesNotExistException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.ws.rs.core.Context;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import static org.herddb.ui.Utils.TS_DEFAULT_PARAM;
import static org.herddb.ui.Utils.formatValue;

@Path("api")
@Produces(MediaType.APPLICATION_JSON)
@SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION", justification = "This is a spotbugs bug")
public class APIResource {

    @Context
    private HttpServletRequest servletRequest;

    protected Connection getConnection() throws SQLException {
        DataSource ds = (DataSource) servletRequest.getSession().getAttribute("datasource");
        if (ds != null) {
            return ds.getConnection();
        } else {
            return null;
        }
    }

    @GET
    @Path("/checklogin")
    public boolean checklogin() {
        DataSource ds = (DataSource) servletRequest.getSession().getAttribute("datasource");
        return ds != null;
    }

    @GET
    @Path("/defaultts")
    public Map<String, String> getDefaultTs() {
        String defaultTablespace = (String) servletRequest.getSession().getAttribute("defaultts");
        Map<String, String> map = new HashMap<>();
        map.put("default", defaultTablespace);
        return map;
    }

    @GET
    @Path("/jdbcurl")
    public Map<String, String> getUrl() {
        String url = (String) servletRequest.getSession().getAttribute("jdbcurl");
        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        return map;
    }

    @GET
    @Path("/logout")
    public void logout() {
        HttpSession session = servletRequest.getSession();
        if (session != null) {
            session.invalidate();
        }
    }

    @SuppressFBWarnings({"J2EE_STORE_OF_NON_SERIALIZABLE_OBJECT_INTO_SESSION"})
    @POST
    @Path("/login")
    public Map<String, Object> login(@FormParam("datasource") String ds,
            @FormParam("username") String username,
            @FormParam("password") String pwd,
            @FormParam("defaultts") String defaultts
    ) {

        Map<String, Object> res = new HashMap<>();
        res.put("defaultts", defaultts);
        HerdDBDataSource da = new HerdDBDataSource();
        da.setUrl(ds);
        da.setUsername(username);
        da.setPassword(pwd);
        try (Connection conn = da.getConnection();
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) from " + defaultts + ".systables")) {
            res.put("ok", true);
            HttpSession session = servletRequest.getSession(true);
            session.setMaxInactiveInterval(60 * 5);
            session.setAttribute("datasource", da);
            session.setAttribute("defaultts", defaultts);
            session.setAttribute("jdbcurl", ds);
        } catch (SQLException | IllegalArgumentException e) {
            LOG.log(Level.SEVERE, "error", e);
            res.put("ok", false);
            if (e.getMessage().contains(UnknownHostException.class.getName()) || e.getMessage().contains("invalid url")) {
                res.put("errormessage", "JDBC URL is not correct. The host " + ds + " is unreachable.");
            } else if (e.getMessage().contains("Authentication failed")) {
                res.put("errormessage", "Username and password are not correct.");
            } else if (e.getMessage().contains(TableSpaceDoesNotExistException.class.getName())) {
                res.put("errormessage", "Tablespace " + defaultts + " doesn't exist.");
            }
            res.put("sqlerror", e.getMessage());
        }
        return res;
    }
    private static final Logger LOG = Logger.getLogger(APIResource.class.getName());

    @GET
    @Path("/test")
    public String test() {
        try (Connection con = getConnection()) {
            return "test! " + con;
        } catch (SQLException | IllegalArgumentException err) {
            return "Internal error: " + err;
        }
    }

    @GET
    @Path("/tablespaces")
    public List<List<Object>> tablespaces(@QueryParam(TS_DEFAULT_PARAM) String defaultTs) {
        try (Connection con = getConnection();
                Statement s = con.createStatement();
                ResultSet rs = s.executeQuery("SELECT * from " + defaultTs + ".systablespaces")) {
            List<List<Object>> result = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                row.add(formatValue(rs.getObject("tablespace_name")));
                row.add(formatValue(rs.getObject("uuid")));
                row.add(formatValue(rs.getObject("leader")));
                row.add(formatValue(rs.getObject("replica")));
                row.add(formatValue(rs.getObject("expectedreplicacount")));
                row.add(formatValue(rs.getObject("maxleaderinactivitytime")));
                result.add(row);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/table")
    public Map<String, List<List<Object>>> table(@QueryParam("tb") String table, @QueryParam("ts") String ts) {
        Map<String, List<List<Object>>> result = new HashMap<>();

        try (Connection con = getConnection();
                PreparedStatement indexes = con.prepareStatement("SELECT * from " + ts + ".sysindexes where table_name=?");
                PreparedStatement stats = con.prepareStatement("SELECT * from " + ts + ".systablestats where table_name=?");
                PreparedStatement metadata = con.prepareStatement("SELECT * from " + ts + ".syscolumns where table_name=?")) {
            metadata.setString(1, table);
            indexes.setString(1, table);
            stats.setString(1, table);
            try (ResultSet rs = metadata.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("column_name")));
                    row.add(formatValue(rs.getObject("ordinal_position")));
                    row.add(formatValue(rs.getObject("is_nullable"), true));
                    row.add(formatValue(rs.getObject("data_type")));
                    row.add(formatValue(rs.getObject("auto_increment"), true));
                    statsResult.add(row);
                }
                result.put("metadata", statsResult);
            }

            try (ResultSet rs = indexes.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("index_name")));
                    row.add(formatValue(rs.getObject("index_type")));
                    statsResult.add(row);
                }
                result.put("indexes", statsResult);
            }

            try (ResultSet rs = stats.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("tablesize")));
                    row.add(formatValue(rs.getObject("loadedpages")));
                    row.add(formatValue(rs.getObject("loadedpagescount")));
                    row.add(formatValue(rs.getObject("unloadedpagescount")));
                    row.add(formatValue(rs.getObject("dirtypages")));
                    row.add(formatValue(rs.getObject("dirtyrecords")));
                    row.add(formatValue(rs.getObject("maxlogicalpagesize")));
                    row.add(formatValue(rs.getObject("keysmemory")));
                    row.add(formatValue(rs.getObject("buffersmemory")));
                    row.add(formatValue(rs.getObject("dirtymemory")));
                    statsResult.add(row);
                }
                result.put("stats", statsResult);
            }

            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/nodes")
    public List<List<Object>> nodes(@QueryParam(TS_DEFAULT_PARAM) String defaultTs) {
        try (Connection con = getConnection();
                Statement s = con.createStatement();
                ResultSet rs = s.executeQuery("SELECT * from " + defaultTs + ".sysnodes")) {
            int count = rs.getMetaData().getColumnCount();

            List<List<Object>> result = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                row.add(formatValue(rs.getObject("nodeid")));
                row.add(formatValue(rs.getObject("address")));
                row.add(formatValue(rs.getObject("ssl"), true));
                result.add(row);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/node")
    public Map<String, List<List<Object>>> node(@QueryParam("nd") String nodeid) {
        try (Connection con = getConnection();
                Statement s = con.createStatement();
                ResultSet rs = s.executeQuery("SELECT * from " + nodeid + ".sysconfig")) {
            int count = rs.getMetaData().getColumnCount();

            List<List<Object>> nodes = new ArrayList<>();
            Map<String, List<List<Object>>> result = new HashMap<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= count; i++) {
                    row.add(formatValue(rs.getObject(i)));
                }
                nodes.add(row);
            }
            result.put("data", nodes);
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/tablespace/stats")
    public Map<String, List<List<Object>>> stats(@QueryParam("ts") String ts) {
        Map<String, List<List<Object>>> result = new HashMap<>();

        try (Connection con = getConnection();
                PreparedStatement stats = con.prepareStatement("SELECT * from " + ts + ".systablestats");) {
            try (ResultSet rs = stats.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("table_name")));
                    row.add(formatValue(rs.getObject("systemtable"), true));
                    row.add(formatValue(rs.getObject("tablesize")));
                    row.add(formatValue(rs.getObject("loadedpages")));
                    row.add(formatValue(rs.getObject("loadedpagescount")));
                    row.add(formatValue(rs.getObject("unloadedpagescount")));
                    row.add(formatValue(rs.getObject("dirtypages")));
                    row.add(formatValue(rs.getObject("dirtyrecords")));
                    row.add(formatValue(rs.getObject("maxlogicalpagesize")));
                    row.add(formatValue(rs.getObject("keysmemory")));
                    row.add(formatValue(rs.getObject("buffersmemory")));
                    row.add(formatValue(rs.getObject("dirtymemory")));
                    statsResult.add(row);
                }
                result.put("stats", statsResult);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/tablespace/tables")
    public Map<String, List<List<Object>>> tables(@QueryParam("ts") String ts) {
        Map<String, List<List<Object>>> result = new HashMap<>();

        try (Connection con = getConnection();
                PreparedStatement tables = con.prepareStatement("SELECT * from " + ts + ".systables where systemtable=false");) {
            try (ResultSet rs = tables.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("table_name")));
                    statsResult.add(row);
                }
                result.put("tables", statsResult);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/tablespace/replicastate")
    public Map<String, List<List<Object>>> replicastate(@QueryParam("ts") String ts) {
        Map<String, List<List<Object>>> result = new HashMap<>();

        try (Connection con = getConnection();
                PreparedStatement repState = con.prepareStatement("SELECT * from " + ts + ".systablespacereplicastate where tablespace_name=?");) {
            repState.setString(1, ts);
            try (ResultSet rs = repState.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("uuid")));
                    row.add(formatValue(rs.getObject("nodeid")));
                    row.add(formatValue(rs.getObject("mode")));
                    row.add(formatValue(rs.getObject("timestamp")));
                    row.add(formatValue(rs.getObject("maxleaderinactivitytime")));
                    row.add(formatValue(rs.getObject("inactivitytime")));
                    statsResult.add(row);
                }
                result.put("replication", statsResult);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

    @GET
    @Path("/tablespace/transactions")
    public Map<String, List<List<Object>>> transactions(@QueryParam("ts") String ts) {
        Map<String, List<List<Object>>> result = new HashMap<>();

        try (Connection con = getConnection();
                PreparedStatement transactions = con.prepareStatement("SELECT * from " + ts + ".systransactions")) {
            try (ResultSet rs = transactions.executeQuery()) {
                List<List<Object>> statsResult = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(formatValue(rs.getObject("txid")));
                    row.add(formatValue(rs.getObject("creationtimestamp")));
                    statsResult.add(row);
                }
                result.put("transactions", statsResult);
            }
            return result;
        } catch (SQLException | IllegalArgumentException err) {
            err.printStackTrace();
            throw new WebApplicationException(err);
        }
    }

}
