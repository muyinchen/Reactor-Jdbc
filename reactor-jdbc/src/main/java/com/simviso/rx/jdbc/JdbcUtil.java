package com.simviso.rx.jdbc;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time  2018/8/24 21:59.
 */
public enum  JdbcUtil {

    ;
    private static final Logger log = LoggerFactory.getLogger(JdbcUtil.class);

    public static void closeSilently(ResultSet rs) {
        Statement stmt = null;
        try {
            stmt = rs.getStatement();
        } catch (SQLException e) {
            // ignore
        }
        try {
            rs.close();
        } catch (SQLException e) {
            // ignore
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                // ignore
            }
            Connection con = null;
            try {
                con = stmt.getConnection();
            } catch (SQLException e1) {
                // ignore
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

    }

    public static void closeAll(Statement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            // ignore
        }
        Connection con = null;
        try {
            con = stmt.getConnection();
        } catch (SQLException e1) {
            // ignore
        }
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }


    /**
     * Sets parameters for the {@link PreparedStatement}.
     *
     * @param ps
     * @param params
     * @throws SQLException
     */
    static void setParameters(PreparedStatement ps, List<Parameter> params, boolean namesAllowed) throws SQLException {
        for (int i = 1; i <= params.size(); i++) {
            if (params.get(i - 1).hasName() && !namesAllowed)
                throw new SQLException("named parameter found but sql does not contain names");
            Object o = params.get(i - 1).value();
            try {
                if (o == null)
                    ps.setObject(i, null);
                else if (o == Database.NULL_CLOB)
                    ps.setNull(i, Types.CLOB);
                else if (o == Database.NULL_BLOB)
                    ps.setNull(i, Types.BLOB);
                else {
                    Class<?> cls = o.getClass();
                    if (Clob.class.isAssignableFrom(cls)) {
                        setClob(ps, i, o, cls);
                    } else if (Blob.class.isAssignableFrom(cls)) {
                        setBlob(ps, i, o, cls);
                    } else if (Calendar.class.isAssignableFrom(cls)) {
                        Calendar cal = (Calendar) o;
                        Timestamp t = new java.sql.Timestamp(cal.getTimeInMillis());
                        ps.setTimestamp(i, t, cal);
                    } else if (Time.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setTime(i, (Time) o, cal);
                    } else if (Timestamp.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setTimestamp(i, (Timestamp) o, cal);
                    } else if (java.sql.Date.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        ps.setDate(i, (java.sql.Date) o, cal);
                    } else if (java.util.Date.class.isAssignableFrom(cls)) {
                        Calendar cal = Calendar.getInstance();
                        java.util.Date date = (java.util.Date) o;
                        ps.setTimestamp(i, new java.sql.Timestamp(date.getTime()), cal);
                    } else
                        ps.setObject(i, o);
                }
            } catch (SQLException e) {
                log.debug("{} when setting ps.setObject({},{})", e.getMessage(), i, o);
                throw e;
            }
        }
    }

    /**
     * Sets a blob parameter for the prepared statement.
     *
     * @param ps
     * @param i
     * @param o
     * @param cls
     * @throws SQLException
     */
    private static void setBlob(PreparedStatement ps, int i, Object o, Class<?> cls) throws SQLException {
        final InputStream is;
        if (o instanceof byte[]) {
            is = new ByteArrayInputStream((byte[]) o);
        } else if (o instanceof InputStream)
            is = (InputStream) o;
        else
            throw new RuntimeException("cannot insert parameter of type " + cls + " into blob column " + i);
        Blob c = ps.getConnection().createBlob();
        OutputStream os = c.setBinaryStream(1);
        copy(is, os);
        ps.setBlob(i, c);
    }

    /**
     * Sets the clob parameter for the prepared statement.
     *
     * @param ps
     * @param i
     * @param o
     * @param cls
     * @throws SQLException
     */
    private static void setClob(PreparedStatement ps, int i, Object o, Class<?> cls) throws SQLException {
        final Reader r;
        if (o instanceof String)
            r = new StringReader((String) o);
        else if (o instanceof Reader)
            r = (Reader) o;
        else
            throw new RuntimeException("cannot insert parameter of type " + cls + " into clob column " + i);
        Clob c = ps.getConnection().createClob();
        Writer w = c.setCharacterStream(1);
        copy(r, w);
        ps.setClob(i, c);
    }

    /**
     * Copies a {@link Reader} to a {@link Writer}.
     *
     * @param input
     * @param output
     * @return
     */
    private static int copy(Reader input, Writer output) {
        try {
            return IOUtils.copy(input, output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Copies an {@link InputStream} to an {@link OutputStream}.
     *
     * @param input
     * @param output
     * @return
     */
    private static int copy(InputStream input, OutputStream output) {
        try {
            return IOUtils.copy(input, output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setNamedParameters(PreparedStatement ps, List<Parameter> parameters,
                                           List<String> names) throws SQLException {
        Map<String, Parameter> map = new HashMap<>();
        for (Parameter p : parameters) {
            if (p.hasName()) {
                map.put(p.name(), p);
            } else {
                throw new SQLException(
                        "named parameters were expected but this parameter did not have a name: "
                                + p);
            }
        }
        List<Parameter> list = new ArrayList<>();
        for (String name : names) {
            if (!map.containsKey(name))
                throw new SQLException("named parameter is missing for '" + name + "'");
            Parameter p = map.get(name);
            list.add(p);
        }
        JdbcUtil.setParameters(ps, list, true);
    }

    static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters, List<String> names)
            throws SQLException {
        List<Parameter> params = parameters.stream().map(o -> {
            if (o instanceof Parameter) {
                return (Parameter) o;
            } else {
                return new Parameter(o);
            }
        }).collect(Collectors.toList());
        if (names.isEmpty()) {
            JdbcUtil.setParameters(ps, params, false);
        } else {
            JdbcUtil.setNamedParameters(ps, params, names);
        }
        return ps;
    }

    static void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception e) {
            // ignore
        }
    }

    static void closePreparedStatementAndConnection(PreparedStatement ps) {
        Connection con = null;
        try {
            con = ps.getConnection();
        } catch (SQLException ignored) {
        }
        closeSilently(ps);
        if (con != null) {
            closeSilently(con);
        }
    }

    static NamedPreparedStatement prepare(Connection con, String sql) throws SQLException {
        SqlWithNames s = SqlWithNames.parse(sql);
        return new NamedPreparedStatement(con.prepareStatement(s.sql()), s.names());
    }

    static NamedPreparedStatement prepareReturnGeneratedKeys(Connection con, String sql) throws SQLException {
        SqlWithNames s = SqlWithNames.parse(sql);
        return new NamedPreparedStatement(con.prepareStatement(s.sql(), Statement.RETURN_GENERATED_KEYS), s.names());
    }


}
