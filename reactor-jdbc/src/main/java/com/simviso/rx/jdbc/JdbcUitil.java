package com.simviso.rx.jdbc;

import java.sql.*;
import java.util.List;

/**
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time  2018/8/24 21:59.
 */
public class JdbcUitil {

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

    static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters) {
        //TODO
        return ps;
    }
}
