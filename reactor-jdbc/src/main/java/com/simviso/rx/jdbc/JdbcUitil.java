package com.simviso.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Author  知秋
 * @email fei6751803@163.com
 * @time Created by Auser on 2018/8/24 21:59.
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
}
