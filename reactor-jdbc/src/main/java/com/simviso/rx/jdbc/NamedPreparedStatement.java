package com.simviso.rx.jdbc;

import java.sql.PreparedStatement;
import java.util.List;

/**
 * Corresponding encapsulation of PreparedStatement for Parameter of SQL statement
 * 针对SQL语句的Parameter做的PreparedStatement的对应封装
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/9 22:25.
 */
public class NamedPreparedStatement {
    final PreparedStatement ps;
    final List<String> names;

    NamedPreparedStatement(PreparedStatement ps, List<String> names) {
        this.ps = ps;
        this.names = names;
    }
}
