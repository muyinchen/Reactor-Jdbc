package com.simviso.rx.jdbc.exception;

import java.sql.SQLException;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/24 18:48.
 */
public class SQLRuntimeException extends RuntimeException {
    static final long serialVersionUID = -7034897190745766955L;

    public SQLRuntimeException(SQLException e) {
        super(e);
    }
}
