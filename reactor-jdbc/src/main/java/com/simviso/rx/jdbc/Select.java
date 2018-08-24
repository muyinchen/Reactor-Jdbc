package com.simviso.rx.jdbc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Author  知秋
 * @email fei6751803@163.com
 * @time Created by Auser on 2018/8/24 22:01.
 */
public class Select {
    public static <T> Flux<T> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql,
                                     Function<? super ResultSet, T> mapper){
        Callable<ResultSet> initialState = () -> {
            Connection con = connectionFactory.call();
            PreparedStatement ps = con.prepareStatement(sql);
            return ps.executeQuery();
        };
        BiFunction<ResultSet, SynchronousSink<T>,ResultSet> generator = (rs, sink) ->
        {
            try {
                if (rs.next()) {
                    sink.next(mapper.apply(rs));
                } else {
                    sink.complete();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return rs;
        };

        Consumer<ResultSet> disposeState = JdbcUitil::closeSilently;
        return Flux.generate(initialState,generator,disposeState);
    }
}
