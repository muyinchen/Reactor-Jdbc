package com.simviso.rx.jdbc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time 2018/8/24 22:01.
 */
public class Select {
   /* public static <T> Flux<T> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql,
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
                throw new RuntimeException(e);
            }

            return rs;
        };

        Consumer<ResultSet> disposeState = JdbcUtil::closeSilently;
        return Flux.generate(initialState,generator,disposeState);
    }*/

    public static <T> Flux<T> create(Flux<Connection> connections, List<Object> parameters, String sql,
                                         Function<? super ResultSet, T> mapper) {
        return (Flux<T>) create(connections.blockFirst(),sql,parameters,mapper);
    }


    private static <T> Flux<? extends T> create(Connection con, String sql, List<Object> parameters,
                                                    Function<? super ResultSet, T> mapper) {
        Callable<ResultSet> initialState = () -> JdbcUtil.setParameters(con.prepareStatement(sql), parameters)
                                                         .getResultSet();
        BiFunction<ResultSet, SynchronousSink<T>,ResultSet> generator = (rs, sink) ->
        {
            try {
                if (rs.next()) {
                    sink.next(mapper.apply(rs));
                } else {
                    sink.complete();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return rs;
        };
        Consumer<ResultSet> disposeState = JdbcUtil::closeSilently;
        return Flux.generate(initialState, generator, disposeState);
    }


}
