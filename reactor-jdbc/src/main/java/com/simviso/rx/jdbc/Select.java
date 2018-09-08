package com.simviso.rx.jdbc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.util.concurrent.Queues;

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
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time 2018/8/24 22:01.
 */
public enum Select {
    ;
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



    public static <T> Flux<T> create(Flux<Connection> connections,  Flux<List<Object>> parameters, String sql,
                                         Function<? super ResultSet, T> mapper) {
        return create(connections.blockFirst(),sql,parameters,mapper);
    }


    private static <T> Flux<T> create(Connection con, String sql, Flux<List<Object>> parameterGroups,
                                          Function<? super ResultSet, T> mapper) {
        Callable<PreparedStatement> initialState = () -> con.prepareStatement(sql);
        Function<PreparedStatement, Flux<T>> observableFactory = ps -> parameterGroups
                .flatMapDelayError(parameters -> create(con, ps, parameters, mapper), 1,Queues.XS_BUFFER_SIZE);
        Consumer<PreparedStatement> disposer = ps -> {
            try {
                ps.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        return Flux.using(initialState, observableFactory, disposer, true);
    }

    private static <T> Flux<? extends T> create(Connection con, PreparedStatement ps, List<Object> parameters,
                                                    Function<? super ResultSet, T> mapper) {
        Callable<ResultSet> initialState = () -> JdbcUtil.setParameters(ps, parameters)
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
        Consumer<ResultSet> disposeState = rs -> {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        return Flux.generate(initialState, generator, disposeState);
    }


}
