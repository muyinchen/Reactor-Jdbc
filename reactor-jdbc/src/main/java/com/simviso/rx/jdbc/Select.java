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


    public static <T> Flux<T> create(Flux<Connection> connections,  Flux<List<Object>> parameters, String sql,
                                         Function<? super ResultSet, T> mapper) {
        return create(connections.blockFirst(),sql,parameters,mapper);
    }


    private static <T> Flux<T> create(Connection con, String sql, Flux<List<Object>> parameterGroups,
                                          Function<? super ResultSet, T> mapper) {
        Callable<NamedPreparedStatement> initialState = () -> JdbcUtil.prepare(con,sql);
        Function<NamedPreparedStatement, Flux<T>> fluxFactory = ps -> parameterGroups
                .flatMapDelayError(parameters -> create(con, ps.ps, parameters, mapper,ps.names), 1,Queues.XS_BUFFER_SIZE);
        Consumer<NamedPreparedStatement> disposer = ps -> JdbcUtil.closePreparedStatementAndConnection(ps.ps);
        return Flux.using(initialState, fluxFactory, disposer, true);
    }

    private static <T> Flux<? extends T> create(Connection con, PreparedStatement ps, List<Object> parameters,
                                                    Function<? super ResultSet, T> mapper , List<String> names) {
        Callable<ResultSet> initialState = () -> JdbcUtil.setParameters(ps, parameters, names)
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
