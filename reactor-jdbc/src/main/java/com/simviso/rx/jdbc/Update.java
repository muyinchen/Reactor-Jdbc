package com.simviso.rx.jdbc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.concurrent.Queues;

import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time 2018/8/24 21:29.
 */
public enum Update {
    ;


    public static Flux<Integer> create(Flux<Connection> connections, Flux<List<Object>> parameterGroups, String sql) {

        return create(connections.blockFirst(),parameterGroups,sql);
    }

    public static Flux<Integer> create(Connection connection, Flux<List<Object>> parameterGroups, String sql) {
        Callable<NamedPreparedStatement> resourceFactory = () -> JdbcUtil.prepare(connection,sql);
        Function<NamedPreparedStatement, Flux<Integer>> fluxFactory = ps -> parameterGroups
                .flatMap(parameters -> create(ps, parameters).flux());
        Consumer<NamedPreparedStatement> disposer = ps -> JdbcUtil.closePreparedStatementAndConnection(ps.ps);

        return Flux.using(
                resourceFactory,
                fluxFactory,
                disposer);
    }
    private static Mono<Integer> create(NamedPreparedStatement ps, List<Object> parameters) {

        return Mono.fromCallable(() ->{
            JdbcUtil.setParameters(ps.ps, parameters,ps.names);
            return ps.ps.executeUpdate();
        });
    }


    public static <T> Flux<T> createReturnGeneratedKeys(Flux<Connection> connections, Flux<List<Object>> parameterGroups, String sql,
                                     Function<? super ResultSet, T> mapper) {
        Connection con = connections.blockFirst();
        Callable<NamedPreparedStatement> resourceFactory =
                () -> JdbcUtil.prepareReturnGeneratedKeys(con, sql);
        Function<NamedPreparedStatement, Flux<T>> fluxFactory = ps -> parameterGroups.flatMapDelayError(parameters ->create(ps, parameters, mapper),1,Queues.XS_BUFFER_SIZE);
        Consumer<NamedPreparedStatement> disposer = ps ->  JdbcUtil.closePreparedStatementAndConnection(ps.ps);
        return Flux.using(resourceFactory, fluxFactory, disposer);
    }

    private static <T> Flux<T> create(NamedPreparedStatement ps, List<Object> parameters, Function<? super ResultSet, T> mapper) {
        Callable<ResultSet> initialState = () -> {
            JdbcUtil.setParameters(ps.ps, parameters,ps.names);
            ps.ps.execute();
            return ps.ps.getGeneratedKeys();
        };
        BiFunction<ResultSet, SynchronousSink<T>, ResultSet> generator = (rs, sink) -> {
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
        Consumer<ResultSet> disposer =  rs -> {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        return Flux.generate(initialState, generator, disposer);
    }
}
