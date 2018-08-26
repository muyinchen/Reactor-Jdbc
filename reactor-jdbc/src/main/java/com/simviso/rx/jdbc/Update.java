package com.simviso.rx.jdbc;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

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
public class Update {
    public static Mono<Integer> create(Flux<Connection> connections, List<Object> parameters, String sql) {

        Connection connection = connections.blockFirst();

        return Mono.using(
                () -> JdbcUtil.setParameters(connection.prepareStatement(sql), parameters),
                ps -> {
                    try {
                        return Mono.just(ps.executeUpdate());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                },
                JdbcUtil::closeAll);
    }


    /*public static Mono<Integer> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql) {
        Callable<PreparedStatement> resourceFactory = () -> {
            Connection con = connectionFactory.call();
            return con.prepareStatement(sql);
        };
        Function<PreparedStatement, Mono<Integer>> singleFactory = ps -> {
            try {
                return Mono.just(ps.executeUpdate());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        Consumer<PreparedStatement> disposer = JdbcUtil::closeAll;
        return Mono.using(resourceFactory, singleFactory, disposer);
    }*/

    public static <T> Flux<T> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql,
                                     Function<? super ResultSet, T> mapper) {
        Callable<PreparedStatement> resourceFactory = () -> {
            Connection con = connectionFactory.call();
            return JdbcUtil.setParameters(con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS),parameters);
        };
        Function<PreparedStatement, Flux<T>> singleFactory = ps -> create(ps, mapper);
        Consumer<PreparedStatement> disposer = JdbcUtil::closeAll;
        return Flux.using(resourceFactory, singleFactory, disposer);
    }

    private static <T> Flux<T> create(PreparedStatement ps, Function<? super ResultSet, T> mapper) {
        Callable<ResultSet> initialState = () -> {
            ps.execute();
            return ps.getGeneratedKeys();
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
        Consumer<ResultSet> disposer = JdbcUtil::closeSilently;
        return Flux.generate(initialState, generator, disposer);
    }
}
