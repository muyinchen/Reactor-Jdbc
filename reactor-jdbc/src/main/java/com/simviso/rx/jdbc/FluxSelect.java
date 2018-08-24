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
 * @time Created by Auser on 2018/8/24 16:18.
 */
public class FluxSelect {

    public static <T> Flux<T> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql,
                                     Function<? super ResultSet, T> mapper){
        Callable<Query> initialState = () -> {
            Connection con = connectionFactory.call();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            return new Query(ps, rs);
        };
        BiFunction<Query, SynchronousSink<T>,Query> generator = (query, sink) ->
        {
            sink.next(mapper.apply(query.rs));
            return query;
        };

        Consumer<Query> disposeState = Query::close;
        return Flux.generate(initialState,generator,disposeState);
    }

    private static class Query {
    final PreparedStatement ps;
    final ResultSet rs;

    Query(PreparedStatement ps, ResultSet rs) {
        this.ps = ps;
        this.rs = rs;
    }

    void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            //ignore
        }
        Connection con = null;
        try {
            con  = ps.getConnection();
        } catch (SQLException e1) {
            // ignore
        }
        try {
            ps.close();
        } catch (SQLException e) {
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

