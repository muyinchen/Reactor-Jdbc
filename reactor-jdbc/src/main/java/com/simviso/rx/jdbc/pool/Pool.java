package com.simviso.rx.jdbc.pool;

import reactor.core.publisher.Flux;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/20 13:55.
 */
public interface Pool<T> {
    Flux<Member<T>> members();

    void shutdown();
}
