package com.simviso.rx.jdbc.pool;

import reactor.core.publisher.Mono;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/20 13:45.
 */
public interface Member<T> {
    Mono<? extends Member<T>> checkout();

    void checkin();

    T value();
}
