package com.simviso.rx.jdbc.pool;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/20 13:58.
 */
public interface MemberFactory<T, P extends Pool<T>> {
    Member<T> create(P pool);
}
