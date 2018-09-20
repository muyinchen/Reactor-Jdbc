package com.simviso.rx.jdbc.pool;

import com.simviso.rx.jdbc.ConnectionNonBlockingMember;

import java.sql.Connection;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/9/20 14:00.
 */
public class ConnectionNonBlockingMemberFactory implements MemberFactory<Connection, NonBlockingPool<Connection>>{
    @Override
    public Member<Connection> create(NonBlockingPool<Connection> pool) {
        return new ConnectionNonBlockingMember(pool);
    }
}
