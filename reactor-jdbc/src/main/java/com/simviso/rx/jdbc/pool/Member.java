package com.simviso.rx.jdbc.pool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *The setting of the object in the pool, through a Boolean atom class
 * to manage the concurrent use state
 *池中对象的设定，通过一个布尔原子类来管理并发下的使用状态
 *
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time  2018/8/25 11:01.
 */
public class Member<T> {
     final T value;
    private final AtomicBoolean inUse = new AtomicBoolean(false);

    public Member(T value) {
        this.value = value;
    }

    public boolean checkout() {
        return inUse.compareAndSet(false, true);
    }

    public void checkin() {
        inUse.set(false);
    }

}
