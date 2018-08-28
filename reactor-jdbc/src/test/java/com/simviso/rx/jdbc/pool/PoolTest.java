package com.simviso.rx.jdbc.pool;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/8/28 16:22.
 */
public class PoolTest {

    @Test
    public void test() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = new Pool<>(count::incrementAndGet, n -> true, n -> {
        }, 3, 1000);
        pool.members().toStream().forEach(System.out::println);
    }
}