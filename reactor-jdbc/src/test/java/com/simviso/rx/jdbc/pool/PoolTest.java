package com.simviso.rx.jdbc.pool;

import com.simviso.rx.jdbc.Database;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: ZhiQiu
 * @email: fei6751803@163.com
 * @date: 2018/8/28 16:22.
 */
public class PoolTest {

    @Test
    public void test() {
        AtomicInteger count = new AtomicInteger();
        MemberFactory<Integer, NonBlockingPool<Integer>> memberFactory = NonBlockingMember::new;
        Pool<Integer> pool = new NonBlockingPool<>(count::incrementAndGet, n -> true,
                n -> {
                }, 3, 1000, memberFactory,Schedulers.parallel());
        pool.members().toStream().forEach(System.out::println);
    }

    @Test
    public void testCreate() {
        Database db = DatabaseCreator.create();
    }

}