package com.simviso.rx.jdbc.pool;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.util.concurrent.Queues;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Through a factory to obtain objects, packaged into pool objects for management,
 * the so-called pool, is a small container, here using Flux back pressure support,
 * and through the Cold data source to achieve the object can be repeatedly issued,
 * here by violating the data The immutability of the element sent in the source,
 * that is, the state of the object is modified to determine whether the pool object is available.
 * <p>
 * 通过一个工厂来获取对象，包装成池对象进行管理，所谓的池，就是一个小容器，这里使用Flux的背压支持，
 * 并通过Cold数据源来做到对象的可以重复下发，这里通过违背数据源中所下发元素的不可变性，
 * 即修改对象的状态来判断该池对象是否可用。
 *
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time 2018/8/25 11:31.
 */
public class NonBlockingPool<T> implements Pool<T>{
    private final Flux<Member<T>> members;


    final ReplayProcessor<Member<T>> processor;
    final Callable<T> factory;
    final Predicate<T> healthy;
    final Consumer<T> disposer;
    final int maxSize;
    final long retryDelayMs;
    final MemberFactory<T, NonBlockingPool<T>> memberFactory;

    public NonBlockingPool(Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer, int maxSize, long retryDelayMs,
                           MemberFactory<T, NonBlockingPool<T>> memberFactory) {
        this.factory = factory;
        this.healthy = healthy;
        this.disposer = disposer;
        this.maxSize = maxSize;
        this.retryDelayMs = retryDelayMs;
        this.memberFactory = memberFactory;

        this.processor = ReplayProcessor.create();

        Flux.range(1, maxSize)
            .map(n -> memberFactory.create(NonBlockingPool.this))
            .subscribe(processor);
        //When the element is issued, the status in this element is set to used.
        //元素下发的时候将此元素内的状态设定为已使用
        this.members = processor.doOnNext(m -> System.out.println("To be checked: " + m))
                                .flatMapDelayError(Member::checkout,1,Queues.XS_BUFFER_SIZE)
                                .doOnNext(m -> System.out.println("checked out: " + m));
    }

    public Flux<Member<T>> members() {
        return members;
    }

    @Override
    public void shutdown() {
        // TODO
    }


}
