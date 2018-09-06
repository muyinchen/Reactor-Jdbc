package com.simviso.rx.jdbc.pool;

import com.simviso.rx.jdbc.exception.DisposerHoldException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;
    private final AtomicInteger state = new AtomicInteger(NOT_INITIALIZED_NOT_IN_USE);

    private volatile T value;
    private final ReplayProcessor<Member<T>> processor;
    private final Callable<T> factory;
    private final long retryDelayMs;
    private final Predicate<T> healthy;
    private final Consumer<T> disposer;




    public Member(ReplayProcessor<Member<T>> replayProcessor, Callable<T> factory, long retryDelayMs, Predicate<T> healthy, Consumer<T> disposer) {

        this.processor = replayProcessor;
        this.factory = factory;
        this.retryDelayMs = retryDelayMs;
        this.healthy = healthy;
        this.disposer = disposer;
    }



    public Mono<Member<T>> checkout() {
        return Mono.defer(() -> {
            if (state.compareAndSet(NOT_INITIALIZED_NOT_IN_USE, INITIALIZED_IN_USE)) {
                try {
                    value = factory.call();
                } catch (Exception e) {
                    throw  new RuntimeException(e);
                }
                return Mono.just(Member.this);
            } else {
                try {
                    if (healthy.test(value)) {
                        if (state.compareAndSet(INITIALIZED_NOT_IN_USE, INITIALIZED_IN_USE)) {
                            return Mono.just(Member.this);
                        } else {
                            return Mono.empty();
                        }
                    } else {
                       return dispose();
                    }
                } catch (Throwable e) {
                   return dispose();
                }
            }
        }).retryWhen(e -> e.timeout(Duration.ofMillis(retryDelayMs)));
    }

    private Mono<? extends Member<T>> dispose() {
        try {
            disposer.accept(value);
            state.set(NOT_INITIALIZED_NOT_IN_USE);
            return Mono.empty();
        } catch (Throwable t) {
            return Mono.error(new DisposerHoldException(t));
        }
    }

    public void checkin() {
        state.set(INITIALIZED_NOT_IN_USE);
        processor.onNext(this);
    }

    public T value() {
        return value;
    }

    @Override
    public String toString() {
        return "Member [value=" +
                value +
                ", state=" +
                state.get() +
                "]";
    }
}
