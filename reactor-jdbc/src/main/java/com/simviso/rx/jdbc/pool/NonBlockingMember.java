package com.simviso.rx.jdbc.pool;

import com.simviso.rx.jdbc.exception.DisposerHoldException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The setting of the object in the pool, through a Boolean atom class
 * to manage the concurrent use state
 * 池中对象的设定，通过一个布尔原子类来管理并发下的使用状态
 *
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time 2018/8/25 11:01.
 */
public class NonBlockingMember<T> implements Member<T> {
    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;
    private final AtomicReference<State> state = new AtomicReference<>(new State(NOT_INITIALIZED_NOT_IN_USE));


    private volatile T value;


    private final ReplayProcessor<Member<T>> processor;

    private final Scheduler.Worker worker;
    private final NonBlockingPool<T> pool;

    public NonBlockingMember(NonBlockingPool<T> pool) {
        this.pool = pool;
        this.worker = Schedulers.parallel().createWorker();
        this.processor = ReplayProcessor.create();
    }




    public Mono<Member<T>> checkout() {
        return Mono.defer(() -> {

                State s = state.get();
                if (s.value == NOT_INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            value = pool.factory.call();
                        } catch (Throwable e) {
                            return dispose();
                        }
                        return Mono.just(NonBlockingMember.this);
                    }
                }
                if (s.value == INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            if (pool.healthy.test(value)) {
                                return Mono.just(NonBlockingMember.this);
                            } else {
                                return dispose();
                            }
                        } catch (Throwable e) {
                            return dispose();
                        }
                    }
                }
                if (s.value == INITIALIZED_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        return Mono.empty();
                    }
                }
            return Mono.empty();

        }).retryWhen(Retry.any()
                          .retryMax(1000).fixedBackoff(Duration.ofMillis(pool.retryDelayMs)));
    }

    private Mono<? extends Member<T>> dispose() {
        try {
            pool.disposer.accept(value);
        } catch (Throwable t) {
            return Mono.error(new DisposerHoldException(t));
        }
        value = null;
        state.set(new State(NOT_INITIALIZED_NOT_IN_USE));

        //worker.schedule(() -> processor.onNext(NonBlockingMember.this), retryDelayMs, TimeUnit.MILLISECONDS);
        return Mono.empty();
    }

    public void checkin() {
        state.set(new State(INITIALIZED_NOT_IN_USE));
        processor.onNext(this);
    }

    public T value() {
        return value;
    }

    private static final class State {
        final int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    @Override
    public String toString() {
        return "NonBlockingMember [value=" +
                value +
                ", state=" +
                state.get().getValue() +
                "]";
    }
}
