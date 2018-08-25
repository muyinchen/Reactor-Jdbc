package com.simviso.rx.jdbc.pool;

import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.util.concurrent.Queues;

import java.util.concurrent.Callable;

/**
 *Obtain objects through a factory, package them into pool objects for management.
 *The so-called pool is a small container, which is managed by an array.
 *
 *通过一个工厂来获取对象，包装成池对象进行管理，所谓的池，就是一个小容器，这里使用一个数组进行管理
 *
 * @author ZhiQiu
 * @email fei6751803@163.com
 * @time  2018/8/25 11:31.
 */
public class Pool<T> {
    private final Callable<T> factory;
    private final Member<T>[] members;
    private final WorkQueueProcessor<Member<T>> processor = WorkQueueProcessor.share("jdbc-pool",Queues.SMALL_BUFFER_SIZE);


    @SuppressWarnings("unchecked")
    public Pool(Callable<T> factory, int maxSize) {
        this.factory = factory;
        this.members = (Member<T>[]) new Object[maxSize];
    }

    public Flux<T> members() {
        return processor.filter(Member::checkout).map(member -> member.value);
    }

    /**
     * Synchronization strategy used without responsive considerations
     * 没有进行响应式考虑的话，所采用的同步策略
     * @return Optional<Member<T>>
     * @throws Exception
     *//*
    public synchronized Optional<Member<T>> checkout0() throws Exception {
        for (int i = 0; i < members.length; i++) {
            if (members[i] == null) {
                members[i] = new Member<T>(factory.call());
                return Optional.of(members[i]);
            } else if (members[i].checkout()) {
                return Optional.of(members[i]);
            }
        }
        return Optional.empty();
    }*/

    /**
     * Responsive asynchronous processing
     * 响应式的异步处理
     * @return boolean
     * @throws Exception
     */
    public boolean checkout() throws Exception {
        //TODO handle asynchronous calls
        for (int i = 0; i < members.length; i++) {
            if (members[i] == null) {
                members[i] = new Member<T>(factory.call());
                processor.onNext(members[i]);
                return true;
            } else if (members[i].checkout()) {
                processor.onNext(members[i]);
                return true;
            }
        }
        return false;
    }


    public void checkin(Member<T> member) {
        member.checkin();
    }

}
