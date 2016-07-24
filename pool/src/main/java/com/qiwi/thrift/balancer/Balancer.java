package com.qiwi.thrift.balancer;

import java.util.Optional;
import java.util.stream.Stream;

public interface Balancer<I> {

    /**
     *
     * @return empty - если нет не одной ноды в балансировке (нулевой вес у всех нод, и т.п.)
     */
    Optional<I> get();

    Stream<I> nodes();

    void reBalance();

    static <I> Balancer<I> empty(){
        return new Balancer<I>() {
            @Override
            public Optional<I> get() {
                return Optional.empty();
            }

            @Override
            public Stream<I> nodes() {
                return Stream.empty();
            }

            @Override
            public void reBalance() {
            }
        };
    }


}
