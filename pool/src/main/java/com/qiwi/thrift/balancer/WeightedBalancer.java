package com.qiwi.thrift.balancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeightedBalancer<I> implements Balancer<I> {
    private volatile Balancer<I> implementation;
    private final List<I> items;
    private final ToDoubleFunction<I> weightGetter;
    private final DoubleSupplier randomSupplier;

    public static <T> void normalize(List<? extends Weight<T>> items){
        double sumWeight = items.stream()
                .filter(item -> !Double.isNaN(item.getWeight()))
                .mapToDouble(Weight::getWeight)
                .map(weight -> Math.max(0.0D, weight))
                .sum();
        if (sumWeight < Double.MIN_NORMAL * 1E+9) {
            items.forEach(item -> item.setWeight(1.0 / items.size()));
        } else if (Double.isInfinite(sumWeight)) {
            long infiniteCount = items.stream()
                    .filter(item -> item.getWeight() == Double.POSITIVE_INFINITY)
                    .count();
            for (Weight<T> item : items) {
                if (item.getWeight() == Double.POSITIVE_INFINITY) {
                    item.setWeight(1.0D / infiniteCount);
                } else {
                    item.setWeight(0);
                }
            }
        } else {
            for (Weight<T> item : items) {
                if (Double.isNaN(item.getWeight())) {
                    item.setWeight(0);
                } else {
                    item.setWeight(Math.max(0.0D, item.getWeight()) / sumWeight);
                }
            }
        }
    }

    public WeightedBalancer(List<I> items, ToDoubleFunction<I> weightGetter) {
        this(items, weightGetter, () -> ThreadLocalRandom.current().nextDouble());
    }

    public WeightedBalancer(List<I> items, ToDoubleFunction<I> weightGetter, DoubleSupplier randomSupplier) {
        this.items = new ArrayList<>(items);
        this.weightGetter = weightGetter;
        this.randomSupplier = randomSupplier;
        reBalance();
    }

    @Override
    public final void reBalance() {
        if (items.isEmpty()) {
            implementation = Balancer.empty();
            return;
        }

        List<Weight<I>> weights = items.stream()
                .map(item -> new Weight<>(item, weightGetter.applyAsDouble(item)))
                .collect(Collectors.toCollection(() -> new ArrayList<>(items.size())));
        normalize(weights);

        double minWeight = 0.001 / items.size();
        List<Weight<I>> filtered = weights.stream()
                .filter(weight -> weight.getWeight() > minWeight)
                .collect(Collectors.toCollection(() -> new ArrayList<>(items.size())));
        if (filtered.isEmpty()) {
            weights.forEach(weight -> weight.setWeight(1.0D / items.size()));
            filtered = weights;
        }
        if (filtered.size() == 1) {
            I item = filtered.get(0).item;
            implementation = new SingleNodeBalancer<I>(item);
        } else {
            implementation = new RandomizeBalancer<>(filtered, randomSupplier);
        }
    }

    private static class RandomizeBalancer<I> implements Balancer<I> {
        // Здесь не может быть пустого значения. Просто небольшая оптимизация чтобы не аллоцировать
        // оптионал на каждый запрос.
        private final Optional<I>[] nodes;
        private final double[] weights;
        private final DoubleSupplier randomSupplier;

        public RandomizeBalancer(List<Weight<I>> nodes, DoubleSupplier randomSupplier) {
            this.randomSupplier = randomSupplier;
            double[] newWights = new double[nodes.size()];
            Optional<I>[] newNodes = new Optional[nodes.size()];
            double currentWeight = 0;
            for (int i = 0; i < nodes.size(); i++) {
                Weight<I> weight = nodes.get(i);
                currentWeight += weight.weight;
                newWights[i] = currentWeight;
                newNodes[i] = Optional.of(weight.item);
            }
            // Из-за погрешности на дробях последний элемент может быть меньше единицы,
            // тогда двоичный поиск вернет индекс за границей массива
            newWights[newWights.length - 1] = 2;
            this.weights = newWights;
            this.nodes = newNodes;
        }

        @Override
        public Optional<I> get() {
            double position = randomSupplier.getAsDouble();
            int idx = Arrays.binarySearch(this.weights, position);
            if (idx < 0) {
                idx = -idx - 1;
            }
            return nodes[idx];
        }

        @Override
        public Stream<I> nodes() {
            return Arrays.stream(nodes).map(Optional::get);
        }

        @Override
        public void reBalance() {
            throw new UnsupportedOperationException("reBalance not supported in internal implementation");
        }
    }



    public static class Weight<T> {
        private final T item;

        private double weight;

        public Weight(T item, double weight) {
            this.item = item;
            this.weight = weight;
        }

        public Weight(T item) {
            this(item, 1);
        }

        public T getItem() {
            return item;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }
    }

    @Override
    public final Optional<I> get(){
        return implementation.get();
    }

    @Override
    public final Stream<I> nodes() {
        return items.stream();
    }
}
