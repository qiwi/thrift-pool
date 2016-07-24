package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.balancer.Balancer;
import com.qiwi.thrift.balancer.WeightedBalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadBasedBalancer<I> implements Balancer<I> {
    private final List<LoadAccumulator<I>> itemsAcc;
    private final ToDoubleFunction<I> loadGetter;

    private final double loadFactor;
    private final double predictFactor;
    private final double weightFactor;

    private final WeightedBalancer<I> balancer;

    public LoadBasedBalancer(
            List<I> items,
            Function<I, LoadAccumulator<I>> accumulatorGetter,
            ToDoubleFunction<I> loadGetter,
            double loadFilterFactor,
            double predictFactor,
            double weightFilterFactor,
            DoubleSupplier randomSupplier
    ) {
        this.itemsAcc = items.stream().map(accumulatorGetter).collect(Collectors.toList());
        this.loadFactor = loadFilterFactor;
        this.predictFactor = predictFactor;
        this.weightFactor = weightFilterFactor;
        this.loadGetter = loadGetter;
        refreshLoad();
        this.balancer = new WeightedBalancer<I>(items, item -> accumulatorGetter.apply(item).getWeight(), randomSupplier);
    }

    public LoadBasedBalancer(
            List<I> items,
            Function<I, LoadAccumulator<I>> accumulatorGetter,
            ToDoubleFunction<I> loadGetter,
            double loadFilterFactor,
            double predictFactor,
            double weightFilterFactor
    ) {
        this(items, accumulatorGetter, loadGetter, loadFilterFactor, predictFactor, weightFilterFactor, () -> ThreadLocalRandom.current().nextDouble());
    }

    @Override
    public Optional<I> get() {
        return balancer.get();
    }

    @Override
    public Stream<I> nodes() {
        return balancer.nodes();
    }

    private double getWeight(double loadOld, double loadNew, double weightOld){
        double predictedLoadValue = (loadNew - loadOld * predictFactor) / (1 - predictFactor);
        return Math.max(weightOld, 0.0001) / Math.max(predictedLoadValue, 0.01);
    }

    @Override
    public void reBalance() {
        refreshLoad();
        balancer.reBalance();
    }

    private void refreshLoad(){
        List<WeightedBalancer.Weight<LoadAccumulator<I>>> weights = new ArrayList<>();
        itemsAcc.forEach(accumulator -> weights.add(new WeightedBalancer.Weight<>(accumulator, accumulator.getWeight())));
        WeightedBalancer.normalize(weights);
        for (WeightedBalancer.Weight<LoadAccumulator<I>> weight : weights) {
            LoadAccumulator<I> accumulator = weight.getItem();
            double loadOld = accumulator.getLoad();
            double loadNew = filterLoad(loadOld, loadGetter.applyAsDouble(accumulator.getItem()));
            accumulator.setLoad(loadNew);
            double weightVal = getWeight(loadOld, loadNew, weight.getWeight());
            if (accumulator.isPreferred()) {
                // формула получена по результатам мат-моделирования. Смотри balance_model.xmcd
                // Обеспечивает плавный переход нагрузки из текущего датацентра - в остальные, если нагрузка
                // превышает 0.6 придельной нагрузки датацентра.
                weightVal /= 0.5 + Math.atan((loadOld - 0.75) * 18) / Math.PI;
            }
            accumulator.setWeight(filterWeight(accumulator.getWeight(), weightVal));
        }
    }

    private double filterLoad(double oldVal, double newVal) {
        return loadFactor * oldVal + (1.0 - loadFactor) * newVal;
    }

    private double filterWeight(double oldVal, double newVal) {
        if (!Double.isFinite(newVal)) {
            return oldVal;
        }
        if (newVal < 0.000001) {
            newVal = 0.000001;
        }
        // use log scale to make value fail quick to react on failing node
        // but grow slow because many nodes can rebalance load to new - non-load node to fast.
        return Math.exp(weightFactor * Math.log(oldVal) + (1.0 - weightFactor) * Math.log(newVal));
    }

    public static class LoadAccumulator<I> {
        private final I item;
        private boolean preferred;
        private double load = 0;
        private double weight = 0.01;

        public LoadAccumulator(I item) {
            this(item, false);
        }

        public LoadAccumulator(I item, boolean preferred) {
            this.item = item;
            this.preferred = preferred;
        }

        public boolean isPreferred() {
            return preferred;
        }

        public void setPreferred(boolean preferred) {
            this.preferred = preferred;
        }

        public double getLoad() {
            return load;
        }

        public void setLoad(double load) {
            this.load = load;
        }

        public I getItem() {
            return item;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        public void reset() {
            this.weight = 0.01;
            this.load = 0.0;
        }
    }
}
