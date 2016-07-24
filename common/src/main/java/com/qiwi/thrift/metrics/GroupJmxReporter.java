package com.qiwi.thrift.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;

class GroupJmxReporter implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(GroupJmxReporter.class);
    
    private final MetricRegistry registry;
    private final JmxListener listener;

    private GroupJmxReporter(MBeanServer mBeanServer,
                        String domain,
                        MetricRegistry registry,
                        MetricFilter filter,
                        MetricTimeUnits timeUnits) {
        this.registry = registry;
        this.listener = new JmxListener(mBeanServer, domain, filter, timeUnits);
    }


    /**
     * Starts the reporter.
     */
    public void start() {
        registry.addListener(listener);
    }

    /**
     * Stops the reporter.
     */
    public void stop() {
        registry.removeListener(listener);
        listener.unregisterAll();
    }

    /**
     * Stops the reporter.
     */
    @Override
    public void close() {
        stop();
    }

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter = MetricFilter.ALL;
        private String domain;
        private Map<String, TimeUnit> specificDurationUnits;
        private Map<String, TimeUnit> specificRateUnits;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.domain = "metrics";
            this.specificDurationUnits = Collections.emptyMap();
            this.specificRateUnits = Collections.emptyMap();
        }

        /**
         * Register MBeans with the given {@link MBeanServer}.
         *
         * @param mBeanServer     an {@link MBeanServer}
         * @return {@code this}
         */
        public Builder registerWith(MBeanServer mBeanServer) {
            this.mBeanServer = mBeanServer;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder inDomain(String domain) {
            this.domain = domain;
            return this;
        }

        /**
         * Use specific {@link TimeUnit}s for the duration of the metrics with these names.
         *
         * @param specificDurationUnits a map of metric names and specific {@link TimeUnit}s
         * @return {@code this}
         */
        public Builder specificDurationUnits(Map<String, TimeUnit> specificDurationUnits) {
            this.specificDurationUnits = Collections.unmodifiableMap(specificDurationUnits);
            return this;
        }


        /**
         * Use specific {@link TimeUnit}s for the rate of the metrics with these names.
         *
         * @param specificRateUnits a map of metric names and specific {@link TimeUnit}s
         * @return {@code this}
         */
        public Builder specificRateUnits(Map<String, TimeUnit> specificRateUnits) {
            this.specificRateUnits = Collections.unmodifiableMap(specificRateUnits);
            return this;
        }

        public GroupJmxReporter build() {
            final MetricTimeUnits timeUnits = new MetricTimeUnits(rateUnit, durationUnit, specificRateUnits, specificDurationUnits);
            return new GroupJmxReporter(mBeanServer, domain, registry, filter, timeUnits);
        }
    }

    private static class MetricTimeUnits {
        private final TimeUnit defaultRate;
        private final TimeUnit defaultDuration;
        private final Map<String, TimeUnit> rateOverrides;
        private final Map<String, TimeUnit> durationOverrides;

        MetricTimeUnits(TimeUnit defaultRate,
                        TimeUnit defaultDuration,
                        Map<String, TimeUnit> rateOverrides,
                        Map<String, TimeUnit> durationOverrides) {
            this.defaultRate = defaultRate;
            this.defaultDuration = defaultDuration;
            this.rateOverrides = rateOverrides;
            this.durationOverrides = durationOverrides;
        }

        public TimeUnit durationFor(String name) {
            return durationOverrides.containsKey(name) ? durationOverrides.get(name) : defaultDuration;
        }

        public TimeUnit rateFor(String name) {
            return rateOverrides.containsKey(name) ? rateOverrides.get(name) : defaultRate;
        }
    }
    
    private static class JmxListener implements MetricRegistryListener {
        private static final List<String> GROUP_NAMES;
        private static final Predicate<String> needQuoteRegexp = Pattern.compile("[\\\\:,\"\\*\\?\\n]").asPredicate();

        public static final int MAX_GROUPS = 16;

        static {
            ArrayList<String> groupNames = new ArrayList<>(16);
            NumberFormat format = new DecimalFormat("00");
            groupNames.addAll(Arrays.asList("type", "server", "address", "class", "method", "name"));
            for (int i = groupNames.size(); i < MAX_GROUPS; i++) {
                groupNames.add("group" + format.format(i));
            }
            GROUP_NAMES = Collections.unmodifiableList(groupNames);
        }

        private final String domain;
        private final MBeanServer mBeanServer;
        private final MetricFilter filter;
        private final MetricTimeUnits timeUnits;
        private final Set<ObjectName> registered = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

        private JmxListener(MBeanServer mBeanServer, String domain, MetricFilter filter, MetricTimeUnits timeUnits) {
            this.mBeanServer = mBeanServer;
            this.domain = domain;
            this.filter = filter;
            this.timeUnits = timeUnits;
         }

        @Override
        public void onGaugeAdded(String name, Gauge<?> gauge) {
            try {
                if (filter.matches(name, gauge)) {
                    final ObjectName objectName = createName("gauges", name);
                    mBeanServer.registerMBean(new JmxGauge(gauge, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                log.debug("Unable to register gauge", e);
            } catch (JMException e) {
                log.warn("Unable to register gauge", e);
            }
        }

        @Override
        public void onGaugeRemoved(String name) {
            try {
                final ObjectName objectName = createName("gauges", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                log.debug("Unable to unregister gauge", e);
            } catch (MBeanRegistrationException e) {
                log.warn("Unable to unregister gauge", e);
            }
        }

        @Override
        public void onCounterAdded(String name, Counter counter) {
            try {
                if (filter.matches(name, counter)) {
                    final ObjectName objectName = createName("counters", name);
                    mBeanServer.registerMBean(new JmxCounter(counter, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                log.debug("Unable to register counter", e);
            } catch (JMException e) {
                log.warn("Unable to register counter", e);
            }
        }

        @Override
        public void onCounterRemoved(String name) {
            try {
                final ObjectName objectName = createName("counters", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                log.debug("Unable to unregister counter", e);
            } catch (MBeanRegistrationException e) {
                log.warn("Unable to unregister counter", e);
            }
        }

        @Override
        public void onHistogramAdded(String name, Histogram histogram) {
            try {
                if (filter.matches(name, histogram)) {
                    final ObjectName objectName = createName("histograms", name);
                    mBeanServer.registerMBean(new JmxHistogram(histogram, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                log.debug("Unable to register histogram", e);
            } catch (JMException e) {
                log.warn("Unable to register histogram", e);
            }
        }

        @Override
        public void onHistogramRemoved(String name) {
            try {
                final ObjectName objectName = createName("histograms", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                log.debug("Unable to unregister histogram", e);
            } catch (MBeanRegistrationException e) {
                log.warn("Unable to unregister histogram", e);
            }
        }

        @Override
        public void onMeterAdded(String name, Meter meter) {
            try {
                if (filter.matches(name, meter)) {
                    final ObjectName objectName = createName("meters", name);
                    mBeanServer.registerMBean(new JmxMeter(meter, objectName, timeUnits.rateFor(name)), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                log.debug("Unable to register meter", e);
            } catch (JMException e) {
                log.warn("Unable to register meter", e);
            }
        }

        @Override
        public void onMeterRemoved(String name) {
            try {
                final ObjectName objectName = createName("meters", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                log.debug("Unable to unregister meter", e);
            } catch (MBeanRegistrationException e) {
                log.warn("Unable to unregister meter", e);
            }
        }

        @Override
        public void onTimerAdded(String name, Timer timer) {
            try {
                if (filter.matches(name, timer)) {
                    final ObjectName objectName = createName("timers", name);
                    mBeanServer.registerMBean(new JmxTimer(timer, objectName, timeUnits.rateFor(name), timeUnits.durationFor(name)), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                log.debug("Unable to register timer", e);
            } catch (JMException e) {
                log.warn("Unable to register timer", e);
            }
        }

        @Override
        public void onTimerRemoved(String name) {
            try {
                final ObjectName objectName = createName("timers", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                log.debug("Unable to unregister timer", e);
            } catch (MBeanRegistrationException e) {
                log.warn("Unable to unregister timer", e);
            }
        }


        private ObjectName createName(String type, String name) {
            try {
                String[] split = name.split("\\.", MAX_GROUPS);
                StringBuilder builder = new StringBuilder(domain.length() + name.length() + split.length * 10);
                builder.append(this.domain);
                char separator = ':';
                for (int i = 0; i < split.length; i++) {
                    String group = split[i];

                    builder.append(separator)
                            .append(GROUP_NAMES.get(i))
                            .append('=')
                            .append(needQuoteRegexp.test(group)? ObjectName.quote(group): group);
                    separator = ',';
                }
                return new ObjectName(builder.toString());
            } catch (MalformedObjectNameException e) {
                log.warn("Unable to register {} {}", type, name, e);
                throw new RuntimeException(e);
             }
        }

        void unregisterAll() {
            for (ObjectName name : registered) {
                try {
                    mBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException e) {
                    log.debug("Unable to unregister metric", e);
                } catch (MBeanRegistrationException e) {
                    log.warn("Unable to unregister metric", e);
                }
            }
            registered.clear();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface MetricMBean {
        ObjectName objectName();
    }

    private abstract static class AbstractBean implements MetricMBean {
        private final ObjectName objectName;

        AbstractBean(ObjectName objectName) {
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName() {
            return objectName;
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface JmxGaugeMBean extends MetricMBean {
        Object getValue();
    }

    private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
        private final Gauge<?> metric;

        private JmxGauge(Gauge<?> metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public Object getValue() {
            return metric.getValue();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface JmxCounterMBean extends MetricMBean {
        long getCount();
    }

    private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
        private final Counter metric;

        private JmxCounter(Counter metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface JmxHistogramMBean extends MetricMBean {
        long getCount();

        long getMin();

        long getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();
    }

    private static class JmxHistogram implements JmxHistogramMBean {
        private final ObjectName objectName;
        private final Histogram metric;

        private JmxHistogram(Histogram metric, ObjectName objectName) {
            this.metric = metric;
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName() {
            return objectName;
        }

        @Override
        public double get50thPercentile() {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }

        @Override
        public long getMin() {
            return metric.getSnapshot().getMin();
        }

        @Override
        public long getMax() {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean() {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev() {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile() {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile() {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile() {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile() {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile() {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values() {
            return metric.getSnapshot().getValues();
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface JmxMeterMBean extends MetricMBean {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }

    private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
        private final Metered metric;
        private final double rateFactor;
        private final String rateUnit;

        private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
            super(objectName);
            this.metric = metric;
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = "events/" + calculateRateUnit(rateUnit);
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }

        @Override
        public double getMeanRate() {
            return metric.getMeanRate() * rateFactor;
        }

        @Override
        public double getOneMinuteRate() {
            return metric.getOneMinuteRate() * rateFactor;
        }

        @Override
        public double getFiveMinuteRate() {
            return metric.getFiveMinuteRate() * rateFactor;
        }

        @Override
        public double getFifteenMinuteRate() {
            return metric.getFifteenMinuteRate() * rateFactor;
        }

        @Override
        public String getRateUnit() {
            return rateUnit;
        }

        private String calculateRateUnit(TimeUnit unit) {
            final String s = unit.toString().toLowerCase(Locale.US);
            return s.substring(0, s.length() - 1);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public interface JmxTimerMBean extends JmxMeterMBean {
        double getMin();

        double getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        String getDurationUnit();
    }

    static class JmxTimer extends JmxMeter implements JmxTimerMBean {
        private final Timer metric;
        private final double durationFactor;
        private final String durationUnit;

        private JmxTimer(Timer metric,
                         ObjectName objectName,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit) {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }

        @Override
        public double get50thPercentile() {
            return metric.getSnapshot().getMedian() * durationFactor;
        }

        @Override
        public double getMin() {
            return metric.getSnapshot().getMin() * durationFactor;
        }

        @Override
        public double getMax() {
            return metric.getSnapshot().getMax() * durationFactor;
        }

        @Override
        public double getMean() {
            return metric.getSnapshot().getMean() * durationFactor;
        }

        @Override
        public double getStdDev() {
            return metric.getSnapshot().getStdDev() * durationFactor;
        }

        @Override
        public double get75thPercentile() {
            return metric.getSnapshot().get75thPercentile() * durationFactor;
        }

        @Override
        public double get95thPercentile() {
            return metric.getSnapshot().get95thPercentile() * durationFactor;
        }

        @Override
        public double get98thPercentile() {
            return metric.getSnapshot().get98thPercentile() * durationFactor;
        }

        @Override
        public double get99thPercentile() {
            return metric.getSnapshot().get99thPercentile() * durationFactor;
        }

        @Override
        public double get999thPercentile() {
            return metric.getSnapshot().get999thPercentile() * durationFactor;
        }

        @Override
        public long[] values() {
            return metric.getSnapshot().getValues();
        }

        @Override
        public String getDurationUnit() {
            return durationUnit;
        }
    }

}
