package com.qiwi.thrift.consul;

import com.qiwi.thrift.server.AbstractThriftServer;
import com.qiwi.thrift.server.StatStopListener;
import com.qiwi.thrift.server.ThriftEndpoint;
import com.qiwi.thrift.utils.ThriftClientAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;


public class ThriftConsulRegistrator  {
    private static final Logger log = LoggerFactory.getLogger(ThriftConsulRegistrator.class);
    public static final Duration HEARTBEAT_PERIOD = Duration.ofSeconds(10);

    final ConsulService consulService;

    private final List<Listener> listeners;
    private ScheduledExecutorService executor = null;
    private long listenerCount = 0;

    @Inject
    public ThriftConsulRegistrator(ConsulService consulService, Collection<AbstractThriftServer> servers){
        this.consulService = consulService;
        this.listeners = new ArrayList<>(servers.size());
        try {
            for (AbstractThriftServer server : servers) {
                Listener listener = new Listener(server);
                server.addListener(listener);
                listeners.add(listener);
            }
        } catch (RuntimeException ex) {
            log.info("Server beans not found", ex);
        }
    }

    private synchronized void scheduleHeartbeat(Listener listener){
        if (listenerCount == 0) {
            executor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "ThriftConsulRegistrator"));
        }
        if (listener.scheduledTask != null) {
            log.warn("Double register of {}", listener.server);
            return;
        }
        listener.scheduledTask = executor.scheduleWithFixedDelay(
                listener::heartbeat,
                HEARTBEAT_PERIOD.toMillis(),
                HEARTBEAT_PERIOD.toMillis(),
                TimeUnit.MILLISECONDS
        );

        listenerCount++;
    }

    private synchronized void stopHeartbeat(Listener listener){
        if (listener.scheduledTask == null) {
            return;
        }
        listener.scheduledTask.cancel(false);
        listener.scheduledTask = null;
        listenerCount--;
        if (listenerCount == 0) {
            executor.shutdown();
            executor = null;
        }
    }


    private static <I> ThriftServiceDescription<I> fromEndpoint(ThriftEndpoint<I> endpoint) {
        return new ThriftServiceDescription.Builder<I>(endpoint.getInterfaceClass())
                .setClusterName(endpoint.getClusterName())
                .setSubServiceName(endpoint.getSubServiceName())
                .build();
    }

    private class Listener implements StatStopListener {
        private final AbstractThriftServer server;
        private Future<?> scheduledTask = null;

        public Listener(AbstractThriftServer server) {
            this.server = server;
        }

        @Override
        public void onStart(AbstractThriftServer server) {
            log.info("Register server {}", server.getClass());
            doForService((endpoint, address) -> consulService.register(fromEndpoint(endpoint), address, endpoint.getTags()));
            scheduleHeartbeat(this);

        }

        @Override
        public void onStop(AbstractThriftServer server) {
            log.info("Deregister server {}", server.getClass());
            stopHeartbeat(this);
            doForService((endpoint, address) -> consulService.deregister(fromEndpoint(endpoint), address));
        }

        public void heartbeat() {
            log.debug("Heartbeat for server {}", server.getClass());
            doForService((endpoint, address) -> consulService.heartbeat(fromEndpoint(endpoint), address, endpoint.getTags()));
        }

        private void doForService(BiConsumer<ThriftEndpoint<?>, ThriftClientAddress> action) {
            try {
                Collection<ThriftEndpoint<?>> serviceList = server.getServiceList();
                serviceList.forEach(endpoint -> action.accept(endpoint, server.getAddress()));
            } catch (RuntimeException e) {
                log.error("Unable connect to consul action for service", e);
            }
        }
    }

}
