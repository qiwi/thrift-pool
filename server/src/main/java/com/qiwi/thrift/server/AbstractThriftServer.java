package com.qiwi.thrift.server;

import com.qiwi.thrift.metrics.ThriftCallType;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.tracing.TCompactTracedProtocol;
import com.qiwi.thrift.tracing.ThriftLogContext;
import com.qiwi.thrift.tracing.ThriftRequestReporter;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftConnectionException;
import com.qiwi.thrift.utils.ThriftRequestStatus;
import com.qiwi.thrift.utils.ThriftRuntimeException;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Конфигурация на TThreadedSelectorServer имеет низкую латентность в случае длительных запросов, завязанных
// на внешние сервисы (база, и т.п.).
// И нормальную на больших запросах порядка мегабайт, особенно если подтормаживает сеть.
//
// Для очень больших запросов жрёт много памяти.
// Если нужно обрабатывать очень много маленьких запросов (от небольшого числа клиентов), лучше работает конфигурация
// на основе TThreadPoolServer
// Ожидаемая производительность для маленьких запросов:
// TThreadedSelectorServer 30 000 rps / latency 0.3 ms
// TThreadPoolServer 70 000 rps / latency 0.1 ms
//
// В качестве протокола используется TFramedTransport->TCompactProtocol - рекомендую использовать везде
//

/**
 * Refactoring
 *      getServerProcessors() replaced with createEndpointConfig();
 *      getConfig() replaced with createConfig();
 */
public abstract class AbstractThriftServer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(AbstractThriftServer.class);
    public static final Duration DEFAULT_START_TIMEOUT = Duration.ofSeconds(5);

    private final List<StatStopListener> listeners = new CopyOnWriteArrayList<>();

    private ThriftServerConfig serverConfig;
    private ThriftEndpointConfig endpointConfig;
    private TNonblockingServerSocket serverSocket;
    private TThreadedSelectorServer server;
    private Thread serverThread;
    private ThriftClientAddress address;

    public abstract ThriftServerConfig createConfig();

    public abstract ThriftEndpointConfig createEndpointConfig();

    public synchronized void startServer(){
        if (serverConfig != null) {
            throw new IllegalStateException("Server already running");
        }
        serverConfig = createConfig();
        endpointConfig = createEndpointConfig();
        int clusterPoolSize = serverConfig.getThriftPoolSize();
        int selectorThreads = serverConfig.getThriftSelectorThreads();

        Map<String, String> tags = new HashMap<>();
        tags.put(ThriftClientAddress.TRACE_TYPE_PARAMETER, serverConfig.getTraceMode().name());
        address = new ThriftClientAddress("localhost", serverConfig.getServerThriftPort(), tags);
        TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
        Map<String, Class<?>>  services = new HashMap<>();
        for (ThriftEndpoint<?> endpoint : endpointConfig.getEndpoints()) {
            for (String name : endpoint.getEndpointNames()) {
                Class<?> oldImplementation = services.put(name, endpoint.getImplementation().getClass());
                if (oldImplementation != null) {
                    throw new IllegalArgumentException("Two service with same name " + name + " registered on server! Implementations " + oldImplementation + " and " + endpoint.getImplementation().getClass());
                }
                TProcessor processor = createProcessor(name, address, endpoint);
                muxProcessor.registerProcessor(name, processor);
            }
        }

        try {
            serverSocket = new TNonblockingServerSocket(serverConfig.getServerThriftPort());
        } catch (TTransportException e) {
            throw new ThriftConnectionException("Unable to open port", e);
        }
        if (selectorThreads <= 0){
            selectorThreads = Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
        }
        ExecutorService serverExecutor = Executors.newFixedThreadPool(clusterPoolSize);
        TProtocolFactory factory;
        switch (serverConfig.getTraceMode()) {
            case BASIC:
            case DISABLED:
                factory = new TCompactTracedProtocol.Factory(
                        serverConfig.getMaxFrameSizeBytes(),
                        serverConfig.getMaxCollectionItemCount(),
                        serverConfig.getTraceMode()
                );
                break;
            default:
                throw new IllegalStateException("Mode " + serverConfig.getTraceMode() + " not implemented");
        }
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(serverSocket)
                .processor(muxProcessor)
                .protocolFactory(factory)
                // TThreadedSelectorServer в текущей версии поддерживает только TFramedTransport
                .transportFactory(new TFramedTransport.Factory(serverConfig.getMaxFrameSizeBytes()))
                .executorService(serverExecutor)
                .acceptQueueSizePerThread(clusterPoolSize * 4 / selectorThreads)
                .selectorThreads(selectorThreads);
        args.maxReadBufferBytes = serverConfig.getMaxFrameSizeBytes();

        server = new TThreadedSelectorServerWithIpLogging(args);

        // Thrift block server running thread, until server stopped
        serverThread = new Thread(() -> server.serve(), "Thrift server");
        serverThread.start();
        log.info("Thrift server starting on port {}", serverSocket.getPort());
        address = new ThriftClientAddress("localhost", serverSocket.getPort(), tags);// actualize server port
        for (StatStopListener listener : listeners) {
            listener.onStart(this);
        }
    }

    public void awaitStart() {
        awaitStart(DEFAULT_START_TIMEOUT);
    }

    public void awaitStart(Duration awaitTime) {
        for (int i = 0; i < awaitTime.toMillis() / 100; i++) {
            if (server.isServing()) {
                log.info("Thrift server starting successful on port {}", serverSocket.getPort());
                return;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Server starting interrupted", e);
                    throw new ThriftRuntimeException("Server starting interrupted", e);
                }
            }
        }
        log.info("Thrift server on port {} failed to start", serverSocket.getPort());
        server.stop();
        throw new ThriftRuntimeException("Unable to start server on port: " + serverSocket.getPort());
    }

    public synchronized void stopServer(){
        if (server != null) {
            for (StatStopListener listener : listeners) {
                listener.onStop(this);
            }
            server.stop();
            server = null;
        }
        if (serverThread != null) {
            try {
                serverThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            serverThread = null;
        }
        if (serverSocket != null) {
            log.info("Thrift server on port {} stopped", serverSocket.getPort());
            serverSocket.close();
            this.serverSocket = null;
        }
        address = null;
        serverConfig = null;
        endpointConfig = null;
    }

    public void addListener(StatStopListener listener){
        listeners.add(listener);
        if (server != null) {
            listener.onStart(this);
        }
    }

    public Collection<ThriftEndpoint<?>> getServiceList() {
        if (endpointConfig == null) {
            return Collections.emptyList();
        } else {
            return endpointConfig.getEndpoints();
        }
    }

    public int getPort() {
        return getAddress().getPort();
    }


    public ThriftClientAddress getAddress() {
        if (address == null) {
            throw new IllegalStateException("Service not started yet");
        }
        return address;
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        stopServer();
    }

    private <I> TProcessor createProcessor(String name, ThriftClientAddress address, ThriftEndpoint<I> endpoint){
        ServerInvocationHandler<I> handler = new ServerInvocationHandler<I>(
                endpoint.getImplementation(),
                name,
                address,
                serverConfig.getRequestReporter()
        );
        I proxy = (I)Proxy.newProxyInstance(
                endpoint.getInterfaceClass().getClassLoader(),
                new Class[] {endpoint.getInterfaceClass()},
                handler
        );
        TProcessor processor = endpoint.getEndpointProcessorFactory().apply(proxy);
        return new TLogProcessor(processor, name, address);
    }

    /**
     * этот процессор можно использовать только как вложенный в TMultiplexedProcessor
     */
    private static class TLogProcessor implements TProcessor {
        private final TProcessor nested;
        private final String serviceName;
        private final ThriftClientAddress address;

        private TLogProcessor(TProcessor nested, String serviceName, ThriftClientAddress address) {
            Objects.requireNonNull(nested, "nested");
            Objects.requireNonNull(serviceName, "processorName");
            this.nested = nested;
            this.serviceName = serviceName;
            this.address = address;
        }

        @Override
        public boolean process(TProtocol in, TProtocol out) throws TException {
            TMessage message = in.readMessageBegin();
            // Проверяем что spanId, parent span id и т.п. были инициализированы.
            ThriftLogContext.getRequestHeader();
            Thread.currentThread().setName("{\"type\":\"thrift\",\"method\":\"" + serviceName + '.' + message.name + "\"}");
            try {
                return nested.process(in, out);
            } finally {
                Thread.currentThread().setName("{\"type\":\"thrift\"}");
                ThriftLogContext.newExecution();
            }
        }
    }


    private static class ServerInvocationHandler<I> implements InvocationHandler{
        private final I delegate;
        private final String serviceName;
        private final ThriftClientAddress address;
        private final ThriftRequestReporter requestReporter;

        private ServerInvocationHandler(
                I delegate,
                String serviceName,
                ThriftClientAddress address,
                ThriftRequestReporter requestReporter
        ) {
            this.delegate = Objects.requireNonNull(delegate);
            this.serviceName = serviceName;
            this.address = address;
            this.requestReporter = requestReporter;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startNanos = System.nanoTime();
            String methodName = method.getName();
            switch (methodName) {
                case "hashCode":
                     if (args == null || args.length == 0) {
                         return delegate.hashCode();
                     }
                     break;
                case "equals":
                    if (args.length == 1) {
                        Object arg = args[0];
                        if (arg != null && Proxy.isProxyClass(arg.getClass())) {
                            InvocationHandler handler = Proxy.getInvocationHandler(arg);
                            if (handler instanceof ServerInvocationHandler) {
                                return delegate.equals(((ServerInvocationHandler) handler).delegate);
                            }
                        }
                        return false;
                    }
                    break;
            }
            try {
                requestReporter.requestBegin(serviceName, methodName, ThriftCallType.SERVER);
                if (log.isTraceEnabled()) {
                    log.trace(
                            "Method call {}.{} with arguments {} client ip {}",
                            serviceName,
                            methodName,
                            ThriftLogContext.getClientAddress(),
                            Arrays.toString(args)
                    );
                }
            } catch (Exception ex) {
                log.warn(
                        "RequestReporter.requestBegin for method {}.{} client ip {} fail with error",
                        serviceName,
                        methodName,
                        ThriftLogContext.getClientAddress(),
                        ex
                );
            }
            ThriftRequestStatus status = ThriftRequestStatus.SUCCESS;
            Throwable cause = null;
            Object response = null;
            try {
                response = method.invoke(delegate, args);
                return response;
            } catch (InvocationTargetException ex) {
                if (ex.getCause() == null) {
                    cause = ex;
                } else {
                    cause = ex.getCause();
                }
                if (cause instanceof RuntimeException) {
                    log.error(
                            "Method {}.{} fail with exception. Args {}. Client ip {}",
                            serviceName,
                            methodName,
                            args,
                            ThriftLogContext.getClientAddress(),
                            cause
                    );
                } else {
                    log.info(
                            "Method {}.{} fail with exception. Args {}. Client ip {}",
                            serviceName,
                            methodName,
                            args,
                            ThriftLogContext.getClientAddress(),
                            cause
                    );
                }
                status = ThriftRequestStatus.UNEXPECTED_ERROR;
                throw cause;
            } catch (Throwable ex) {
                cause = ex;
                log.error("Method {}.{} fail with error. Client ip {}", serviceName, methodName, ex);
                throw ex;
            } finally {
                long duration = System.nanoTime() - startNanos;
                try {
                    ThriftMonitoring.getMonitor().logMethodCall(
                            ThriftCallType.SERVER,
                            address,
                            serviceName,
                            methodName,
                            duration,
                            status
                    );
                    requestReporter.requestEnd(
                            serviceName,
                            methodName,
                            ThriftCallType.SERVER,
                            status,
                            duration,
                            Optional.ofNullable(cause)
                    );
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Implementation respond {} in {} ms to method {}.{} with args {}",
                                response,
                                duration / 1_000_000d,
                                serviceName,
                                method,
                                Arrays.toString(args),
                                cause
                        );
                    }
                } catch (Exception ex) {
                    log.warn(
                            "RequestReporter.requestEnd for method {}.{} client ip {} fail with error",
                            serviceName,
                            methodName,
                            ThriftLogContext.getClientAddress(),
                            ex
                    );
                }
            }
        }

    }
}
