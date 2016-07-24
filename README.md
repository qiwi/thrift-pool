
# Thrift-pool

This microservice core library contains following features:

1. Connection pooling logic for Thrift protocol.
1. Low latency client side load balancing.
1. Auto-configuration for multiplexing thrift client and server
1. Hidden MDC trace id sending, for request tracing through microservices callstack
1. Request statistic collection for graphite and jmx monitoring
1. Build-in request logging
1. Service discovery through Consul
1. Thrift based object serialization
1. Key based balancing
   1. request sharding mode
   1. request affinity mode
1. Sync and Async Thrift interfaces support
1. Support of Kotlin and Scala

Library creates and configures thrift client and thrift server to work together.
You can connect to Thrift implementation in any other languages and libraries.
* Without special configuration to any service with, CompactProtocol and FramedTransport.
* With special configuration to any other service.

Library has single mandatory dependency: org.apache.commons:commons-pool2.
All other dependencies are optional.



## Usage minimal example


This minimal fully functional code with Consul.
```java
@Bean(destroyMethod = "close")
public DemoServer.Iface demoServerClient(ThriftLoadBalancerFactory factory, Environment environment) {
    ParameterSource subpath = ParameterSource.subpath("spring.cloud.", environment::getProperty);
    return factory.createSyncClient(
            DemoServer.Iface.class,
            new ThriftBalancerConfig.Builder(CIRCUIT_BREAK)
                   .fromParameterSource(subpath)

    );
}


@Inject
private DemoServer.Iface demoServer;


void doLoadTesting() {
    for (int i = 0; i < 50; i++) {
        MapResult result = demoServer.request("test", 42);
        assertEquals(result.getStatus(), Status.OK);
    }
}
```

To setup address manually.

```java
@Bean(destroyMethod = "close")
public DemoServer.Iface demoServerClient(ThriftLoadBalancerFactory factory, Environment environment) {
    return factory.createSyncClient(
            DemoServer.Iface.class,
            new ThriftBalancerConfig.Builder(CIRCUIT_BREAK)
                    .setServers(Arrays.asList(
                            new ThriftClientAddress("test1.host.com", 9090),
                            new ThriftClientAddress("test2.host.com", 9090)
                    ))
    );
}
```

To setup configuration through Consul:
```java
@Bean(destroyMethod = "close")
public DemoServer.Iface demoServerClient(ThriftConsulFactory factory) {
    return factory.create(DemoServer.Iface.class, CIRCUIT_BREAK);
}



@Inject
private DemoServer.Iface demoServer;


void doLoadTesting() {
    for (int i = 0; i < 50; i++) {
        MapResult result = demoServer.request("test", 42);
        assertEquals(result.getStatus(), Status.OK);
    }
}
```

## Working with thrift pool without spring
```java
ReflectConfigurator reflectConfigurator = new ReflectConfigurator();
ThriftLoadBalancerFactory factory = new ThriftLoadBalancerFactory(
        new ThriftLoadBalancer.Factory(),
        new SyncClientFactory(reflectConfigurator),
        new AsyncClientFactory(reflectConfigurator),
        reflectConfigurator
);

Properties config = new Properties();
// emulating data loading from property file
config.put(
        "thrift.test.client.servers",
        "localhoost:10001,dc=s1;localhoost:10002,dc=s2"
);

ParameterSource parameterSource = ParameterSource.subpath(
        "thrift.test.client.",
        (name, defaultValue) -> config.containsKey(name)? config.getProperty(name): defaultValue
);

ThriftBalancerConfig.Builder configBuilder = new ThriftBalancerConfig.Builder(CIRCUIT_BREAK)
        .fromParameters(parameterSource);
client = poolFactory.createSyncClient(
        DemoServer.Iface.class,
        configBuilder
);
```

## Usage full configuration to work through Consul


```java
@Configuration
@ComponentScan(value = "com.qiwi")
@PropertySource("classpath:client.properties")
public class ClientConfiguration {
    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
       return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public ConsulService consulService(Environment environment) {
        ParameterSource subpath = ParameterSource.subpath("spring.cloud.", environment::getProperty);
        ThriftConsulConfig config = new ThriftConsulConfig.Builder()
                .fromParameterSource(subpath)
                .setApplicationName(appName)
                .build();
        return new ConsulService(config);
    }

    @Bean(destroyMethod = "close")
    @Lazy
    public DemoServer.Iface demoServerClient(ThriftConsulFactory factory) {
        return factory.create(DemoServer.Iface.class, CIRCUIT_BREAK);
    }
}

public class DemoServer {
    @Inject
    private DemoServer.Iface demoServer;

    public void doLoadTesting() {
        for (int i = 0; i < 50; i++) {
            MapResult result = demoServer.request("test", 42);
            assertEquals(result.getStatus(), Status.OK);
        }
    }
}
```


Server:
```java
@Configuration
@ComponentScan(value = "com.qiwi")
@PropertySource("classpath:server.properties")
public class ServerConfiguration {
    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigurer() {
       return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public ConsulService consulService(Environment environment) {
        ParameterSource subpath = ParameterSource.subpath("spring.cloud.", environment::getProperty);
        ThriftConsulConfig config = new ThriftConsulConfig.Builder()
                .fromParameterSource(subpath)
                .setApplicationName(appName)
                .build();
        return new ConsulService(config);
    }

    @Bean
    public ThriftConsulRegistrator consulRegistrator(
            ConsulService consulService,
            Collection<AbstractThriftServer> servers
    ){
        return new ThriftConsulRegistrator(consulService, servers);
    }
}

@Named
public class TestThriftServer extends AbstractThriftServer {
    public final DemoServerImp demoServerImp;

    @Inject
    public TestThriftServer(DemoServerImp serverImp) {
        this.demoServerImp = serverImp;
    }

    @Override
    public ThriftEndpointConfig createEndpointConfig() {
        ThriftEndpointConfig config = new ThriftEndpointConfig.Builder()
                .addEndpoint(DemoServer.Iface.class, demoServerImp)
                .build();
        return config;
    }

    @Override
    public ThriftServerConfig createConfig() {
        return new ThriftServerConfig.Builder().build();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
```

## Other usage example

ThriftConsulDemoTest - fully functional spring application with support for:
1. Single port service multiplexing
1. Consul
1. Sync and async clients

BalancerSyncDemoTest - balancing usage examples
SyncDemoTest - usage of thrift pool only in pulling mode without any balancer and circuit breaker
ThriftKeySyncClientImpTest - some litle example with key based sharding balancing

## Supported pooling options

To view all pooling options see javadoc for ThriftBalancerConfig.Builder

highly recommended call enableCircuitBreak because default option is not so good.

## Contribution
Pull requests are welcome! Please write test for you patches.
After making any changes, please run all tests.

