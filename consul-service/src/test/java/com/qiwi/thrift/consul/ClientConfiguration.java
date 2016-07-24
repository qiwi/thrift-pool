package com.qiwi.thrift.consul;

import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;
import com.qiwi.thrift.pool.ThriftAsyncClient;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.pool.server.SecondServer;
import com.qiwi.thrift.pool.server.TestException;
import com.qiwi.thrift.utils.ParameterSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import java.util.Optional;

import static com.qiwi.thrift.balancer.load.ThriftBalancerConfig.MethodOfFailureHandling.CIRCUIT_BREAK;

@Configuration
@ComponentScan(value = "com.qiwi")
@Profile("client")
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
        // for circuit-break example see secondServer factory
        return factory.create(DemoServer.Iface.class, CIRCUIT_BREAK);
    }


    @Bean(destroyMethod = "close")
    @Lazy
    public ThriftAsyncClient<DemoServer.AsyncIface> demoServerAsyncClient(ThriftConsulFactory factory) {
        return factory.createAsync(DemoServer.AsyncIface.class, CIRCUIT_BREAK);
    }


    /**
     * Client will connect only to server with specific cluster name
     *
     * Not connect to servers running inside other test - because cluster name is UUID
     */
    @Bean(destroyMethod = "close")
    @Lazy
    public SecondServer.Iface secondServerClient(ThriftConsulFactory factory) {
        ThriftBalancerConfig.Builder builder = new ThriftBalancerConfig.Builder(CIRCUIT_BREAK)
                .setSubServiceName(Optional.of("imp2"))
                // When calculate circuit-break level ignore exceptions of type TestException if it massage not contains "Unexpected"
                .enableCircuitBreak(0.3, ex -> {
                    if (ex instanceof TestException) {
                        return ex.getMessage().contains("Unexpected");
                    } else {
                        return true;
                    }
                });
        return factory.create(
                SecondServer.Iface.class,
                ThriftConsulDemoTest.clusterUUID,
                builder
        );
    }
}
