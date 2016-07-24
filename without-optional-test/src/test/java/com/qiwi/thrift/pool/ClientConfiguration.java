package com.qiwi.thrift.pool;

import com.qiwi.thrift.consul.ConsulService;
import com.qiwi.thrift.consul.ThriftConsulConfig;
import com.qiwi.thrift.consul.ThriftConsulFactory;
import com.qiwi.thrift.pool.server.DemoServer;
import com.qiwi.thrift.utils.ParameterSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

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
    public DemoServer.Iface demoServer(ThriftConsulFactory factory) {
        return factory.create(DemoServer.Iface.class, CIRCUIT_BREAK);
    }
}
