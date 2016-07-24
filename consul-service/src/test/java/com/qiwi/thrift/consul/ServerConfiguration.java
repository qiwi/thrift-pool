package com.qiwi.thrift.consul;

import com.qiwi.thrift.server.AbstractThriftServer;
import com.qiwi.thrift.utils.ParameterSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import java.util.Collection;

@Configuration
@ComponentScan(value = "com.qiwi")
@Profile("server")
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
