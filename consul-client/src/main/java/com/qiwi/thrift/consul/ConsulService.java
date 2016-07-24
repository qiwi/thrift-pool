package com.qiwi.thrift.consul;


import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.qiwi.thrift.metrics.ThriftGraphiteConfig;
import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.utils.ParameterSource;
import com.qiwi.thrift.utils.ThriftClientAddress;
import com.qiwi.thrift.utils.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class ConsulService {
    private static final Logger log = LoggerFactory.getLogger(ConsulService.class);
    public static final String HEARTBEAT_MAX_TTL = "30s";

    private final ThriftConsulConfig config;
    private final ConsulClient client;
    private final List<ConsulParameterSource> configLevels;

    public ConsulService(ThriftConsulConfig config) {
        this.config = config;
        this.client = new ConsulClient(config.getConsulHost(), config.getConsulPort());

        String dcName = config.getDcName().orElse(ThriftClientAddress.DEFAULT_DC_NAME_SERVER);

        List<String> paths = new ArrayList<>();
        paths.add("config/application/");
        paths.add("config/application," + dcName + "/");
        String applicationName = config.getApplicationName();
        if (ThriftUtils.empty(applicationName) || "application".equals(applicationName)) {
            applicationName = "application";
        } else {
            paths.add("config/" + applicationName + "/");
        }
        paths.add("config/" + applicationName + "," + dcName + "/");
        paths.add("config/" + applicationName + "," + config.getHostName() + "/");
        log.info("Config query paths:\n{}", paths.stream().collect(Collectors.joining("\n")));

        List<ConsulParameterSource> configs = new ArrayList<>();
        paths.forEach(path -> configs.add(new ConsulParameterSource(client, config, path)));
        configLevels = configs;

        try {
            ThriftGraphiteConfig graphiteConfig = new ThriftGraphiteConfig.Builder()
                    .setCluster(config.getApplicationName())
                    .setHostName(config.getHostName())
                    .fromParameterSource(merge(configs, config.getParameterSource()))
                    .build();
            ThriftMonitoring.getMonitor().connectToGraphite(graphiteConfig);
        } catch (Exception ex) {
            log.error("Unable connect to graphite", ex);
        }
    }

    public Optional<String> getDcName(){
        return config.getDcName();
    }

    <I> ParameterSource getParameters(ThriftServiceDescription<I> service, ParameterSource root) {
        List<ConsulParameterSource> parameters = new ArrayList<>();
        parameters.addAll(configLevels);
        String serviceName = service.getFullServiceName();
        parameters.add(new ConsulParameterSource(client, config, "config/" + serviceName + "/"));
        String dcName = config.getDcName().orElse(ThriftClientAddress.DEFAULT_DC_NAME_SERVER);
        parameters.add(new ConsulParameterSource(client, config, "config/" + serviceName + "," + service.getClusterName() + "/"));
        parameters.add(new ConsulParameterSource(client, config, "config/" + serviceName + "," + dcName + "/"));
        parameters.add(new ConsulParameterSource(client, config, "config/" + serviceName + "," + config.getHostName() + "/"));
        return merge(parameters, root);
    }

    private ParameterSource merge(List<ConsulParameterSource> parameters, ParameterSource root){
        ParameterSource current = root;
        for (ConsulParameterSource parameter : parameters) {
            parameter.refresh();
            current = ParameterSource.combine(current, parameter);
        }
        return current;
    }

    static String getHost(HealthService healthService) {
        HealthService.Service service = healthService.getService();
        HealthService.Node node = healthService.getNode();

        if (!ThriftUtils.empty(service.getAddress())) {
            return service.getAddress();
        } else if (!ThriftUtils.empty(node.getAddress())) {
            return node.getAddress();
        }
        return node.getNode();
    }

    private ThriftClientAddress parseAddress(HealthService service){
        List<String> tags = service.getService().getTags();
        Map<String, String> params = new HashMap<>();
        for (String tag : tags) {
            String[] split = tag.split("=", 2);
            if (split.length > 1) {
                params.put(split[0], split[1]);
            } else {
                params.put(split[0], "");
            }
        }

        return new ThriftClientAddress(
                getHost(service),
                service.getService().getPort(),
                params
        );
    }


    <I> Set<ThriftClientAddress> getServices(ThriftServiceDescription<I> service) {
        Response<List<HealthService>> services = client.getHealthServices(
                service.getFullServiceName(),
                ThriftClientAddress.CLUSTER_NAME_PARAMETER + "=" + service.getClusterName(),
                false,
                QueryParams.DEFAULT,
                config.getAclToken().orElse(null)
        );
        Set<ThriftClientAddress> addressSet = services.getValue().stream()
                .map(this::parseAddress)
                .collect(Collectors.toSet());
        log.debug("Queried nodes for service {}, nodes: {}", service.getFullServiceName(), addressSet);
        return addressSet;
    }

    private String getServiceId(ThriftServiceDescription<?> service, int port) {
        return service.getFullServiceName() + ('_') + config.getHostName() + ('_') + port;
    }

    void register(ThriftServiceDescription<?> service, ThriftClientAddress address, Map<String, String> tags) {
        Map<String, String> newTags = new HashMap<>(address.getParameters().size() + tags.size() + 2);
        newTags.putAll(address.getParameters());
        newTags.putAll(tags);
        newTags.putIfAbsent(ThriftClientAddress.DC_PARAMETER, config.getDcName().orElse(ThriftClientAddress.DEFAULT_DC_NAME_SERVER));
        newTags.put(ThriftClientAddress.CLUSTER_NAME_PARAMETER, service.getClusterName());
        List<String> tagList = new ArrayList<>(newTags.size());
        for (Map.Entry<String, String> entry : newTags.entrySet()) {
            if (ThriftUtils.empty(entry.getValue()) || entry.getKey().equals(entry.getValue())) {
                tagList.add(entry.getKey());
            } else {
                tagList.add(entry.getKey() + '=' + entry.getValue());
            }
        }

        // register new service with associated health check
        NewService newService = new NewService();
        newService.setName(service.getFullServiceName());
        String serviceId = getServiceId(service, address.getPort());
        newService.setId(serviceId);
        newService.setPort(address.getPort());
        newService.setAddress(config.getHostName());
        newService.setTags(tagList);

        NewService.Check serviceCheck = new NewService.Check();
        serviceCheck.setTtl(HEARTBEAT_MAX_TTL);
        serviceCheck.setDeregisterCriticalServiceAfter("30m");

        newService.setCheck(serviceCheck);

        Response<Void> response = client.agentServiceRegister(
                newService,
                config.getAclToken().orElse(null)
        );
         log.debug("Register service {}", serviceId);
    }

    void deregister(ThriftServiceDescription<?> service, ThriftClientAddress address){
        String serviceId = getServiceId(service, address.getPort());
        client.agentServiceDeregister(serviceId, config.getAclToken().orElse(null));
        log.debug("Deregister service {}", serviceId);
    }

    void heartbeat(ThriftServiceDescription<?> service, ThriftClientAddress address, Map<String, String> parameters) {
        try {
            String serviceId = getServiceId(service, address.getPort());
            Response<Void> voidResponse = client.agentCheckPass(
                    "service:" + serviceId,
                    "Alive by thrift pool heartbeat",
                    config.getAclToken().orElse(null)
            );
            log.trace("Heartbeat for service {}", serviceId);
        } catch (OperationException ex) {
            log.warn("Unable to write heartbeat, try re-register service", ex);
            register(service, address, parameters);
        }
    }

}
