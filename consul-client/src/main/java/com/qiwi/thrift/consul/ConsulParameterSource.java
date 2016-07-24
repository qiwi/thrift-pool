package com.qiwi.thrift.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.qiwi.thrift.utils.ParameterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ConsulParameterSource implements ParameterSource {
    private static final Logger log = LoggerFactory.getLogger(ConsulService.class);
    public static final Duration REFRESH_PERIOD = Duration.ofMinutes(1);

    private final ConsulClient client;
    private final ThriftConsulConfig config;
    private final String path;

    private volatile Instant nextRefresh = Instant.MIN;
    private volatile Map<String, String> parameters = null;


    public ConsulParameterSource(
            ConsulClient client,
            ThriftConsulConfig config,
            String path
    ) {
        this.client = client;
        this.config = config;
        this.path = path.endsWith("/")? path: path + '/';
    }

    public String getPath() {
        return path;
    }

    @Override
    public String getString(String name, String defaultValue) {
        return parameters.getOrDefault(name, defaultValue);
    }

    @Override
    public void refresh() {
        if (nextRefresh.isAfter(Instant.now()) &&  parameters != null) {
            return;
        }
        synchronized (this) {
            log.debug("Reloading config from consul. Path {}", path);
            if (nextRefresh.isAfter(Instant.now()) &&  parameters != null) {
                return;
            }
            try {
                nextRefresh = Instant.MAX;
                Response<List<GetValue>> response = client.getKVValues(
                        path,
                        config.getAclToken().orElse(null),
                        QueryParams.DEFAULT
                );
                Map<String, String> newParameters = new HashMap<>();
                if (response.getValue() != null) {
                    for (GetValue value : response.getValue()) {
                        String key = value.getKey();
                        String val = value.getDecodedValue(StandardCharsets.UTF_8);
                        if (key != null && val != null && !key.endsWith("/")) {
                            key = key.replace(path, "").replace('/', '.');
                            newParameters.put(key, val);
                        }
                    }
                }
                parameters = newParameters;
                log.debug("Config reloaded. Path {}. Parameters loaded {}", path, newParameters.size());
            } finally {
                nextRefresh = Instant.now().plus(REFRESH_PERIOD);
            }
        }
    }
}
