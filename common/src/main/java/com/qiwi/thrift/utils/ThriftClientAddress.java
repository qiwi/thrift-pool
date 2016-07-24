package com.qiwi.thrift.utils;

import com.qiwi.thrift.metrics.ThriftMonitoring;
import com.qiwi.thrift.tracing.ThriftTraceMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ThriftClientAddress implements Comparable<ThriftClientAddress> {
    private static final Logger log = LoggerFactory.getLogger(ThriftClientAddress.class);

    public static final int DEFAULT_SERVER_THRIFT_PORT = 9090;
    public static final String DEFAULT_DC_NAME_CLIENT = "none";
    public static final String DEFAULT_DC_NAME_SERVER = "unset";
    public static final String DEFAULT_CLUSTER_NAME = "V1";
    public static final String DC_PARAMETER = "dc";
    public static final String TRACE_TYPE_PARAMETER = "trace";
    public static final String CLUSTER_NAME_PARAMETER = "clusterName";
    @Deprecated
    public static final String VERSION_PARAMETER = "version";

    private final String host;
    private final int port;
    private final String address;
    private final String addressString;
    private final String escapedString;
    /**
     * Здесь хранятся параметры переданные в консул, такие как текущий dc, имя кластера.
     */
    private final Map<String, String> parameters;

    public ThriftClientAddress(String host, int port) {
        this(host, port, Collections.emptyMap());
    }

    public ThriftClientAddress(String host, int port, Map<String, String> parameters) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.parameters = Collections.unmodifiableMap(parameters);
        StringBuilder builder = new StringBuilder(host.length() + 10 + parameters.size() * 20);
        builder.append(host).append(':').append(port);
        address = builder.toString();
        escapedString = ThriftMonitoring.escapeDots(address);
        if (!parameters.isEmpty()) {
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                builder.append(',').append(entry.getKey());
                if (!ThriftUtils.empty(entry.getValue())) {
                    builder.append("=").append(entry.getValue());
                }
            }
        }
        addressString = builder.toString();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public String getDc(){
        return parameters.getOrDefault(DC_PARAMETER, DEFAULT_DC_NAME_SERVER);
    }

    public String getClusterName(){
        return parameters.getOrDefault(CLUSTER_NAME_PARAMETER, DEFAULT_CLUSTER_NAME);
    }

    public Optional<ThriftTraceMode> getTraceMode() {
        String str = parameters.get(TRACE_TYPE_PARAMETER);

        if (str != null) {
            try {
                return Optional.of(ThriftTraceMode.valueOf(str));
            } catch (IllegalArgumentException ex) {
                log.warn("Unsupported trace type {}, try to fallback to BASIC", str, ex);
                // this is some new trace type unknown to client, but SIMPLE must be supported for backward compatibility
                return Optional.of(ThriftTraceMode.BASIC);
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     *
     * @return address in host:port format
     */
    public String toShortString() {
        return address;
    }

    public String toEscapedString() {
        return escapedString;
    }

    @Override
    public String toString() {
        return addressString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThriftClientAddress that = (ThriftClientAddress) o;

        if (port != that.port) {
            return false;
        }
        return host.equals(that.host);

    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    public static ThriftClientAddress parse(String address){
        Objects.requireNonNull(address, "address");
        String[] params = address.split(",");

        String[] urlSplit = params[0].split(":", 2);
        String host = urlSplit[0];
        if (host.contains("=") || host.trim().isEmpty()) {
            throw new ThriftConnectionException("Unable to parse address " + address);
        }
        int port;
        if (urlSplit.length == 2) {
            try {
                port = Integer.parseInt(urlSplit[1]);
            } catch (NumberFormatException e) {
                throw new ThriftConnectionException("Unable to parse address " + address, e);
            }
        } else {
            port = DEFAULT_SERVER_THRIFT_PORT;
        }

        Map<String, String> paramsMap;
        if (params.length > 1) {
            paramsMap = new LinkedHashMap<>(params.length);
            for (int i = 1; i < params.length; i++) {
                String[] nameValue = params[i].split("=", 2);
                if (nameValue.length == 1) {
                    paramsMap.put(nameValue[0], "");
                } else {
                    paramsMap.put(nameValue[0], nameValue[1]);
                }
            }
        } else {
            paramsMap = Collections.emptyMap();
        }

        return new ThriftClientAddress(host, port, paramsMap);
    }

    public static ThriftClientAddress of(ParameterSource source) {
        return supplier(source).get();
    }


    public static Supplier<ThriftClientAddress> supplier(ParameterSource source) {
        return () -> {
            String address = source.getString("address", null);
            if (address != null) {
                return parse(address);
            } else {
                String host = source.getString("host", null);
                int port = source.getInteger("port", DEFAULT_SERVER_THRIFT_PORT);
                if (host == null) {
                    throw new ThriftConnectionException("Parameter \"address\" not found in parameter source");
                } else {
                    return new ThriftClientAddress(host, port);
                }
            }
        };
    }

    public static List<ThriftClientAddress> parseList(String servers) {
        String[] split = servers.split(";");
        return Arrays.stream(split)
                .map(ThriftClientAddress::parse)
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(ThriftClientAddress o) {
        return address.compareTo(o.address);
    }

}

