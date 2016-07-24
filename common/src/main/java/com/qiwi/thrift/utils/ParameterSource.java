package com.qiwi.thrift.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@FunctionalInterface
public interface ParameterSource {
    Logger log = LoggerFactory.getLogger(ParameterSource.class);
    String NOT_DEFINED = "NOT_DEFINED";

    ParameterSource EMPTY = new ParameterSource() {
        @Override
        public String getString(String name, String defaultValue) {
            return defaultValue;
        }

        @Override
        public int getInteger(String name, int defaultValue) {
            return defaultValue;
        }

        @Override
        public double getDouble(String name, double defaultValue) {
            return defaultValue;
        }

        @Override
        public boolean getBoolean(String name, boolean defaultValue) {
            return defaultValue;
        }

        @Override
        public Duration getDuration(String name, Duration defaultValue) {
            return defaultValue;
        }

        @Override
        public String getFullPath(String name) {
            return name;
        }
    };

    String getString(String name, String defaultValue);

    default int getInteger(String name, int defaultValue){
        String val = getString(name, NOT_DEFINED);
        if (NOT_DEFINED.equals(val)) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                log.warn("Unable to parse parameter: {} value: {}", getFullPath(name), val);
                return defaultValue;
            }
        }
    }

    default double getDouble(String name, double defaultValue){
        String val = getString(name, NOT_DEFINED);
        if (NOT_DEFINED.equals(val)) {
            return defaultValue;
        } else {
            try {
                return Double.parseDouble(val);
            } catch (NumberFormatException e) {
                log.warn("Unable to parse parameter: {} value: {}", getFullPath(name), val);
                return defaultValue;
            }
        }
    }

    @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
    default boolean getBoolean(String name, boolean defaultValue) {
        String val = getString(name, NOT_DEFINED);
        if (NOT_DEFINED.equals(val)) {
            return defaultValue;
        } else {
            try {
                return ThriftUtils.parseBoolean(val);
            } catch (NumberFormatException e) {
                log.warn("Unable to parse parameter: {} value: {}", getFullPath(name), val);
                return defaultValue;
            }
        }
    }

    default String getFullPath(String name) {
        return name;
    }

    default Duration getDuration(String name, Duration defaultValue) {
        String val = getString(name, NOT_DEFINED);
        if (NOT_DEFINED.equals(val)) {
            return defaultValue;
        } else {
            try {
                int millis = Integer.parseInt(val);
                return Duration.ofMillis(millis);
            } catch (NumberFormatException e) {
                log.warn("Unable to parse parameter: {} value: {}", getFullPath(name), val);
                return defaultValue;
            }
        }
    }

    default void refresh() {
    }

    static ParameterSource subpath(String path, ParameterSource parameterSource){
        return new ParameterSource() {
            @Override
            public String getString(String name, String defaultValue) {
                return parameterSource.getString(path + name, defaultValue);
            }

            @Override
            public int getInteger(String name, int defaultValue) {
                return parameterSource.getInteger(path + name, defaultValue);
            }

            @Override
            public Duration getDuration(String name, Duration defaultValue) {
                return parameterSource.getDuration(path + name, defaultValue);
            }

            @Override
            public double getDouble(String name, double defaultValue) {
                return parameterSource.getDouble(path + name, defaultValue);
            }

            @Override
            public boolean getBoolean(String name, boolean defaultValue) {
                return parameterSource.getBoolean(path + name, defaultValue);
            }

            @Override
            public String getFullPath(String name) {
                return path + parameterSource.getFullPath(name);
            }

            @Override
            public void refresh() {
                parameterSource.refresh();
            }
        };
    }

    static ParameterSource combine(ParameterSource base, ParameterSource overrides){
        return new ParameterSource() {
            @Override
            public String getString(String name, String defaultValue) {
                return overrides.getString(name, base.getString(name, defaultValue));
            }

            @Override
            public int getInteger(String name, int defaultValue) {
                return overrides.getInteger(name, base.getInteger(name, defaultValue));
            }

            @Override
            public double getDouble(String name, double defaultValue) {
                return overrides.getDouble(name, base.getDouble(name, defaultValue));
            }

            @Override
            public boolean getBoolean(String name, boolean defaultValue) {
                return overrides.getBoolean(name, base.getBoolean(name, defaultValue));
            }

            @Override
            public String getFullPath(String name) {
                return overrides.getFullPath(name);
            }

            @Override
            public Duration getDuration(String name, Duration defaultValue) {
                return overrides.getDuration(name, base.getDuration(name, defaultValue));
            }

            @Override
            public void refresh() {
                base.refresh();
                overrides.refresh();
            }
        };
    }
}
