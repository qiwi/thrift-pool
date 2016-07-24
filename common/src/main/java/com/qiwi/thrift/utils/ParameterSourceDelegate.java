package com.qiwi.thrift.utils;

import java.time.Duration;

public class ParameterSourceDelegate implements ParameterSource {
    private ParameterSource delegate;

    public ParameterSourceDelegate(ParameterSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getString(String name, String defaultValue) {
        return delegate.getString(name, defaultValue);
    }

    @Override
    public int getInteger(String name, int defaultValue) {
        return delegate.getInteger(name, defaultValue);
    }

    @Override
    public double getDouble(String name, double defaultValue) {
        return delegate.getDouble(name, defaultValue);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        return delegate.getBoolean(name, defaultValue);
    }

    @Override
    public String getFullPath(String name) {
        return delegate.getFullPath(name);
    }

    @Override
    public Duration getDuration(String name, Duration defaultValue) {
        return delegate.getDuration(name, defaultValue);
    }

    @Override
    public void refresh() {
        delegate.refresh();
    }
}
