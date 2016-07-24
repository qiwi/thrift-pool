package com.qiwi.thrift.balancer.key;

import com.qiwi.thrift.balancer.load.ThriftBalancerConfig;

public class ThriftKeyBalancerConfig extends ThriftBalancerConfig {
    private final int quorumSize;

    protected ThriftKeyBalancerConfig(Builder builder) {
        super(builder);
        quorumSize = builder.getQuorumSize();
        if (quorumSize < 1) {
            throw new IllegalArgumentException("To small quorum size");
        }
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ThriftKeyBalancerConfig that = (ThriftKeyBalancerConfig) o;

        return quorumSize == that.quorumSize;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + quorumSize;
        return result;
    }

    public static class Builder extends ThriftBalancerConfig.UntypedBuilder<Builder> {
        private int quorumSize = 1;
        /**
         * private final double minNodesInRingRatio;
         * private final int minAliveNodes;
         * private final int maxFailNodes;
         *
         * @param failureHandling - CIRCUIT_BREAK if application can work without this pool
         *                        TRY_CONTINUE - if pool critical to current application.
         */
        public Builder(MethodOfFailureHandling failureHandling) {
            super(failureHandling);
        }

        public int getQuorumSize() {
            return source.getInteger("quorum_size", quorumSize);
        }

        public void setQuorumSize(int quorumSize) {
            this.quorumSize = quorumSize;
        }

        public Builder fromKeyBalancerConfig(ThriftKeyBalancerConfig config) {
            super.fromBalancerConfig(config);

            return super.getThis();
        }

        public ThriftKeyBalancerConfig build() {
            try {
                getServersSupplier().get();
            } catch (Exception ex) {
                throw new IllegalStateException(
                        "No client adders provided in parameter " + source.getFullPath("servers")
                        + " not found. Try to use ThriftConsulFactory.",
                        ex
                );
            }
            return new ThriftKeyBalancerConfig(this);
        }
    }
}
