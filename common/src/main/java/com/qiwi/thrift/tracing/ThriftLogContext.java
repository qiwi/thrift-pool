package com.qiwi.thrift.tracing;

import com.qiwi.thrift.tracing.thrift.RequestHeader;
import com.qiwi.thrift.utils.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.EnumMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

public class ThriftLogContext {
    private static final Logger log = LoggerFactory.getLogger(ThriftLogContext.class);

    public static final String TRACE_ID = "traceId";
    public static final String SPAN_ID = "spanId";
    public static final String PARENT_SPAN_ID = "parentSpanId";
    public static final String SAMPLED = "sampled";
    public static final String CLIENT_ADDRESS = "clientAdr";

    private static Optional<BooleanSupplier> sampledSupplier = Optional.empty();

    private static ThriftRequestReporter defaultClientReporter = ThriftRequestReporter.NULL_REPORTER;
    private static ThriftRequestReporter defaultServerReporter = ThriftRequestReporter.NULL_REPORTER;
    private ThriftLogContext(){}

    // Cache - to prevent slow string-to-long conversion
    private static final ThreadLocal<Context> threadLocal = ThreadLocal.withInitial(() -> new Context());


    /**
     * Assign new TraceId and SpanId to MDC
     *
     * Example:
     *
     * protected void doGet(HttpServletRequest req, HttpServletResponse resp)
     *     throws ServletException, IOException {
     *
     *     ThriftLogContext.newExecution();
     *     resp.getWriter().write("Request result: " + doRequestProcessing(req.getParametersMap()));
     * }
     * Thrift pool call this automatically
     */
    public static void newExecution() {
        MDC.clear();
        threadLocal.get().setHeader(new RequestHeader());
    }

    public static long getTraceId(){
        return threadLocal.get().getHeader().getTraceId();
    }

    public static void setTraceId(long traceId) {
        MDC.put(TRACE_ID, Long.toHexString(traceId));
    }

    public static long getSpanId(){
        return threadLocal.get().getHeader().getSpanId();
    }

    public static void setSpanId(long spanId) {
        MDC.put(SPAN_ID, Long.toHexString(spanId));
    }

    public static OptionalLong getParentSpanId(){
        RequestHeader header = threadLocal.get().getHeader();
        return header.isSetParentSpanId()? OptionalLong.of(header.getParentSpanId()): OptionalLong.empty();
    }

    public static void setParentSpanId(OptionalLong parentSpanId) {
        if (parentSpanId.isPresent()) {
            MDC.put(PARENT_SPAN_ID, Long.toHexString(parentSpanId.getAsLong()));
        } else {
            MDC.remove(PARENT_SPAN_ID);
        }
    }

    public static Optional<Boolean> isSampled() {
        RequestHeader header = threadLocal.get().getHeader();
        return header.isSetSampled()? Optional.of(header.isSampled()): Optional.empty();
    }

    public static void setSampled(Optional<Boolean> sampled) {
        if (sampled.isPresent()) {
            MDC.put(SAMPLED, sampled.get().toString());
        } else {
            MDC.remove(SAMPLED);
        }
    }


    public static RequestHeader getRequestHeader() {
        return threadLocal.get().getHeader();
    }

    public static void setRequestHeader(RequestHeader header) {
        threadLocal.get().setHeader(header);
    }

    public static void setParentRequestHeader(RequestHeader header) {
        header.setParentSpanId(header.getSpanId());
        header.setSpanId(ThreadLocalRandom.current().nextLong());
        threadLocal.get().setHeader(header);
    }




    // Guarantee 16 char length
    public static String generateId() {
        return Long.toHexString((Long) generate(RequestHeader._Fields.TRACE_ID));
    }

    public static Optional<BooleanSupplier> getSampledSupplier() {
        return sampledSupplier;
    }

    public static void setSampledSupplier(Optional<BooleanSupplier> sampledSupplier) {
        ThriftLogContext.sampledSupplier = sampledSupplier;
    }

    public static void setDefaultReporter(ThriftRequestReporter defaultRequestReporter) {
        ThriftLogContext.defaultClientReporter = Objects.requireNonNull(defaultRequestReporter);
        ThriftLogContext.defaultServerReporter = Objects.requireNonNull(defaultRequestReporter);
    }

    public static ThriftRequestReporter getDefaultClientReporter() {
        return defaultClientReporter;
    }

    public static void setDefaultClientReporter(ThriftRequestReporter defaultClientReporter) {
        ThriftLogContext.defaultClientReporter = defaultClientReporter;
    }

    public static ThriftRequestReporter getDefaultServerReporter() {
        return defaultServerReporter;
    }

    public static void setDefaultServerReporter(ThriftRequestReporter defaultServerReporter) {
        ThriftLogContext.defaultServerReporter = defaultServerReporter;
    }

    private static Object generate(RequestHeader._Fields field) {
        switch (field) {
            case TRACE_ID:
            case SPAN_ID:
                long id;
                do {
                    id = ThreadLocalRandom.current().nextLong();
                    // Search number with 16 char length in hex representation
                } while ((id & 0xF000_0000_0000_0000L) == 0);
                return id;
            case PARENT_SPAN_ID:
                return null;
            case SAMPLED:
                return sampledSupplier.map(BooleanSupplier::getAsBoolean).orElse(null);
            default:
                throw new IllegalArgumentException("Unsupported field " + field);
        }
    }

    public static String getClientAddress() {
        return MDC.get(CLIENT_ADDRESS);
    }

    public static void setClientAddress(String clientAddress) {
        MDC.put(CLIENT_ADDRESS, clientAddress);
    }

    private static class Context{
        private EnumMap<RequestHeader._Fields, String> cached = new EnumMap<>(RequestHeader._Fields.class);
        private RequestHeader cachedHeader = new RequestHeader();

        public RequestHeader getHeader(){
            readField(cachedHeader, TRACE_ID, RequestHeader._Fields.TRACE_ID);
            readField(cachedHeader, SPAN_ID, RequestHeader._Fields.SPAN_ID);
            readField(cachedHeader, PARENT_SPAN_ID, RequestHeader._Fields.PARENT_SPAN_ID);
            readField(cachedHeader, SAMPLED, RequestHeader._Fields.SAMPLED);
            return cachedHeader;
        }

        public void setHeader(RequestHeader header){
            this.cachedHeader = header;
            storeFieldToMDC(cachedHeader, TRACE_ID, RequestHeader._Fields.TRACE_ID);
            storeFieldToMDC(cachedHeader, SPAN_ID, RequestHeader._Fields.SPAN_ID);
            storeFieldToMDC(cachedHeader, PARENT_SPAN_ID, RequestHeader._Fields.PARENT_SPAN_ID);
            storeFieldToMDC(cachedHeader, SAMPLED, RequestHeader._Fields.SAMPLED);
        }


        private void readField(RequestHeader header, String mdcParameterName, RequestHeader._Fields field) {
            String strValue = MDC.get(mdcParameterName);
            if (Objects.equals(cached.get(field), strValue)) {
                if (strValue != null
                        || field == RequestHeader._Fields.PARENT_SPAN_ID
                        || field == RequestHeader._Fields.SAMPLED) {
                    return;
                }
            }
            Object value = null;
            if (!ThriftUtils.empty(strValue)) {
                try {
                    value = parse(field, strValue);
                } catch (RuntimeException ex) {
                    log.error("Unable to parse MDC parameter {}, value {}", mdcParameterName, strValue, ex);
                }
            }
            if (value == null) {
                value = generate(field);
                strValue = toString(field, value);
            }

            header.setFieldValue(field, value);
            putToMdc(mdcParameterName, field, strValue);
        }


        private void storeFieldToMDC(RequestHeader header, String mdcParameterName, RequestHeader._Fields field){
            Object value;
            if (header.isSet(field)) {
                value = header.getFieldValue(field);
            } else {
                value = generate(field);
                header.setFieldValue(field, value);
            }
            String strValue = toString(field, value);
            putToMdc(mdcParameterName, field, strValue);
        }

        private void putToMdc(String mdcParameterName, RequestHeader._Fields field, String strValue) {
            if (strValue == null) {
                MDC.remove(mdcParameterName);
                cached.remove(field);
            } else {
                MDC.put(mdcParameterName, strValue);
                cached.put(field, strValue);
            }
        }

        public Object parse(RequestHeader._Fields field, String strValue) {
            switch (field) {
                case TRACE_ID:
                case SPAN_ID:
                case PARENT_SPAN_ID:
                    return Long.parseUnsignedLong(strValue, 16);
                case SAMPLED:
                    return ThriftUtils.parseBoolean(strValue);
                 default:
                    throw new IllegalArgumentException("Unsupported field " + field);
            }
        }



        public String toString(RequestHeader._Fields field, Object value) {
            if (value == null) {
                return null;
            }
            switch (field) {
                case TRACE_ID:
                case SPAN_ID:
                case PARENT_SPAN_ID:
                    return Long.toHexString((Long)value);
                case SAMPLED:
                    return value.toString();
                default:
                    throw new IllegalArgumentException("Unsupported field " + field);
            }
        }
    }

}
