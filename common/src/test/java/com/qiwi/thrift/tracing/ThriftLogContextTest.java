package com.qiwi.thrift.tracing;

import com.qiwi.thrift.tracing.thrift.RequestHeader;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;

import static org.testng.Assert.*;

@SuppressWarnings("MagicNumber")
public class ThriftLogContextTest {
    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        ThriftLogContext.newExecution();
    }

    @Test(groups = "unit")
    public void getField() throws Exception {
        long traceId = ThriftLogContext.getTraceId();
        assertEquals(ThriftLogContext.getTraceId(), traceId);
        assertTrue(traceId != 0);

        traceId = ThreadLocalRandom.current().nextLong();
        MDC.put(ThriftLogContext.TRACE_ID, Long.toHexString(traceId));
        assertEquals(ThriftLogContext.getTraceId(), traceId);

        MDC.remove(ThriftLogContext.TRACE_ID);
        assertNotEquals(ThriftLogContext.getTraceId(), traceId);

        RequestHeader header = new RequestHeader();
        header.setTraceId(0xDEAD);
        ThriftLogContext.setRequestHeader(header);
        assertEquals(ThriftLogContext.getTraceId(), 0xDEAD);
        assertEquals(ThriftLogContext.getTraceId(), 0xDEAD);

        header = new RequestHeader();
        ThriftLogContext.setRequestHeader(header);
        assertNotEquals(MDC.get(ThriftLogContext.TRACE_ID), "DEAD");
        assertNotEquals(ThriftLogContext.getSpanId(), 0);
    }


    @Test(groups = "unit")
    public void getSpanId() throws Exception {
        long spanId = ThriftLogContext.getSpanId();
        assertEquals(ThriftLogContext.getSpanId(), spanId);
        assertTrue(spanId != 0);

        spanId = ThreadLocalRandom.current().nextLong();
        MDC.put(ThriftLogContext.SPAN_ID, Long.toHexString(spanId));
        assertEquals(ThriftLogContext.getSpanId(), spanId);

        MDC.remove(ThriftLogContext.SPAN_ID);
        assertNotEquals(ThriftLogContext.getSpanId(), spanId);

        RequestHeader header = new RequestHeader();
        header.setSpanId(0xDEAD);
        ThriftLogContext.setRequestHeader(header);
        assertEquals(ThriftLogContext.getSpanId(), 0xDEAD);
        assertEquals(MDC.get(ThriftLogContext.SPAN_ID), "dead");

        header = new RequestHeader();
        ThriftLogContext.setRequestHeader(header);
        assertNotEquals(MDC.get(ThriftLogContext.SPAN_ID), "dead");
        assertNotEquals(ThriftLogContext.getSpanId(), 0);
    }

    @Test(groups = "unit")
    public void getParentSpanId() throws Exception {
        assertEquals(ThriftLogContext.getParentSpanId().isPresent(), false);

        long parentId = ThreadLocalRandom.current().nextLong();
        MDC.put(ThriftLogContext.PARENT_SPAN_ID, Long.toHexString(parentId));
        assertEquals(ThriftLogContext.getParentSpanId().getAsLong(), parentId);
        assertEquals(ThriftLogContext.getRequestHeader().getParentSpanId(), parentId);

        MDC.remove(ThriftLogContext.PARENT_SPAN_ID);
        assertEquals(ThriftLogContext.getParentSpanId().isPresent(), false);

        RequestHeader header = new RequestHeader();
        header.setParentSpanId(0xDEAD);
        ThriftLogContext.setRequestHeader(header);
        assertEquals(ThriftLogContext.getParentSpanId().getAsLong(), 0xDEAD);
    }

    @Test(groups = "unit")
    public void simpled() throws Exception {
        assertEquals(ThriftLogContext.isSampled().isPresent(), false);

        MDC.put(ThriftLogContext.SAMPLED, "true");
        assertEquals(ThriftLogContext.isSampled().get(), (Boolean) true);
        assertEquals(ThriftLogContext.isSampled().get(), (Boolean) true);

        MDC.remove(ThriftLogContext.SAMPLED);
        assertEquals(ThriftLogContext.getParentSpanId().isPresent(), false);

        RequestHeader header = new RequestHeader();
        header.setSampled(false);
        ThriftLogContext.setRequestHeader(header);
        assertEquals(ThriftLogContext.isSampled().get(), (Boolean) false);
    }

    @Test(groups = "unit")
    public void getRequestHeader() throws Exception {
        MDC.put(ThriftLogContext.TRACE_ID, "DEAD");
        MDC.put(ThriftLogContext.SPAN_ID, "DEAE");
        MDC.put(ThriftLogContext.PARENT_SPAN_ID, "DEAF");
        MDC.put(ThriftLogContext.SAMPLED, "TRUE");
        RequestHeader requestHeader = ThriftLogContext.getRequestHeader();
        assertEquals(requestHeader.getTraceId(), 0xDEAD);
        assertEquals(requestHeader.getSpanId(), 0xDEAE);
        assertEquals(requestHeader.getParentSpanId(), 0xDEAF);
        assertEquals(requestHeader.isSampled(), true);
    }

    @Test(groups = "unit")
    public void setRequestHeader() throws Exception {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setTraceId(0xDEAD);
        requestHeader.setSpanId(0xDEAF);
        requestHeader.setSampled(true);

        ThriftLogContext.setRequestHeader(requestHeader);

        assertEquals(MDC.get(ThriftLogContext.TRACE_ID), "dead");
        assertEquals(MDC.get(ThriftLogContext.SPAN_ID), "deaf");
        assertEquals(MDC.get(ThriftLogContext.PARENT_SPAN_ID), null);
        assertEquals(MDC.get(ThriftLogContext.SAMPLED), "true");
    }

    @Test(groups = "unit")
    public void setParentRequestHeader() throws Exception {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setTraceId(0xDEAD);
        requestHeader.setSpanId(0xDEAF);
        requestHeader.setParentSpanId(0xDEAE);

        ThriftLogContext.setParentRequestHeader(requestHeader);

        assertEquals(MDC.get(ThriftLogContext.TRACE_ID), "dead");
        assertNotEquals(MDC.get(ThriftLogContext.SPAN_ID), "deaf");
        assertEquals(MDC.get(ThriftLogContext.PARENT_SPAN_ID), "deaf");
        assertEquals(MDC.get(ThriftLogContext.SAMPLED), null);
    }

    @Test(groups = "unit")
    public void newExecution() throws Exception {
        MDC.put(ThriftLogContext.TRACE_ID, "DEAD");
        MDC.put(ThriftLogContext.SPAN_ID, "DEAE");
        MDC.put(ThriftLogContext.PARENT_SPAN_ID, "DEAF");
        ThriftLogContext.newExecution();
        assertNotEquals(MDC.get(ThriftLogContext.TRACE_ID), "dead");
        assertNotEquals(MDC.get(ThriftLogContext.SPAN_ID), "deae");
        assertNotEquals(MDC.get(ThriftLogContext.PARENT_SPAN_ID), Optional.of("deaf"));
    }

    @Test(groups = "unit")
    public void setters() throws Exception {
        ThriftLogContext.setTraceId(0xDEAD);
        ThriftLogContext.setSpanId(0xDEAE);
        ThriftLogContext.setParentSpanId(OptionalLong.of(0xDEAF));
        ThriftLogContext.setSampled(Optional.of(true));

        assertEquals(ThriftLogContext.getTraceId(), 0xDEAD);
        assertEquals(ThriftLogContext.getSpanId(), 0xDEAE);
        assertEquals(ThriftLogContext.getParentSpanId(), OptionalLong.of(0xDEAF));
        assertEquals(ThriftLogContext.isSampled(), Optional.of(true));
    }

}
