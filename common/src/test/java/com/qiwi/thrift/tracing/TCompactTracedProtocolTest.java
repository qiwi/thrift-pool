package com.qiwi.thrift.tracing;

import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

public class TCompactTracedProtocolTest {

    @Test(groups = "unit")
    public void readBinary() throws Exception {
        TFastFramedTransport transport = new TFastFramedTransport(new TMemoryBuffer(1024));

        TCompactTracedProtocol protocol = new TCompactTracedProtocol(transport, 1024, 1024, ThriftTraceMode.DISABLED);
        protocol.writeBinary(ByteBuffer.wrap("Test1".getBytes(StandardCharsets.UTF_8)));
        transport.flush();
        protocol.writeBinary(ByteBuffer.wrap("Test2".getBytes(StandardCharsets.UTF_8)));
        transport.flush();
        transport.open();


        ByteBuffer byteBuffer1 = protocol.readBinary();
        ByteBuffer byteBuffer2 = protocol.readBinary();

        byte[] bytes1 = new byte[byteBuffer1.remaining()];
        byteBuffer1.get(bytes1);
        byte[] bytes2 = new byte[byteBuffer2.remaining()];
        byteBuffer2.get(bytes2);
        assertEquals(new String(bytes1, StandardCharsets.UTF_8), "Test1", "Second string");
        assertEquals(new String(bytes2, StandardCharsets.UTF_8), "Test2");

    }
}
