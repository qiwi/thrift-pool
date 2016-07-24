package com.qiwi.thrift.pool;

import com.qiwi.thrift.demo.AbstractThriftServerTest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.testng.annotations.Test;

import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ServerAttackTest extends AbstractThriftServerTest {

    @Test(groups = "unit", timeOut = 10000L)
    public void randomDataTest() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", thriftPort)){
            socket.getOutputStream().write("some long long test data".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            socket.shutdownOutput();
            try {
                assertEquals(socket.getInputStream().read(), -1);
            } catch (SocketException exception) {
                // На unix летит Connection reset, на Windows закрытие сокета
            }
        }
    }

    @Test(groups = "unit")
    public void halfRandomDataTest() throws Exception {
        try (TSocket socket = new TSocket("127.0.0.1", thriftPort)){
            TFastFramedTransport framedTransport = new TFastFramedTransport(socket);
            framedTransport.open();
            byte[] header = {0, 0, 0, -127, // размер фрейма
                    -126, // протокол
                    33, // версия
                    1, // seqid
                    16, // длина строки
                    68, 101, 109, 111, 83, 101, 114, 118, 101, 114, 58, 99, 114, 97, 115, 104,  // "DemoServer:crash",
                    8,// Тип
                    20, // Id * 2
                    (byte)0x81, (byte)0x81, (byte)0x81, (byte)0x81, 4
            };
            String testText = "some long long test data";
            testText += testText;
            testText += testText;
            testText += testText;
            testText += testText;


            byte[] testData = testText.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = Arrays.copyOf(header, header.length + testData.length);
            System.arraycopy(testData, 0, bytes, header.length, testData.length);
            socket.write(bytes);
            socket.flush();
            TCompactProtocol protocol = new TCompactProtocol(framedTransport);
            TMessage message = protocol.readMessageBegin();
            assertEquals(message.type, TMessageType.EXCEPTION);
            TApplicationException exception = TApplicationException.read(protocol);
            assertTrue(exception.getMessage().contains("Length exceeded max allowed"), exception.getMessage());

        }
    }
}
