package com.qiwi.thrift.tracing;

import com.qiwi.thrift.tracing.thrift.RequestHeader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

public class TCompactTracedProtocol extends TCompactProtocol {
    public static final byte MESSAGE_TYPE_CALL_WITH_TRACE_ID = 6;
    public static final byte MESSAGE_TYPE_ONEWAY_WITH_TRACE_ID = 7;
    public static final byte[] EMPTY_BYTES = new byte[0];

    public static class Factory implements TProtocolFactory {
        private final long stringLengthLimit;
        private final long containerLengthLimit;
        private final ThriftTraceMode traceMode;

        public Factory(
                long stringLengthLimit,
                long containerLengthLimit,
                ThriftTraceMode traceMode
        ) {
            this.stringLengthLimit = stringLengthLimit;
            this.containerLengthLimit = containerLengthLimit;
            this.traceMode = traceMode;
        }

        @Override
        public TProtocol getProtocol(TTransport trans) {
            return new TCompactTracedProtocol(
                    trans,
                    stringLengthLimit,
                    containerLengthLimit,
                    traceMode
            );
        }
    }

    private final ThriftTraceMode traceMode;
    private final long stringLengthLimit;

    public TCompactTracedProtocol(
            TTransport transport,
            long stringLengthLimit,
            long containerLengthLimit,
            ThriftTraceMode traceMode
    ) {
        super(transport, stringLengthLimit, containerLengthLimit);
        this.stringLengthLimit = stringLengthLimit;
        this.traceMode = traceMode;
    }

    @Override
    public TMessage readMessageBegin() throws TException {
        TMessage message = super.readMessageBegin();
        if (message.type == MESSAGE_TYPE_CALL_WITH_TRACE_ID) {
            return readHeader(message, TMessageType.CALL);
        } else if (message.type == MESSAGE_TYPE_ONEWAY_WITH_TRACE_ID) {
            return readHeader(message, TMessageType.ONEWAY);
        } else {
            return message;
        }
    }

    private TMessage readHeader(TMessage message, byte messageType) throws TException {
        RequestHeader header = new RequestHeader();
        header.read(this);
        ThriftLogContext.setParentRequestHeader(header);
        return new TMessage(message.name, messageType, message.seqid);
    }

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
        if (traceMode == ThriftTraceMode.BASIC) {
            if (message.type == TMessageType.CALL) {
                writeHeader(message, MESSAGE_TYPE_CALL_WITH_TRACE_ID);
            } else if (message.type == TMessageType.ONEWAY) {
                writeHeader(message, MESSAGE_TYPE_ONEWAY_WITH_TRACE_ID);
            } else {
                super.writeMessageBegin(message);
            }
        } else {
            super.writeMessageBegin(message);
        }
    }

    private void writeHeader(TMessage message, byte messageType) throws TException {
        super.writeMessageBegin(new TMessage(message.name, messageType, message.seqid));
        ThriftLogContext.getRequestHeader().write(this);
    }


     private int readVarint32() throws TException {
        int result = 0;
        int shift = 0;
        if (trans_.getBytesRemainingInBuffer() >= 5) {
            byte[] buf = trans_.getBuffer();
            int pos = trans_.getBufferPosition();
            int off = 0;
            while (true) {
                byte b = buf[pos+off];
                result |= (int) (b & 0x7f) << shift;
                if ((b & 0x80) != 0x80) {
                    break;
                }
                shift += 7;
                off++;
            }
            trans_.consumeBuffer(off+1);
        } else {
            while (true) {
                byte b = readByte();
                result |= (int) (b & 0x7f) << shift;
                if ((b & 0x80) != 0x80) {
                    break;
                }
                shift += 7;
            }
        }
        return result;
    }

    private void checkStringReadLength(int length) throws TProtocolException {
        if (length < 0) {
            throw new TProtocolException(TProtocolException.NEGATIVE_SIZE,
                    "Negative length: " + length);
        }
        if (stringLengthLimit != -1 && length > stringLengthLimit) {
            throw new TProtocolException(TProtocolException.SIZE_LIMIT,
                    "Length exceeded max allowed: " + length);
        }
    }

    public ByteBuffer readBinary() throws TException {
        int length = readVarint32();
        checkStringReadLength(length);
        if (length == 0) {
            return ByteBuffer.wrap(EMPTY_BYTES);
        }

        if (trans_.getBytesRemainingInBuffer() >= length) {
            ByteBuffer bb = ByteBuffer.allocate(length);
            bb.put(trans_.getBuffer(), trans_.getBufferPosition(), length);
            bb.rewind();
            trans_.consumeBuffer(length);
            return bb;
        }

        byte[] buf = new byte[length];
        trans_.readAll(buf, 0, length);
        return ByteBuffer.wrap(buf);
    }

}
