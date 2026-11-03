package com.volo.broker.broker.message;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class ByteArrayPayload implements Payload {

    private final byte[] bytes;

    public ByteArrayPayload(byte[] bytes) {
        this.bytes = bytes == null ? new byte[0] : bytes.clone();
    }

    ByteArrayPayload(byte[] bytes, boolean trusted) {
        this.bytes = (trusted || bytes == null) ? (bytes == null ? new byte[0] : bytes) : bytes.clone();
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public ByteString toByteString() {
        return ByteString.copyFrom(bytes);
    }

    @Override
    public byte[] toByteArray() {
        return bytes.clone();
    }

    @Override
    public ByteBuffer asReadOnlyByteBuffer() {
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes);
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ByteArrayPayload other) {
            return Arrays.equals(bytes, other.bytes);
        }
        if (o instanceof Payload other) {
            if (this.size() != other.size()) return false;
            if (other instanceof ByteStringPayload bsp) {
                return bsp.toByteString().equals(this.toByteString());
            }
            return Arrays.equals(this.bytes, other.toByteArray());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toByteString().hashCode();
    }

    @Override
    public String toString() {
        return "ByteArrayPayload[size=" + size() + "]";
    }
}

