package com.volo.broker.broker.message;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class ByteStringPayload implements Payload {

    public static final ByteStringPayload EMPTY = new ByteStringPayload(ByteString.EMPTY);

    private final ByteString byteString;

    public ByteStringPayload(ByteString byteString) {
        this.byteString = Objects.requireNonNull(byteString, "byteString must not be null");
    }

    @Override
    public int size() {
        return byteString.size();
    }

    @Override
    public ByteString toByteString() {
        return byteString;
    }

    @Override
    public byte[] toByteArray() {
        return byteString.toByteArray();
    }

    @Override
    public ByteBuffer asReadOnlyByteBuffer() {
        return byteString.asReadOnlyByteBuffer();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        byteString.writeTo(out);
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(byteString.asReadOnlyByteBuffer());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ByteStringPayload other) {
            return byteString.equals(other.byteString);
        }
        if (o instanceof Payload other) {
            if (this.size() != other.size()) return false;
            return byteString.equals(other.toByteString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return byteString.hashCode();
    }

    @Override
    public String toString() {
        return "ByteStringPayload[size=" + size() + "]";
    }
}

