package com.volo.broker.broker.message;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Payload {

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }

    ByteString toByteString();

    byte[] toByteArray();

    ByteBuffer asReadOnlyByteBuffer();

    void writeTo(OutputStream out) throws IOException;

    void writeTo(ByteBuffer buffer);

    static Payload empty() {
        return ByteStringPayload.EMPTY;
    }

    static Payload of(ByteString byteString) {
        if (byteString == null || byteString.isEmpty()) {
            return ByteStringPayload.EMPTY;
        }
        return new ByteStringPayload(byteString);
    }

    static Payload of(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return ByteStringPayload.EMPTY;
        }
        return new ByteArrayPayload(bytes);
    }

    static Payload wrap(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return ByteStringPayload.EMPTY;
        }
        return new ByteArrayPayload(bytes, true);
    }
}

