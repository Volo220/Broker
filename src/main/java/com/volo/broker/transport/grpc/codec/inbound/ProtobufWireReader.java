package com.volo.broker.transport.grpc.codec.inbound;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Fast, low-overhead Protobuf wire format reader operating directly on an {@link InputStream}.
 */
public final class ProtobufWireReader {

    private final InputStream in;
    private int currentTag;

    public ProtobufWireReader(InputStream in) {
        this.in = Objects.requireNonNull(in, "in must not be null");
    }

    public InputStream inputStream() {
        return in;
    }

    /**
     * Reads the next field tag and returns its field number, or 0 if end-of-stream is reached cleanly.
     */
    public int nextFieldNumber() throws IOException {
        currentTag = readTag();
        return currentTag >>> 3;
    }

    /**
     * Returns the raw tag of the most recently read field.
     */
    public int currentTag() {
        return currentTag;
    }

    /**
     * Returns the wire type of the most recently read field.
     */
    public int currentWireType() {
        return currentTag & 0x07;
    }

    /**
     * Skips the current field based on its wire type.
     */
    public void skipCurrentField() throws IOException {
        skipField(currentTag);
    }

    /**
     * Reads a field tag (varint32) or returns 0 if end-of-stream is reached cleanly.
     */
    public int readTag() throws IOException {
        int first = in.read();
        if (first == -1) {
            return 0;
        }
        if ((first & 0x80) == 0) {
            return first;
        }
        return readTagContinuation(first);
    }

    private int readTagContinuation(int first) throws IOException {
        int result = first & 0x7F;
        int shift = 7;
        while (shift < 32) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("Unexpected EOF while reading tag");
            }
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new IOException("Tag is too long (> 5 bytes)");
    }

    /**
     * Reads a 32-bit varint.
     */
    public int readVarint32() throws IOException {
        return (int) readVarint64();
    }

    /**
     * Reads a 64-bit varint.
     */
    public long readVarint64() throws IOException {
        long result = 0;
        int shift = 0;
        while (shift < 64) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("Unexpected EOF while reading varint");
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new IOException("Varint is too long (> 10 bytes)");
    }

    /**
     * Reads raw bytes of specified length.
     */
    public byte[] readBytes(int length) throws IOException {
        if (length <= 0) {
            return new byte[0];
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length < length) {
            throw new EOFException("Unexpected EOF: expected " + length + " bytes, read " + bytes.length);
        }
        return bytes;
    }

    /**
     * Reads a UTF-8 string of specified byte length.
     */
    public String readString(int length) throws IOException {
        return new String(readBytes(length), StandardCharsets.UTF_8);
    }

    /**
     * Skips the specified number of bytes, looping until fully skipped or EOF.
     */
    public void skip(int length) throws IOException {
        if (length <= 0) {
            return;
        }
        long remaining = length;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) {
                if (in.read() == -1) {
                    throw new EOFException("Unexpected EOF while skipping bytes");
                }
                remaining--;
            } else {
                remaining -= skipped;
            }
        }
    }

    /**
     * Skips a field given its tag based on wire type.
     */
    public void skipField(int tag) throws IOException {
        int wireType = tag & 0x07;
        switch (wireType) {
            case 0 -> readVarint64();
            case 1 -> skip(8);
            case 2 -> skip(readVarint32());
            case 5 -> skip(4);
            default -> throw new IOException("Unsupported wire type: " + wireType);
        }
    }
}
