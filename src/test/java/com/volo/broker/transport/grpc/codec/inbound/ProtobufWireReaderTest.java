package com.volo.broker.transport.grpc.codec.inbound;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtobufWireReaderTest {

    @Test
    @DisplayName("readTag returns 0 on empty stream")
    void readTag_shouldReturnZeroOnEof() throws IOException {
        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(new byte[0]));
        assertThat(reader.readTag()).isZero();
    }

    @Test
    @DisplayName("readTag correctly reads single-byte and multi-byte tags")
    void readTag_shouldReadValidTags() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeTag(1, 2); // field 1, wire 2 -> 10 (0x0A)
        protoOut.writeTag(150, 0); // multi-byte tag
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(reader.readTag()).isEqualTo((1 << 3) | 2);
        assertThat(reader.readTag()).isEqualTo((150 << 3) | 0);
        assertThat(reader.readTag()).isZero();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 300, 16384, Integer.MAX_VALUE})
    @DisplayName("readVarint32 reads values matching protobuf specification")
    void readVarint32_shouldMatchProtobuf(int value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeUInt32NoTag(value);
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(reader.readVarint32()).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 127L, 128L, 1000000L, 1L << 40, Long.MAX_VALUE})
    @DisplayName("readVarint64 reads values matching protobuf specification")
    void readVarint64_shouldMatchProtobuf(long value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeUInt64NoTag(value);
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(reader.readVarint64()).isEqualTo(value);
    }

    @Test
    @DisplayName("readVarint throws EOFException on incomplete varint")
    void readVarint_shouldThrowEofOnIncompleteBytes() {
        // MSB set indicating more bytes, but stream ends
        byte[] incomplete = new byte[]{(byte) 0x80};
        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(incomplete));

        assertThatThrownBy(reader::readVarint32).isInstanceOf(EOFException.class);
    }

    @Test
    @DisplayName("readString correctly reads UTF-8 characters")
    void readString_shouldReadUtf8String() throws IOException {
        String original = "Hello Слава Україні! 🚀";
        byte[] bytes = original.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(bytes));
        assertThat(reader.readString(bytes.length)).isEqualTo(original);
    }

    @Test
    @DisplayName("readString throws EOFException if stream ends early")
    void readString_shouldThrowOnPrematureEof() {
        byte[] bytes = new byte[]{1, 2, 3};
        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(bytes));

        assertThatThrownBy(() -> reader.readString(10)).isInstanceOf(EOFException.class);
    }

    @Test
    @DisplayName("skipField safely skips wire types 0, 1, 2, and 5")
    void skipField_shouldSkipAllStandardWireTypes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeInt64(1, 123456789L); // wire 0
        protoOut.writeFixed64(2, 987654321L); // wire 1
        protoOut.writeString(3, "test-string"); // wire 2
        protoOut.writeFixed32(4, 42); // wire 5
        protoOut.writeInt32(5, 777); // target to read
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));
        // Skip field 1
        reader.skipField(reader.readTag());
        // Skip field 2
        reader.skipField(reader.readTag());
        // Skip field 3
        reader.skipField(reader.readTag());
        // Skip field 4
        reader.skipField(reader.readTag());

        // Field 5 should be next
        int tag = reader.readTag();
        assertThat(tag >>> 3).isEqualTo(5);
        assertThat(reader.readVarint32()).isEqualTo(777);
    }

    @Test
    @DisplayName("skipField throws IOException on invalid wire type")
    void skipField_shouldThrowOnInvalidWireType() {
        // Tag with wire type 7
        int invalidTag = (1 << 3) | 7;
        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(new byte[10]));

        assertThatThrownBy(() -> reader.skipField(invalidTag))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported wire type");
    }

    @Test
    @DisplayName("nextFieldNumber extracts field number and updates current tag and wire type")
    void nextFieldNumber_shouldExtractFieldNumberAndCurrentTag() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeInt64(1, 100L); // field 1, wire 0
        protoOut.writeString(7, "hello"); // field 7, wire 2
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));

        assertThat(reader.nextFieldNumber()).isEqualTo(1);
        assertThat(reader.currentTag()).isEqualTo((1 << 3) | 0);
        assertThat(reader.currentWireType()).isZero();
        assertThat(reader.readVarint64()).isEqualTo(100L);

        assertThat(reader.nextFieldNumber()).isEqualTo(7);
        assertThat(reader.currentTag()).isEqualTo((7 << 3) | 2);
        assertThat(reader.currentWireType()).isEqualTo(2);
        assertThat(reader.readString(reader.readVarint32())).isEqualTo("hello");

        assertThat(reader.nextFieldNumber()).isZero();
    }

    @Test
    @DisplayName("skipCurrentField skips fields without explicitly passing tag")
    void skipCurrentField_shouldSkipCurrentField() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CodedOutputStream protoOut = CodedOutputStream.newInstance(out);
        protoOut.writeInt64(1, 999L);
        protoOut.writeString(2, "skip-me");
        protoOut.writeInt32(3, 42);
        protoOut.flush();

        ProtobufWireReader reader = new ProtobufWireReader(new ByteArrayInputStream(out.toByteArray()));

        assertThat(reader.nextFieldNumber()).isEqualTo(1);
        reader.skipCurrentField();

        assertThat(reader.nextFieldNumber()).isEqualTo(2);
        reader.skipCurrentField();

        assertThat(reader.nextFieldNumber()).isEqualTo(3);
        assertThat(reader.readVarint32()).isEqualTo(42);
        assertThat(reader.nextFieldNumber()).isZero();
    }
}
