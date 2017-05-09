package eu.antidotedb.client;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.stream.Collectors;

public interface ValueCoder<T> {

    ByteString encode(T value);

    T decode(ByteString bytes);

    default List<T> decodeList(List<ByteString> byteStringList) {
        return byteStringList.stream().map(this::decode).collect(Collectors.toList());
    }

    default List<ByteString> encodeList(List<T> valueList) {
        return valueList.stream().map(this::encode).collect(Collectors.toList());
    }


    /**
     * Stores Strings in utf8 encoding
     */
    ValueCoder<String> utf8String = new ValueCoder<String>() {
        @Override
        public ByteString encode(String value) {
            return ByteString.copyFromUtf8(value);
        }

        @Override
        public String decode(ByteString bytes) {
            return bytes.toStringUtf8();
        }
    };

    /**
     * Stores plain ByteStrings
     * <p>
     * This is the identity-coder
     */
    ValueCoder<ByteString> bytestringEncoder = new ValueCoder<ByteString>() {
        @Override
        public ByteString encode(ByteString value) {
            return value;
        }

        @Override
        public ByteString decode(ByteString bytes) {
            return bytes;
        }

        // optimized versions for encoding/decoding lists:
        @Override
        public List<ByteString> encodeList(List<ByteString> valueList) {
            return valueList;
        }

        @Override
        public List<ByteString> decodeList(List<ByteString> byteStringList) {
            return byteStringList;
        }
    };


}