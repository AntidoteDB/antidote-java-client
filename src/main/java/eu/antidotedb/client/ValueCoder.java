package eu.antidotedb.client;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A ValueCoder is used encode and decode values to and from ByteStrings.
 * In Antidote every value in registers and sets is stored as a ByteString, but in
 * Java it is often desirable to use other types to get more type safety and avoid
 * calling conversion functions all the time.
 * <p>
 * As an example consider the following UserId class:
 * <pre><code>
 * class UserId {
 *     private String id;
 *
 *     public UserId(String id) {
 *         this.id = id;
 *     }
 *
 *     public String getId() {
 *         return id;
 *     }
 *
 *     // hashCode, equals, etc.
 * }
 * </code></pre>
 * <p>
 * We can create a ValueCoder for this class using the static method {@link #stringCoder(Function, Function)}:
 * <pre><code>
 *     ValueCoder&lt;UserId&gt; userIdCoder = ValueCoder.stringCoder(UserId::getId, UserId::new);
 * </code></pre>
 * <p>
 * Then this ValueCoder can be used to get a key to a set of UserIds:
 * <pre><code>
 *     SetKey&lt;UserId&gt; userSet = Key.set("users", userIdCoder);
 * </code></pre>
 * <p>
 * Now the userSet can be updated and read without converting UserIds to a lower level type like ByteString:
 * <pre><code>
 *     UserId user1 = new UserId("user1");
 *     UserId user2 = new UserId("user2");
 *     // update the user set
 *     bucket.update(tx, userSet.addAll(user1, user2));
 *
 *     // read the user set
 *     List&lt;UserId&gt; value = bucket.read(tx, userSet);
 * </code></pre>
 *
 * @param <T> The target type for the encoding.
 */
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


    /**
     * A helper method to create a custom ValueCoder.
     *
     * @param toBytestring   A function to convert T to ByteString.
     * @param fromBytestring A function to convert a ByteString to T.
     * @param <T>            the target type for the coder
     * @return A coder for T.
     */
    static <T> ValueCoder<T> byteCoder(Function<T, ByteString> toBytestring, Function<ByteString, T> fromBytestring) {
        return new ValueCoder<T>() {
            @Override
            public ByteString encode(T value) {
                return toBytestring.apply(value);
            }

            @Override
            public T decode(ByteString bytes) {
                return fromBytestring.apply(bytes);
            }
        };
    }

    /**
     * A helper method to create a custom ValueCoder based on a string encoding.
     * Internally this will convert the strings to ByteStrings with utf8 encoding.
     *
     * @param toString   A function to convert T to String.
     * @param fromString A function to convert a String to T.
     * @param <T>        the target type for the coder
     * @return A coder for T.
     */
    static <T> ValueCoder<T> stringCoder(Function<T, String> toString, Function<String, T> fromString) {
        return new ValueCoder<T>() {
            @Override
            public ByteString encode(T value) {
                return ByteString.copyFromUtf8(toString.apply(value));
            }

            @Override
            public T decode(ByteString bytes) {
                return fromString.apply(bytes.toStringUtf8());
            }
        };
    }


}
