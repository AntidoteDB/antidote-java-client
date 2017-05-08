package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import java.util.Objects;

/**
 * The Class AntidoteMapKey.
 */
public final class AntidoteMapKey implements Comparable<AntidoteMapKey> {

    /**
     * The key.
     */
    private final ByteString key;

    /**
     * The type.
     */
    private final CRDT_type type;

    /**
     * The apb key.
     */
    private final ApbMapKey apbKey;

    /**
     * Instantiates a new antidote map key.
     *
     * @param type the type
     * @param key  the key
     */
    public AntidoteMapKey(CRDT_type type, String key) {
        this.key = ByteString.copyFromUtf8(key);
        this.type = type;
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setKey(this.key);
        apbKeyBuilder.setType(type);
        this.apbKey = apbKeyBuilder.build();
    }

    /**
     * Instantiates a new antidote map key.
     *
     * @param type the type
     * @param key  the key
     */
    public AntidoteMapKey(CRDT_type type, ByteString key) {
        this.key = key;
        this.type = type;
        ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
        apbKeyBuilder.setKey(key);
        apbKeyBuilder.setType(type);
        this.apbKey = apbKeyBuilder.build();
    }

    /**
     * Instantiates a new antidote map key.
     *
     * @param key the key
     */
    protected AntidoteMapKey(ApbMapKey key) {
        this.key = key.getKey();
        this.type = key.getType();
        this.apbKey = key;
    }

    /**
     * Gets the key.
     *
     * @return the key
     */
    public String getKey() {
        return key.toStringUtf8();
    }

    /**
     * Gets the key as ByteString.
     *
     * @return the key BS
     */
    public ByteString getKeyBS() {
        return key;
    }

    /**
     * Gets the apb key.
     *
     * @return the apb key
     */
    protected ApbMapKey getApbKey() {
        return apbKey;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public CRDT_type getType() {
        return type;
    }

    @Override
    public int compareTo(AntidoteMapKey other) {
        int r = type.compareTo(other.type);
        if (r != 0) {
            return r;
        }
        int minSize = Math.min(key.size(), other.key.size());
        for (int i = 0; i < minSize; i++) {
            if (key.byteAt(i) < other.key.byteAt(i)) {
                return -1;
            } else if (key.byteAt(i) > other.key.byteAt(i)) {
                return 1;
            }
        }
        return Integer.compare(key.size(), other.key.size());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AntidoteMapKey) {
            AntidoteMapKey other = (AntidoteMapKey) o;

            return Objects.equals(type, other.type)
                    && Objects.equals(key, other.key);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }
}
