package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;

import java.util.Objects;

/**
 * A key in a map
 */
public class MapKey<K> {
    private final AntidotePB.CRDT_type type;
    private final K key;

    public MapKey(AntidotePB.CRDT_type type, K key) {
        this.type = type;
        this.key = key;
    }

    public MapKey(AntidotePB.ApbMapKey key, ValueCoder<K> keyCoder) {
        this(key.getType(), keyCoder.decode(key.getKey()));
    }

    public AntidotePB.CRDT_type getType() {
        return type;
    }

    public K getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapKey<?> mapKey = (MapKey<?>) o;
        return type == mapKey.type &&
                Objects.equals(key, mapKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }

    AntidotePB.ApbMapKey toApb(ValueCoder<K> keyCoder) {
        return AntidotePB.ApbMapKey.newBuilder()
                .setKey(keyCoder.encode(key))
                .setType(type)
                .build();
    }
}