package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

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


    public static <Key> MapKey<Key> counter(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.COUNTER, key);
    }

    public static <Key> MapKey<Key> fatCounter(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.FATCOUNTER, key);
    }

    public static <Key> MapKey<Key> integer(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.INTEGER, key);
    }


    public static <Key> MapKey<Key> register(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.LWWREG, key);
    }


    public static <Key> MapKey<Key> multiValueRegister(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.MVREG, key);
    }

    public static <Key> MapKey<Key> set(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.ORSET, key);
    }

    public static <Key> MapKey<Key> set_removeWins(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.RWSET, key);
    }

    public static <Key> MapKey<Key> map_aw(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.AWMAP, key);
    }

    public static <Key> MapKey<Key> map_rr(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.RRMAP, key);
    }

    public static <Key> MapKey<Key> map_g(Key key) {
        return new MapKey<>(AntidotePB.CRDT_type.GMAP, key);
    }


}