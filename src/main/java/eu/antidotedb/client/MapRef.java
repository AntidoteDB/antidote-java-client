package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The Class LowLevelMap.
 * <p>
 * TODO add alternatives for homogeneous maps and maps that are used like structs
 */
public class MapRef<Key> extends ObjectRef<MapRef.MapReadResult<Key>> implements CrdtContainer<Key> {

    private ValueCoder<Key> keyCoder;

    /**
     * Instantiates a new low level map.
     */
    MapRef(CrdtContainer<?> container, ByteString key, CRDT_type type, ValueCoder<Key> keyCoder) {
        super(container, key, type);
        this.keyCoder = keyCoder;
    }


    @Override
    MapReadResult<Key> readResponseToValue(ApbReadObjectResp resp) {
        return ResponseDecoder.map(keyCoder).readResponseToValue(resp);
    }

    @Override
    public ApbReadObjectResp read(TransactionWithReads tx, CRDT_type type, ByteString key) {
        MapReadResult<Key> res = read(tx);
        return res.getRawFromByteString(type, key);
    }

    @Override
    public BatchReadResult<ApbReadObjectResp> readBatch(BatchRead tx, CRDT_type type, ByteString key) {
        BatchReadResult<MapReadResult<Key>> res = read(tx);
        return res.map(r -> r.getRawFromByteString(type, key));
    }

    @Override
    public void update(AntidoteTransaction tx, CRDT_type type, ByteString key, ApbUpdateOperation.Builder operation) {
        ApbMapUpdate.Builder mapUpdate = ApbMapUpdate.newBuilder();
        ApbMapNestedUpdate.Builder nestedUpdate = ApbMapNestedUpdate.newBuilder();
        nestedUpdate.setKey(ApbMapKey.newBuilder().setType(type).setKey(key));
        nestedUpdate.setUpdate(operation);
        mapUpdate.addUpdates(nestedUpdate);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdate);
        getContainer().update(tx, getType(), getKey(), updateOperation);
    }

    @Override
    public ValueCoder<Key> keyCoder() {
        return keyCoder;
    }

    /**
     * Removes a key from the map
     */
    public void removeKey(AntidoteTransaction tx, CRDT_type type, ByteString key) {
        removeKey(tx, ApbMapKey.newBuilder().setType(type).setKey(key).build());
    }

    /**
     * Removes a key from the map
     */
    public void removeKey(AntidoteTransaction tx, ApbMapKey key) {
        removeApbKeys(tx, Arrays.asList(key));
    }

    /**
     * Removes a key from the map
     */
    public void removeKey(AntidoteTransaction tx, MapKey<Key> key) {
        removeKeys(tx, Arrays.asList(key));
    }

    /**
     * Removes several keys from the map
     */
    public void removeKeys(AntidoteTransaction tx, Iterable<MapKey<Key>> keys) {
        Stream<ApbMapKey> collect = StreamSupport.stream(keys.spliterator(), false).map(k -> k.toApb(keyCoder));
        removeApbKeys(tx, collect::iterator);
    }

    /**
     * Removes several keys from the map
     */
    public void removeApbKeys(AntidoteTransaction tx, Iterable<ApbMapKey> keys) {
        ApbMapUpdate.Builder mapUpdate = ApbMapUpdate.newBuilder();
        mapUpdate.addAllRemovedKeys(keys);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setMapop(mapUpdate);
        getContainer().update(tx, getType(), getKey(), updateOperation);
    }

    public CrdtMapDynamic<Key> toMutable() {
        return new CrdtMapDynamic<>(this);
    }

    public <V extends AntidoteCRDT> CrdtMap<Key, V> toMutable(CrdtCreator<V> valueCreator) {
        return new CrdtMap<>(this, keyCoder, valueCreator);
    }


    public static class MapReadResult<Key> {
        private List<ApbMapEntry> entries;
        private ValueCoder<Key> keyCoder;

        MapReadResult(List<ApbMapEntry> entriesList, ValueCoder<Key> keyCoder) {
            entries = entriesList;
            this.keyCoder = keyCoder;
        }

        /**
         * Returns the set of keys contained in this map (discarding CRDT types in the keys)
         */
        public Set<Key> keySet() {
            return entries.stream().map(e -> keyCoder.decode(e.getKey().getKey())).collect(Collectors.toSet());
        }

        /**
         * Returns the set of keys contained in the map
         */
        public Set<MapKey<Key>> mapKeySet() {
            return entries.stream()
                    .map(e -> {
                        CRDT_type t = e.getKey().getType();
                        Key k = keyCoder.decode(e.getKey().getKey());
                        return new MapKey<>(t, k);
                    })
                    .collect(Collectors.toSet());
        }

        /**
         * Returns the set of keys contained in the map (CRDT type + raw ByteString)
         */
        public Set<ApbMapKey> mapKeySetRaw() {
            return entries.stream().map(ApbMapEntry::getKey).collect(Collectors.toSet());
        }

        /**
         * Returns an unmodifiable Collection of the raw entries in this map.
         */
        public Collection<ApbMapEntry> entries() {
            return Collections.unmodifiableCollection(entries);
        }

        /**
         * Converts this MapReadResult to a map.
         * Assumes that the map only contains entries with the same value type.
         *
         * @param nested  an ObjectRef which acts as an example for all values in the.
         * @param <Value> the type of values in the map
         * @return the MapReadResult represented as a Java Map
         */
        public <Value> Map<Key, Value> asJavaMap(ResponseDecoder<Value> nested) {
            LinkedHashMap<Key, Value> res = new LinkedHashMap<>();
            for (ApbMapEntry entry : entries) {
                Key key = keyCoder.decode(entry.getKey().getKey());
                Value val = nested.readResponseToValue(entry.getValue());
                res.put(key, val);
            }
            return res;
        }


        public ApbReadObjectResp getRaw(CRDT_type type, Key key) {
            return getRawFromByteString(type, keyCoder.encode(key));
        }

        public <Value> Optional<Value> get(MapKey<Key> key, ResponseDecoder<Value> value) {
            ApbReadObjectResp resp = getRaw(key.getType(), key.getKey());
            if (resp == null) {
                return Optional.empty();
            }
            return Optional.of(value.readResponseToValue(resp));
        }

        public ApbReadObjectResp getRawFromByteString(CRDT_type type, ByteString key) {
            for (ApbMapEntry entry : entries) {
                if (entry.getKey().getType().equals(type)
                        && entry.getKey().getKey().equals(key)) {
                    return entry.getValue();
                }
            }
            return null;
        }

        public int counter(Key key) {
            return get(MapKey.counter(key), ResponseDecoder.counter()).orElse(0);
        }

        public int fatCounter(Key key) {
            return get(MapKey.fatCounter(key), ResponseDecoder.counter()).orElse(0);
        }

        public long integer(Key key) {
            return get(MapKey.integer(key), ResponseDecoder.integer()).orElse(0L);
        }

        public <T> T register(Key key, ValueCoder<T> format) {
            return get(MapKey.register(key), ResponseDecoder.register(format)).orElse(null);
        }

        public <T> List<T> multiValueRegister(Key key, ValueCoder<T> format) {
            return get(MapKey.multiValueRegister(key), ResponseDecoder.multiValueRegister(format))
                    .orElse(Collections.emptyList());
        }


        public <T> List<T> set(Key key, ValueCoder<T> format) {
            return get(MapKey.set(key), ResponseDecoder.set(format))
                    .orElse(Collections.emptyList());
        }

        public <T> List<T> set_removeWins(Key key, ValueCoder<T> format) {
            return get(MapKey.set_removeWins(key), ResponseDecoder.set(format))
                    .orElse(Collections.emptyList());
        }

        public <K> MapReadResult<K> map_aw(Key key, ValueCoder<K> keyCoder) {
            return get(MapKey.map_aw(key), ResponseDecoder.map(keyCoder))
                    .orElseGet(() -> new MapReadResult<>(Collections.emptyList(), keyCoder));
        }

        public <K> MapReadResult<K> map_rr(Key key, ValueCoder<K> keyCoder) {
            return get(MapKey.map_rr(key), ResponseDecoder.map(keyCoder))
                    .orElseGet(() -> new MapReadResult<>(Collections.emptyList(), keyCoder));
        }

        public <K> MapReadResult<K> map_g(Key key, ValueCoder<K> keyCoder) {
            return get(MapKey.map_g(key), ResponseDecoder.map(keyCoder))
                    .orElseGet(() -> new MapReadResult<>(Collections.emptyList(), keyCoder));
        }

    }

}
