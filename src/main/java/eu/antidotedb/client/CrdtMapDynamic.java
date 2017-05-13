package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbReadObjectResp;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A homogeneous map with values of type V
 */
public class CrdtMapDynamic<K> extends AntidoteCRDT {

    private final MapRef ref;
    private ValueCoder<K> keyCoder;

    /**
     * the Object value is either an AntidoteCRDT or the last read response
     **/
    private final Map<MapKey<K>, Object> data = new LinkedHashMap<>();
    private final Set<MapKey<K>> removedKeys = new HashSet<>();



    public CrdtMapDynamic(MapRef ref, ValueCoder<K> keyCoder) {
        this.ref = ref;
        this.keyCoder = keyCoder;
    }


    @Override
    public ObjectRef getRef() {
        return ref;
    }

    @Override
    void updateFromReadResponse(ApbReadObjectResp object) {
        List<AntidotePB.ApbMapEntry> entries = object.getMap().getEntriesList();
        Set<MapKey<K>> toRemove = new LinkedHashSet<>(data.keySet());
        for (AntidotePB.ApbMapEntry entry : entries) {
            AntidotePB.ApbMapKey key = entry.getKey();
            ApbReadObjectResp value = entry.getValue();
            MapKey<K> mapKey = new MapKey<>(key, keyCoder);

            Object mapValue = data.get(mapKey);
            if (mapValue != null) {
                // don't remove this value
                toRemove.remove(mapKey);
            }
            if (mapValue instanceof AntidoteCRDT) {
                AntidoteCRDT crdt = (AntidoteCRDT) mapValue;
                // update existing CRDT:
                crdt.updateFromReadResponse(value);
            } else {
                // store value for later use
                data.put(mapKey, value);
            }
        }
        for (MapKey<K> key : toRemove) {
            data.remove(key);
        }
    }


    @Override
    public void push(AntidoteTransaction tx) {
        // TODO more efficient to collect updates in a special transaction and then build a single batch update?
        for (Object v : data.values()) {
            if (v instanceof AntidoteCRDT) {
                ((AntidoteCRDT) v).push(tx);
            }
        }
        List<AntidotePB.ApbMapKey> removedApbKeys = removedKeys.stream()
                .map(key -> key.toApb(keyCoder))
                .collect(Collectors.toList());
        ref.removeKeys(tx, removedApbKeys);
        removedKeys.clear();
    }

    /**
     * Removes an entry from the map
     */
    public void removeKey(MapKey<K> key) {
        data.remove(key);
        removedKeys.add(key);
    }


    /**
     * gets an entry from the map.
     * <p>
     * If the entry does not exist, a new one is created
     */
    public <V extends AntidoteCRDT> V get(K key, CrdtCreator<V> valueCreator) {
        MapKey<K> mapKey = new MapKey<>(valueCreator.type(), key);
        Object value = data.get(mapKey);
        if (value instanceof AntidoteCRDT) {
            return valueCreator.cast(((AntidoteCRDT) value));
        } else {
            V crdt = valueCreator.create(ref, keyCoder.encode(mapKey.getKey()));
            if (value instanceof ApbReadObjectResp) {
                // if we have a cached value, use it to update
                crdt.updateFromReadResponse(((ApbReadObjectResp) value));
            }
            return crdt;
        }
    }

    /**
     * Checks if an entry is in the map
     */
    public boolean containsKey(MapKey<K> key) {
        return data.containsKey(key);
    }

    /**
     * Returns an unmodifiable view of all keys in the map
     */
    public Set<MapKey<K>> keySet() {
        return Collections.unmodifiableSet(data.keySet());
    }

    // TODO inline or rename the functions below

    public CrdtCounter getCounterEntry(K key) {
        return get(key, CrdtCounter.creator());
    }

    public CrdtInteger getIntegerEntry(K key) {
        return get(key, CrdtInteger.creator());
    }

    public <V> CrdtSet<V> getORSetEntry(K key, ValueCoder<V> valueCoder) {
        return get(key, CrdtSet.creator(valueCoder));
    }

    public <V> CrdtSet<V> getRWSetEntry(K key, ValueCoder<V> valueCoder) {
        return get(key, CrdtSet.creatorRemoveWins(valueCoder));
    }

    public <V> CrdtRegister<V> getLWWRegisterEntry(K key, ValueCoder<V> valueCoder) {
        return get(key, CrdtRegister.creator(valueCoder));
    }

    public <V> CrdtMVRegister<V> getMVRegisterEntry(K key, ValueCoder<V> valueCoder) {
        return get(key, CrdtMVRegister.creator(valueCoder));
    }

    public CrdtMapDynamic<K> getAWMapEntry(K key) {
        return get(key, CrdtMapDynamic.creator_aw(keyCoder));
    }

    public <K2> CrdtMapDynamic<K2> getAWMapEntry(K key, ValueCoder<K2> keyCoder) {
        return get(key, CrdtMapDynamic.creator_aw(keyCoder));
    }

    public CrdtMapDynamic<K> getGMapEntry(K key) {
        return get(key, creatorGrowOnly(keyCoder));
    }

    public <K2> CrdtMapDynamic<K2> getGMapEntry(K key, ValueCoder<K2> keyCoder) {
        return get(key, creatorGrowOnly(keyCoder));
    }

    public static <K2> CrdtCreator<CrdtMapDynamic<K2>> creator_aw(ValueCoder<K2> keyCoder) {
        return new CrdtCreator<CrdtMapDynamic<K2>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.AWMAP;
            }

            @Override
            public CrdtMapDynamic<K2> create(CrdtContainer c, ByteString key) {
                return c.map_aw(key).getMutable(keyCoder);
            }

            @Override
            public CrdtMapDynamic<K2> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return (CrdtMapDynamic<K2>) value;
            }
        };
    }

    public static <K2> CrdtCreator<CrdtMapDynamic<K2>> creatorGrowOnly(ValueCoder<K2> keyCoder) {
        return new CrdtCreator<CrdtMapDynamic<K2>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.GMAP;
            }

            @Override
            public CrdtMapDynamic<K2> create(CrdtContainer c, ByteString key) {
                return c.map_g(key).getMutable(keyCoder);
            }

            @Override
            public CrdtMapDynamic<K2> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return (CrdtMapDynamic<K2>) value;
            }
        };
    }


}
