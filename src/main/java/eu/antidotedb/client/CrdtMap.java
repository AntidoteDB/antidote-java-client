package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A homogeneous map with values of type V
 */
public class CrdtMap<K, V extends AntidoteCRDT> extends AntidoteCRDT {

    private final MapRef<K> ref;
    private final CrdtCreator<V> valueCreator;

    private final Map<K, V> data = new LinkedHashMap<>();
    private final Set<K> removedKeys = new HashSet<>();

    public CrdtMap(MapRef<K> ref, ValueCoder<K> keyCoder, CrdtCreator<V> valueCreator) {
        this.ref = ref;
        this.valueCreator = valueCreator;
    }


    @Override
    public MapRef<K> getRef() {
        return ref;
    }

    @Override
    void updateFromReadResponse(AntidotePB.ApbReadObjectResp object) {
        List<AntidotePB.ApbMapEntry> entries = object.getMap().getEntriesList();
        Set<K> toRemove = new LinkedHashSet<>(data.keySet());
        for (AntidotePB.ApbMapEntry entry : entries) {
            AntidotePB.ApbMapKey key = entry.getKey();
            if (key.getType() != valueCreator.type()) {
                // ignore other types
                continue;
            }
            AntidotePB.ApbReadObjectResp value = entry.getValue();
            ByteString keyBytes = key.getKey();
            K keyValue = ref.keyCoder().decode(keyBytes);
            V valueCrdt = data.get(keyValue);
            if (valueCrdt == null) {
                // newTransformer a new entry
                valueCrdt = valueCreator.create(ref, keyValue);
                data.put(keyValue, valueCrdt);
            } else {
                // don't remove entry:
                toRemove.remove(keyValue);
            }
            // update entry with new value
            valueCrdt.updateFromReadResponse(value);
        }
        for (K key : toRemove) {
            data.remove(key);
        }
    }


    @Override
    public void push(AntidoteTransaction tx) {
        // collect all updates before applying them:
        List<AntidotePB.ApbUpdateOp.Builder> updates = new ArrayList<>();
        AntidoteStaticTransaction tempTx = new AntidoteStaticTransaction(null);

        for (V v : data.values()) {
            v.push(tempTx);
        }
        List<AntidotePB.ApbMapKey> removedApbKeys = removedKeys.stream()
                .map(key -> AntidotePB.ApbMapKey.newBuilder()
                        .setType(valueCreator.type())
                        .setKey(ref.keyCoder().encode(key))
                        .build())
                .collect(Collectors.toList());
        if (!removedKeys.isEmpty()) {
            ref.removeKeys(tempTx, removedApbKeys);
            removedKeys.clear();
        }
        tx.performUpdates(tempTx.getTransactionUpdateList());
    }

    /**
     * Removes an entry from the map
     */
    public void removeKey(K key) {
        data.remove(key);
        removedKeys.add(key);
    }


    /**
     * gets an entry from the map.
     * <p>
     * If the entry does not exist, a new one is created
     */
    public V get(K key) {
        return data.computeIfAbsent(key, k -> valueCreator.create(ref, k));
    }

    /**
     * Checks if an entry is in the map
     */
    public boolean containsKey(K key) {
        return data.containsKey(key);
    }

    /**
     * Returns an unmodifiable view of all keys in the map
     */
    public Set<K> keySet() {
        return Collections.unmodifiableSet(data.keySet());
    }

    /**
     * Returns an unmodifiable view of all entries in the map
     */
    public Set<Map.Entry<K, V>> entries() {
        return Collections.unmodifiableSet(data.entrySet());
    }


    public static <K, V extends AntidoteCRDT> CrdtCreator<CrdtMap<K, V>> creator_aw(ValueCoder<K> keyCoder, CrdtCreator<V> crdtCreator) {
        return new CrdtCreator<CrdtMap<K, V>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.AWMAP;
            }

            @Override
            public <K1> CrdtMap<K, V> create(CrdtContainer<K1> c, K1 key) {
                return c.map_aw(key, keyCoder).toMutable(crdtCreator);
            }

            @Override
            public CrdtMap<K, V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return ((CrdtMap<K, V>) value);
            }
        };
    }

    public static <K, V extends AntidoteCRDT> CrdtCreator<CrdtMap<K, V>> creator_g(ValueCoder<K> keyCoder, CrdtCreator<V> crdtCreator) {
        return new CrdtCreator<CrdtMap<K, V>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.GMAP;
            }

            @Override
            public <K1> CrdtMap<K, V> create(CrdtContainer<K1> c, K1 key) {
                return c.map_g(key, keyCoder).toMutable(crdtCreator);
            }

            @Override
            public CrdtMap<K, V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return ((CrdtMap<K, V>) value);
            }
        };
    }

    public static <K, V extends AntidoteCRDT> CrdtCreator<CrdtMap<K, V>> creator_rr(ValueCoder<K> keyCoder, CrdtCreator<V> crdtCreator) {
        return new CrdtCreator<CrdtMap<K, V>>() {
            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.RRMAP;
            }

            @Override
            public <K1> CrdtMap<K, V> create(CrdtContainer<K1> c, K1 key) {
                return c.map_rr(key, keyCoder).toMutable(crdtCreator);
            }

            @Override
            public CrdtMap<K, V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return ((CrdtMap<K, V>) value);
            }
        };
    }

}
