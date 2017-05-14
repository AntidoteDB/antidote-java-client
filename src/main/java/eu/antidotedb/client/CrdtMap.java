package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A homogeneous map with values of type V
 */
public class CrdtMap<K, V extends AntidoteCRDT> extends AntidoteCRDT {

    private final MapRef ref;
    private ValueCoder<K> keyCoder;
    private final CrdtCreator<V> valueCreator;

    private final Map<K, V> data = new LinkedHashMap<>();
    private final Set<K> removedKeys = new HashSet<>();

    public CrdtMap(MapRef ref, ValueCoder<K> keyCoder, CrdtCreator<V> valueCreator) {
        this.ref = ref;
        this.keyCoder = keyCoder;
        this.valueCreator = valueCreator;
    }


    @Override
    public ObjectRef getRef() {
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
            K keyValue = keyCoder.decode(keyBytes);
            V valueCrdt = data.get(keyValue);
            if (valueCrdt == null) {
                // create a new entry
                valueCrdt = valueCreator.create(ref, keyBytes);
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
        // TODO more efficient to collect updates in a special transaction and then build a single batch update?
        for (V v : data.values()) {
            v.push(tx);
        }
        List<AntidotePB.ApbMapKey> removedApbKeys = removedKeys.stream()
                .map(key -> AntidotePB.ApbMapKey.newBuilder()
                        .setType(valueCreator.type())
                        .setKey(keyCoder.encode(key))
                        .build())
                .collect(Collectors.toList());
        ref.removeKeys(tx, removedApbKeys);
        removedKeys.clear();
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
        return data.computeIfAbsent(key, k -> valueCreator.create(ref, keyCoder.encode(k)));
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


}
