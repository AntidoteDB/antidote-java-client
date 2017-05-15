package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB.*;

import java.util.Arrays;
import java.util.List;

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
        return new MapReadResult<>(resp.getMap().getEntriesList(), keyCoder);
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

    public void removeKey(AntidoteTransaction tx, CRDT_type type, ByteString key) {
        removeKey(tx, ApbMapKey.newBuilder().setType(type).setKey(key).build());
    }

    public void removeKey(AntidoteTransaction tx, ApbMapKey key) {
        removeKeys(tx, Arrays.asList(key));
    }

    public void removeKeys(AntidoteTransaction tx, List<ApbMapKey> keys) {
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

        public ApbReadObjectResp getRaw(CRDT_type type, Key key) {
            return getRawFromByteString(type, keyCoder.encode(key));
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
            return getRaw(CRDT_type.COUNTER, key).getCounter().getValue();
        }

        public int fatCounter(Key key) {
            return getRaw(CRDT_type.FATCOUNTER, key).getCounter().getValue();
        }

        public long integer(Key key) {
            return getRaw(CRDT_type.INTEGER, key).getInt().getValue();
        }

        public <T> T register(Key key, ValueCoder<T> format) {
            return format.decode(getRaw(CRDT_type.LWWREG, key).getReg().getValue());
        }

        public <T> List<T> multiValueRegister(Key key, ValueCoder<T> format) {
            return format.decodeList(getRaw(CRDT_type.MVREG, key).getMvreg().getValuesList());
        }


        public <T> List<T> set(Key key, ValueCoder<T> format) {
            return format.decodeList(getRaw(CRDT_type.ORSET, key).getSet().getValueList());
        }

        public <T> List<T> set_removeWins(Key key, ValueCoder<T> format) {
            return format.decodeList(getRaw(CRDT_type.RWSET, key).getSet().getValueList());
        }

        public <K> MapReadResult<K> map_aw(Key key, ValueCoder<K> keyCoder) {
            List<ApbMapEntry> entries = getRaw(CRDT_type.AWMAP, key).getMap().getEntriesList();
            return new MapReadResult<>(entries, keyCoder);
        }

        public <K> MapReadResult<K> map_rr(Key key, ValueCoder<K> keyCoder) {
            List<ApbMapEntry> entries = getRaw(CRDT_type.RRMAP, key).getMap().getEntriesList();
            return new MapReadResult<>(entries, keyCoder);
        }

        public <K> MapReadResult<K> map_g(Key key, ValueCoder<K> keyCoder) {
            List<ApbMapEntry> entries = getRaw(CRDT_type.GMAP, key).getMap().getEntriesList();
            return new MapReadResult<>(entries, keyCoder);
        }

    }

}
