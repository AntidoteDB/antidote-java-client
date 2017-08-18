package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.*;

public class MapKey extends Key<MapKey.MapReadResult> {


    MapKey(AntidotePB.CRDT_type type, ByteString key) {
        super(type, key);
    }

    @Override
    MapReadResult readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.map().readResponseToValue(resp);
    }

    @CheckReturnValue
    public InnerUpdateOp update(InnerUpdateOp... keyUpdates) {
        return update(Arrays.asList(keyUpdates));
    }

    @CheckReturnValue
    public InnerUpdateOp update(Iterable<InnerUpdateOp> keyUpdates) {
        return new InnerMapUpdateOp().update(keyUpdates);
    }

    @CheckReturnValue
    public InnerUpdateOp removeKeys(Key<?>... keys) {
        return removeKeys(Arrays.asList(keys));
    }

    @CheckReturnValue
    public InnerUpdateOp removeKeys(Iterable<? extends Key<?>> keys) {
        return new InnerMapUpdateOp().removeKeys(keys);
    }


    class InnerMapUpdateOp extends InnerUpdateOp {
        Set<Key<?>> changedKeys = new HashSet<>();
        AntidotePB.ApbMapUpdate.Builder op = AntidotePB.ApbMapUpdate.newBuilder();

        public InnerMapUpdateOp() {
            super(MapKey.this);
        }


        @Override
        AntidotePB.ApbUpdateOperation.Builder getApbUpdate() {
            AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
            updateOperation.setMapop(op);
            return updateOperation;
        }

        public InnerUpdateOp update(InnerUpdateOp... keyUpdates) {
            return update(Arrays.asList(keyUpdates));
        }


        public InnerUpdateOp update(Iterable<InnerUpdateOp> keyUpdates) {
            for (InnerUpdateOp keyUpdate : keyUpdates) {
                if (!changedKeys.add(keyUpdate.getKey())) {
                    throw new AntidoteException("Key " + keyUpdate.getKey() + " is already changed in this map update.");
                }
                op.addUpdates(keyUpdate.toApbNestedUpdate());
            }
            return this;
        }

        public InnerUpdateOp removeKeys(Key<?>... keys) {
            return removeKeys(Arrays.asList(keys));
        }

        public InnerUpdateOp removeKeys(Iterable<? extends Key<?>> keys) {
            for (Key<?> key : keys) {
                if (!changedKeys.add(key)) {
                    throw new AntidoteException("Key " + key + " is already changed in this map update.");
                }
                op.addRemovedKeys(key.toApbMapKey());
            }
            return this;
        }
    }


    public static class MapReadResult extends AbstractMap<Key<?>, Object> {
        private Map<Key<?>, AntidotePB.ApbReadObjectResp> responses = new LinkedHashMap<>();


        MapReadResult(List<AntidotePB.ApbMapEntry> entriesList) {
            for (AntidotePB.ApbMapEntry entry : entriesList) {
                AntidotePB.ApbMapKey key = entry.getKey();
                responses.put(Key.fromApbMapKey(key), entry.getValue());
            }
        }



        @Override
        public Object get(Object key) {
            if (key instanceof Key<?>) {
                return get((Key<?>) key);
            }
            return null;
        }

        public <V> V get(Key<V> key) {
            AntidotePB.ApbReadObjectResp resp = responses.get(key);
            return key.readResponseToValue(resp);
        }

        @Override
        public boolean containsKey(Object key) {
            return responses.containsKey(key);
        }

        @Override
        public int size() {
            return responses.size();
        }

        @Override
        public Set<Key<?>> keySet() {
            return Collections.unmodifiableSet(responses.keySet());
        }

        @Override
        public Set<Entry<Key<?>, Object>> entrySet() {
            return new AbstractSet<Entry<Key<?>, Object>>() {
                @Override
                public Iterator<Entry<Key<?>, Object>> iterator() {
                    Iterator<Entry<Key<?>, AntidotePB.ApbReadObjectResp>> it = responses.entrySet().iterator();
                    return new Iterator<Entry<Key<?>, Object>>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<Key<?>, Object> next() {
                            Entry<Key<?>, AntidotePB.ApbReadObjectResp> entry = it.next();
                            return new Entry<Key<?>, Object>() {
                                @Override
                                public Key<?> getKey() {
                                    return entry.getKey();
                                }

                                @Override
                                public Object getValue() {
                                    return entry.getKey().readResponseToValue(entry.getValue());
                                }

                                @Override
                                public Object setValue(Object value) {
                                    throw new UnsupportedOperationException("The MapReadResult cannot be changed.");
                                }
                            };
                        }
                    };
                }

                @Override
                public int size() {
                    return responses.size();
                }

            };
        }

        public <V> Map<String, V> asJavaMap(ResponseDecoder<V> responseDecoder) {
            return asJavaMap(ValueCoder.utf8String, responseDecoder);
        }

        public <K,V> Map<K, V> asJavaMap(ValueCoder<K> keyCoder, ResponseDecoder<V> responseDecoder) {
            LinkedHashMap<K, V> res = new LinkedHashMap<>();
            for (Entry<Key<?>, AntidotePB.ApbReadObjectResp> entry : responses.entrySet()) {
                res.put(keyCoder.decode(entry.getKey().getKey()), responseDecoder.readResponseToValue(entry.getValue()));
            }
            return res;
        }
    }

}