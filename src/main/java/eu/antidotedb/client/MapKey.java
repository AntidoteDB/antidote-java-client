package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.*;
import java.util.function.Function;

public class MapKey extends Key<MapKey.MapReadResult> {


    MapKey(AntidotePB.CRDT_type type, ByteString key) {
        super(type, key);
    }

    @Override
    MapReadResult readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.map().readResponseToValue(resp);
    }

    /**
     * Creates an update operation, which updates the CRDTs embedded in this map.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp update(UpdateOp... keyUpdates) {
        return update(Arrays.asList(keyUpdates));
    }

    /**
     * Creates an update operation, which updates the CRDTs embedded in this map.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp update(Iterable<UpdateOp> keyUpdates) {
        return new MapUpdateOp().update(keyUpdates);
    }

    /**
     * Creates an update operation, which removes keys from the map.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp removeKeys(Key<?>... keys) {
        return removeKeys(Arrays.asList(keys));
    }

    /**
     * Creates an update operation, which removes keys from the map.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp removeKeys(Iterable<? extends Key<?>> keys) {
        return new MapUpdateOp().removeKeys(keys);
    }


    class MapUpdateOp extends UpdateOp {
        Set<Key<?>> changedKeys = new HashSet<>();
        AntidotePB.ApbMapUpdate.Builder op = AntidotePB.ApbMapUpdate.newBuilder();

        MapUpdateOp() {
            super(MapKey.this);
        }


        @Override
        AntidotePB.ApbUpdateOperation.Builder getApbUpdate() {
            AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
            updateOperation.setMapop(op);
            return updateOperation;
        }

        /**
         * Adds more updates to this map update and returns a reference to the same object to allow chaining of methods.
         */
        public UpdateOp update(UpdateOp... keyUpdates) {
            return update(Arrays.asList(keyUpdates));
        }

        /**
         * Adds more updates to this map update and returns a reference to the same object to allow chaining of methods.
         */
        public UpdateOp update(Iterable<UpdateOp> keyUpdates) {
            for (UpdateOp keyUpdate : keyUpdates) {
                if (!changedKeys.add(keyUpdate.getKey())) {
                    throw new AntidoteException("Key " + keyUpdate.getKey() + " is already changed in this map update.");
                }
                op.addUpdates(keyUpdate.toApbNestedUpdate());
            }
            return this;
        }

        /**
         * Adds more removed keys to this map update and returns a reference to the same object to allow chaining of methods.
         */
        public UpdateOp removeKeys(Key<?>... keys) {
            return removeKeys(Arrays.asList(keys));
        }

        /**
         * Adds more removed keys to this map update and returns a reference to the same object to allow chaining of methods.
         */
        public UpdateOp removeKeys(Iterable<? extends Key<?>> keys) {
            for (Key<?> key : keys) {
                if (!changedKeys.add(key)) {
                    throw new AntidoteException("Key " + key + " is already changed in this map update.");
                }
                op.addRemovedKeys(key.toApbMapKey());
            }
            return this;
        }
    }


    /**
     * Presents the result of a read request on a map CRDT.
     * This implements the {@link Map} interface, but is not mutable.
     */
    public static class MapReadResult extends AbstractMap<Key<?>, Object> {
        private Map<Key<?>, AntidotePB.ApbReadObjectResp> responses = new LinkedHashMap<>();


        MapReadResult(List<AntidotePB.ApbMapEntry> entriesList) {
            for (AntidotePB.ApbMapEntry entry : entriesList) {
                AntidotePB.ApbMapKey key = entry.getKey();
                responses.put(Key.fromApbMapKey(key), entry.getValue());
            }
        }


        /**
         * @deprecated Use the get-method which takes a Key instead.
         */
        @Override
        @Deprecated
        public Object get(Object key) {
            if (key instanceof Key<?>) {
                return get((Key<?>) key);
            }
            throw new IllegalArgumentException("Invalid type for key " + key);
        }

        /**
         * Reads an entry from the map.
         */
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

        /**
         * Same as {@link #asJavaMap(Function, ResponseDecoder)} with Strings as keys (keyCoder = key -> key.getKey().toStringUtf8())
         */
        public <V> Map<String, V> asJavaMap(ResponseDecoder<V> responseDecoder) {
            return asJavaMap(key -> key.getKey().toStringUtf8(), responseDecoder);
        }


        /**
         * Converts this result to a plain Java map
         *
         * @param keyCoder        a coder which specifies how CRDT-keys are converted to keys in the map
         * @param responseDecoder a decoder specifying how values are decoded (See static methods in {@link ResponseDecoder})
         * @param <V>             The values in the resulting map
         * @return A plain Java map representing this result.
         */
        public <K, V> Map<K, V> asJavaMap(Function<Key<?>, K> keyCoder, ResponseDecoder<V> responseDecoder) {
            LinkedHashMap<K, V> res = new LinkedHashMap<>();
            for (Entry<Key<?>, AntidotePB.ApbReadObjectResp> entry : responses.entrySet()) {
                res.put(keyCoder.apply(entry.getKey()), responseDecoder.readResponseToValue(entry.getValue()));
            }
            return res;
        }
    }

}