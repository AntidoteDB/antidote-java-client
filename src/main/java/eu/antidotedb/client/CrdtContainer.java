package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;

/**
 * A CRDT container contains CRDTs stored under keys
 * <p>
 * <ul>
 * <li>A bucket is a CRDT container storing top-level CRDTs</li>
 * <li>A Map CRDT is CRDT container since it contains nested CRDTs</li>
 * </ul>
 *
 * @param <Key> the key used to address objects stored in this container
 */
public interface CrdtContainer<Key> {
    /**
     * An internal helper method for reading an object in this container.
     */
    AntidotePB.ApbReadObjectResp read(TransactionWithReads tx, CRDT_type type, ByteString key);

    /**
     * An internal helper method for reading an object in this container.
     */
    BatchReadResult<AntidotePB.ApbReadObjectResp> readBatch(BatchRead tx, CRDT_type type, ByteString key);


    /**
     * An internal helper method for updating an object in this container.
     */
    void update(AntidoteTransaction tx, CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder);

    /**
     * The ValueCoder encoding the keys of entries in this container
     */
    ValueCoder<Key> keyCoder();


    /**
     * A counter CRDT.
     */
    default CounterRef counter(Key key) {
        return new CounterRef(this, keyCoder().encode(key), CRDT_type.COUNTER);
    }

    /**
     * A counter CRDT.
     * Is like a counter, but can be reset and is less efficient.
     */
    default CounterRef fatCounter(Key key) {
        return new CounterRef(this, keyCoder().encode(key), CRDT_type.FATCOUNTER);
    }

    /**
     * An integer can be incremented and assigned to.
     */
    default IntegerRef integer(Key key) {
        return new IntegerRef(this, keyCoder().encode(key));
    }

    /**
     * A last-writer-wins register.
     *
     * @param key    the key
     * @param format format of values stored in the register
     * @param <T>    type of value stored in the register
     * @return
     */
    default <T> RegisterRef<T> register(Key key, ValueCoder<T> format) {
        return new RegisterRef<>(this, keyCoder().encode(key), CRDT_type.LWWREG, format);
    }

    /**
     * @see #register(Object, ValueCoder)
     */
    default RegisterRef<String> register(Key key) {
        return register(key, ValueCoder.utf8String);
    }

    /**
     * A multi-value register.
     * Reading a value returns all written values, which are not overridden by another write-operation.
     */
    default <T> MVRegisterRef<T> multiValueRegister(Key key, ValueCoder<T> format) {
        return new MVRegisterRef<>(this, keyCoder().encode(key), CRDT_type.MVREG, format);
    }

    /**
     * @see #multiValueRegister(Object, ValueCoder)
     */
    default MVRegisterRef<String> multiValueRegister(Key key) {
        return multiValueRegister(key, ValueCoder.utf8String);
    }

    /**
     * An add-wins set.
     */
    default <T> SetRef<T> set(Key key, ValueCoder<T> format) {
        return new SetRef<>(this, keyCoder().encode(key), CRDT_type.ORSET, format);
    }

    /**
     * @see #set(Object, ValueCoder)
     */
    default SetRef<String> set(Key key) {
        return set(key, ValueCoder.utf8String);
    }

    /**
     * A remove-wins register.
     */
    default <T> SetRef<T> set_removeWins(Key key, ValueCoder<T> format) {
        return new SetRef<>(this, keyCoder().encode(key), CRDT_type.RWSET, format);
    }

    /**
     * @see #set_removeWins(Object, ValueCoder)
     */
    default SetRef<String> set_removeWins(Key key) {
        return set_removeWins(key, ValueCoder.utf8String);
    }

    /**
     * An add-wins map.
     * Updates win over concurrent deletes.
     * Deleting an entry uses tombstones which are not garbage-collected.
     */
    default <K> MapRef<K> map_aw(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.AWMAP, keyCoder);
    }

    /**
     * Remove-resets map.
     * Removing an entry resets the corresponding CRDT.
     * Therefore this map should mainly be used with embedded CRDTs that support a reset operation.
     * <p>
     * Reading the map only returns entries which have a value, where the internal state is not equal to the initial CRDT state.
     */
    default <K> MapRef<K> map_rr(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.RRMAP, keyCoder);
    }

    /**
     * Grow-only map.
     * Does not support removing entries.
     * It can be used for modelling struct, where the set of keys does not change over time.
     */
    default <K> MapRef<K> map_g(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.GMAP, keyCoder);
    }

    /**
     * @see #map_aw(Object, ValueCoder)
     */
    default MapRef<String> map_aw(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.AWMAP, ValueCoder.utf8String);
    }

    /**
     * @see #map_rr(Object, ValueCoder)
     */
    default MapRef<String> map_rr(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.RRMAP, ValueCoder.utf8String);
    }

    /**
     * @see #map_g(Object, ValueCoder)
     */
    default MapRef<String> map_g(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.GMAP, ValueCoder.utf8String);
    }


}
