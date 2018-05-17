package eu.antidotedb.client;


import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.Objects;

/**
 * An Antidote key consists of a CRDT type and a corresponding key.
 * It can be used as a top-level-key of an Antidote object in a bucket or as a key in a map_rr.
 * <p>
 * Use the static methods of this class to create keys for the respective CRDT types.
 */
public abstract class Key<Value> {
    private final AntidotePB.CRDT_type type;
    private final ByteString key;

    Key(AntidotePB.CRDT_type type, ByteString key) {
        this.type = type;
        this.key = key;
    }

    /**
     * @return the CRDT type for this key
     */
    public AntidotePB.CRDT_type getType() {
        return type;
    }

    /**
     * @return the ByteString component of this key
     */
    public ByteString getKey() {
        return key;
    }


    abstract Value readResponseToValue(AntidotePB.ApbReadObjectResp resp);


    @Override
    public String toString() {
        return type + "_" + key.toStringUtf8();
    }

    /**
     * Equality and hashCode on keys is defined only in terms of the key and type fields.
     */
    @Override
    public final boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Key<?>)) {
            return false;
        }
        Key<?> k = (Key<?>) obj;
        return k.type.equals(type) && k.key.equals(key);
    }

    /**
     * Equality and hashCode on keys is defined only in terms of the key and type fields.
     */
    @Override
    public final int hashCode() {
        return Objects.hash(type, key);
    }

    /**
     * A counter CRDT.
     */
    public static CounterKey counter(ByteString key) {
        return new CounterKey(AntidotePB.CRDT_type.COUNTER, key);
    }

    /**
     * A counter CRDT.
     */
    public static CounterKey counter(String key) {
        return new CounterKey(AntidotePB.CRDT_type.COUNTER, ByteString.copyFromUtf8(key));
    }

    /**
     * A counter CRDT.
     * Is like a counter, but can be reset and is less efficient.
     */
    public static CounterKey fatCounter(ByteString key) {
        return new CounterKey(AntidotePB.CRDT_type.FATCOUNTER, key);
    }

    /**
     * A counter CRDT.
     * Is like a counter, but can be reset and is less efficient.
     */
    public static CounterKey fatCounter(String key) {
        return fatCounter(ByteString.copyFromUtf8(key));
    }

    /**
     * A last-writer-wins register.
     *
     * @param key    the key
     * @param format format of values stored in the register
     * @param <T>    type of value stored in the register
     */
    public static <T> RegisterKey<T> register(ByteString key, ValueCoder<T> format) {
        return new RegisterKey<>(AntidotePB.CRDT_type.LWWREG, key, format);
    }

    /**
     * @see #register(ByteString, ValueCoder)
     */
    public static RegisterKey<String> register(ByteString key) {
        return register(key, ValueCoder.utf8String);
    }

    /**
     * A last-writer-wins register.
     *
     * @param key    the key
     * @param format format of values stored in the register
     * @param <T>    type of value stored in the register
     */
    public static <T> RegisterKey<T> register(String key, ValueCoder<T> format) {
        return register(ByteString.copyFromUtf8(key), format);
    }

    /**
     * @see #register(ByteString, ValueCoder)
     */
    public static RegisterKey<String> register(String key) {
        return register(key, ValueCoder.utf8String);
    }

    /**
     * A multi-value register.
     * Reading a value returns all written values, which are not overridden by another write-operation.
     */
    public static <T> MVRegisterKey<T> multiValueRegister(ByteString key, ValueCoder<T> format) {
        return new MVRegisterKey<>(key, format);
    }

    /**
     * @see #multiValueRegister(ByteString, ValueCoder)
     */
    public static MVRegisterKey<String> multiValueRegister(ByteString key) {
        return multiValueRegister(key, ValueCoder.utf8String);
    }


    /**
     * @see #multiValueRegister(ByteString, ValueCoder)
     */
    public static <T> MVRegisterKey<T> multiValueRegister(String key, ValueCoder<T> format) {
        return multiValueRegister(ByteString.copyFromUtf8(key), format);
    }

    /**
     * @see #multiValueRegister(ByteString, ValueCoder)
     */
    public static MVRegisterKey<String> multiValueRegister(String key) {
        return multiValueRegister(key, ValueCoder.utf8String);
    }

    /**
     * An add-wins set.
     */
    public static <T> SetKey<T> set(ByteString key, ValueCoder<T> format) {
        return new SetKey<>(AntidotePB.CRDT_type.ORSET, key, format);
    }

    /**
     * @see #set(ByteString, ValueCoder)
     */
    public static SetKey<String> set(ByteString key) {
        return set(key, ValueCoder.utf8String);
    }

    /**
     * @see #set(ByteString, ValueCoder)
     */
    public static <T> SetKey<T> set(String key, ValueCoder<T> format) {
        return set(ByteString.copyFromUtf8(key), format);
    }

    /**
     * @see #set(ByteString, ValueCoder)
     */
    public static SetKey<String> set(String key) {
        return set(key, ValueCoder.utf8String);
    }

    /**
     * A remove-wins set.
     */
    public static <T> SetKey<T> set_removeWins(ByteString key, ValueCoder<T> format) {
        return new SetKey<>(AntidotePB.CRDT_type.RWSET, key, format);
    }

    /**
     * @see #set_removeWins(ByteString, ValueCoder)
     */
    public static SetKey<String> set_removeWins(ByteString key) {
        return set_removeWins(key, ValueCoder.utf8String);
    }


    /**
     * @see #set_removeWins(ByteString, ValueCoder)
     */
    public static <T> SetKey<T> set_removeWins(String key, ValueCoder<T> format) {
        return set_removeWins(ByteString.copyFromUtf8(key), format);
    }

    /**
     * @see #set_removeWins(ByteString, ValueCoder)
     */
    public static SetKey<String> set_removeWins(String key) {
        return set_removeWins(key, ValueCoder.utf8String);
    }

    /**
     * Remove-resets map.
     * Removing an entry resets the corresponding CRDT.
     * Entries using a CRDT that does not support resets cannot be removed form the map.
     * Therefore this map should mainly be used with embedded CRDTs that support a reset operation.
     * <p>
     * Reading the map only returns entries which have a value, where the internal state is not equal to the initial CRDT state.
     */
    public static MapKey map_rr(ByteString key) {
        return new MapKey(AntidotePB.CRDT_type.RRMAP, key);
    }

    /**
     * @see #map_rr(ByteString)
     */
    public static MapKey map_rr(String key) {
        return map_rr(ByteString.copyFromUtf8(key));
    }

    /**
     * Grow-only map.
     * Does not support removing entries.
     * It can be used for modelling struct, where the set of keys does not change over time.
     */
    public static MapKey map_g(ByteString key) {
        return new MapKey(AntidotePB.CRDT_type.GMAP, key);
    }

    /**
     * @see #map_g(ByteString)
     */
    public static MapKey map_g(String key) {
        return map_g(ByteString.copyFromUtf8(key));
    }


    public static FlagKey flag_ew(ByteString key) {
        return new FlagKey(AntidotePB.CRDT_type.FLAG_EW, key);
    }

    public static FlagKey flag_ew(String key) {
        return flag_ew(ByteString.copyFromUtf8(key));
    }

    public static FlagKey flag_dw(ByteString key) {
        return new FlagKey(AntidotePB.CRDT_type.FLAG_DW, key);
    }

    public static FlagKey flag_dw(String key) {
        return flag_ew(ByteString.copyFromUtf8(key));
    }

    public static <V> MergeRegisterKey<V> mergeRegister(ByteString key, ValueCoder<V> format, MergeRegisterKey.ValueMerger<V> merger) {
        return new MergeRegisterKey<>(key, format, merger);
    }

    public static <V> MergeRegisterKey<V> mergeRegister(String key, ValueCoder<V> format, MergeRegisterKey.ValueMerger<V> merger) {
        return mergeRegister(ByteString.copyFromUtf8(key), format, merger);
    }


    AntidotePB.ApbMapKey.Builder toApbMapKey() {
        AntidotePB.ApbMapKey.Builder builder = AntidotePB.ApbMapKey.newBuilder();
        builder.setType(type);
        builder.setKey(key);
        return builder;
    }

    static Key<?> fromApbMapKey(AntidotePB.ApbMapKey key) {
        ByteString k = key.getKey();
        AntidotePB.CRDT_type type = key.getType();
        return create(type, k);
    }

    /**
     * Creates a Key from a CRDT_type and a ByteString.
     */
    public static Key<?> create(AntidotePB.CRDT_type type, ByteString k) {
        switch (type) {
            case COUNTER:
                return counter(k);
            case ORSET:
                return set(k);
            case LWWREG:
                return register(k);
            case MVREG:
                return multiValueRegister(k);
            case GMAP:
                return map_g(k);
            case RWSET:
                return set_removeWins(k);
            case RRMAP:
                return map_rr(k);
            case FATCOUNTER:
                return fatCounter(k);
            case FLAG_EW:
                return flag_ew(k);
            case FLAG_DW:
                return flag_dw(k);
            default:
                throw new RuntimeException("CRDT not yet supported: " + type);
        }
    }

    /**
     * Creates a reset operation.
     * If the underlying CRDT type has full support for reset, this operation has the effect
     * of undoing all previous operations and it has no effect on concurrent operations.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOp reset() {
        AntidotePB.ApbCrdtReset.Builder op = AntidotePB.ApbCrdtReset.newBuilder();
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setResetop(op);
        return new UpdateOpDefaultImpl(this, updateOperation);
    }
}
