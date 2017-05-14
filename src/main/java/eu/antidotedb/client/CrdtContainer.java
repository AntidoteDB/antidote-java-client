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
 */
public interface CrdtContainer<Key> {
    AntidotePB.ApbReadObjectResp read(TransactionWithReads tx, CRDT_type type, ByteString key);

    BatchReadResult<AntidotePB.ApbReadObjectResp> readBatch(BatchRead tx, CRDT_type type, ByteString key);


    void update(AntidoteTransaction tx, CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder);

    ValueCoder<Key> keyCoder();


    default CounterRef counter(Key key) {
        return new CounterRef(this, keyCoder().encode(key), CRDT_type.COUNTER);
    }

    default CounterRef fatCounter(Key key) {
        return new CounterRef(this, keyCoder().encode(key), CRDT_type.FATCOUNTER);
    }

    default IntegerRef integer(Key key) {
        return new IntegerRef(this, keyCoder().encode(key));
    }

    default <T> RegisterRef<T> register(Key key, ValueCoder<T> format) {
        return new RegisterRef<>(this, keyCoder().encode(key), CRDT_type.LWWREG, format);
    }

    default RegisterRef<String> register(Key key) {
        return register(key, ValueCoder.utf8String);
    }

    default <T> MVRegisterRef<T> multiValueRegister(Key key, ValueCoder<T> format) {
        return new MVRegisterRef<>(this, keyCoder().encode(key), CRDT_type.MVREG, format);
    }

    default MVRegisterRef<String> multiValueRegister(Key key) {
        return multiValueRegister(key, ValueCoder.utf8String);
    }

    default <T> SetRef<T> set(Key key, ValueCoder<T> format) {
        return new SetRef<>(this, keyCoder().encode(key), CRDT_type.ORSET, format);
    }

    default SetRef<String> set(Key key) {
        return set(key, ValueCoder.utf8String);
    }

    default <T> SetRef<T> set_removeWins(Key key, ValueCoder<T> format) {
        return new SetRef<>(this, keyCoder().encode(key), CRDT_type.RWSET, format);
    }

    default SetRef<String> set_removeWins(Key key) {
        return set_removeWins(key, ValueCoder.utf8String);
    }

    default <K> MapRef<K> map_aw(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.AWMAP, keyCoder);
    }

    default <K> MapRef<K> map_rr(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.RRMAP, keyCoder);
    }

    default <K> MapRef<K> map_g(Key key, ValueCoder<K> keyCoder) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.GMAP, keyCoder);
    }

    default MapRef<String> map_aw(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.AWMAP, ValueCoder.utf8String);
    }

    default MapRef<String> map_rr(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.RRMAP, ValueCoder.utf8String);
    }

    default MapRef<String> map_g(Key key) {
        return new MapRef<>(this, keyCoder().encode(key), CRDT_type.GMAP, ValueCoder.utf8String);
    }


}
