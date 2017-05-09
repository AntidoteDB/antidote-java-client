package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * A CRDT container contains CRDTs stored under keys
 * <p>
 * <ul>
 * <li>A bucket is a CRDT container storing top-level CRDTs</li>
 * <li>A Map CRDT is CRDT container since it contains nested CRDTs</li>
 * </ul>
 */
public interface CrdtContainer {
    AntidotePB.ApbReadObjectResp read(InteractiveTransaction tx, CRDT_type type, ByteString key);

    void update(AntidoteTransaction tx, CRDT_type type, ByteString key, AntidotePB.ApbUpdateOperation.Builder builder);

    default CounterRef counter(ByteString key) {
        return new CounterRef(this, key, CRDT_type.COUNTER);
    }

    default CounterRef fatCounter(ByteString key) {
        return new CounterRef(this, key, CRDT_type.COUNTER); // TODO change to fat counter
    }

    default IntegerRef integer(ByteString key) {
        return new IntegerRef(this, key);
    }

    default <T> RegisterRef<T> register(ByteString key, ValueCoder<T> format) {
        return new RegisterRef<T>(this, key, CRDT_type.LWWREG, format);
    }

    default <T> MVRegisterRef<T> multiValueRegister(ByteString key, ValueCoder<T> format) {
        return new MVRegisterRef<T>(this, key, CRDT_type.MVREG, format);
    }

    default <T> SetRef<T> set(ByteString key, ValueCoder<T> format) {
        return new SetRef<T>(this, key, CRDT_type.ORSET, format);
    }

    default <T> SetRef<T> set_removeWins(ByteString key, ValueCoder<T> format) {
        return new SetRef<T>(this, key, CRDT_type.RWSET, format);
    }

    default MapRef map_aw(ByteString key) {
        return new MapRef(this, key, CRDT_type.AWMAP);
    }

    default MapRef map_rr(ByteString key) {
        return new MapRef(this, key, CRDT_type.AWMAP); // TODO fix type
    }

    default MapRef map_g(ByteString key) {
        return new MapRef(this, key, CRDT_type.GMAP);
    }


    default CounterRef counter(String key) {
        return counter(ByteString.copyFromUtf8(key));
    }

    default CounterRef fatCounter(String key) {
        return fatCounter(ByteString.copyFromUtf8(key));
    }

    default IntegerRef integer(String key) {
        return integer(ByteString.copyFromUtf8(key));
    }

    default <T> RegisterRef<T> register(String key, ValueCoder<T> format) {
        return register(ByteString.copyFromUtf8(key), format);
    }

    default <T> MVRegisterRef<T> multiValueRegister(String key, ValueCoder<T> format) {
        return multiValueRegister(ByteString.copyFromUtf8(key), format);
    }

    default <T> SetRef<T> set(String key, ValueCoder<T> format) {
        return set(ByteString.copyFromUtf8(key), format);
    }

    default <T> SetRef<T> set_removeWins(String key, ValueCoder<T> format) {
        return set_removeWins(ByteString.copyFromUtf8(key), format);
    }

    default MapRef map_aw(String key) {
        return map_aw(ByteString.copyFromUtf8(key));
    }

    default MapRef map_rr(String key) {
        return map_rr(ByteString.copyFromUtf8(key));
    }

    default MapRef gmap(String key) {
        return map_g(ByteString.copyFromUtf8(key));
    }


}
