package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;

public class RegisterKey<T> extends Key<T> {
    private final ValueCoder<T> format;

    RegisterKey(AntidotePB.CRDT_type type, ByteString key, ValueCoder<T> format) {
        super(type, key);
        this.format = format;
    }


    /**
     * Creates an update operation which assigns a new value to the register.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOpDefaultImpl assign(T value) {
        return buildRegisterUpdate(this, format.encode(value));
    }

    static UpdateOpDefaultImpl buildRegisterUpdate(Key<?> key, ByteString value) {
        AntidotePB.ApbRegUpdate.Builder regUpdateInstruction = AntidotePB.ApbRegUpdate.newBuilder();
        regUpdateInstruction.setValue(value);
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        return new UpdateOpDefaultImpl(key, updateOperation);
    }

    @Override
    T readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.register(format).readResponseToValue(resp);
    }
}
