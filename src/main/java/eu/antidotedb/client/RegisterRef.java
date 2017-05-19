package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbRegUpdate;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOperation;
import eu.antidotedb.antidotepb.AntidotePB.CRDT_type;

/**
 * The Class LowLevelRegister.
 */
public class RegisterRef<T> extends ObjectRef<T> {

    private ValueCoder<T> format;

    RegisterRef(CrdtContainer<?> container, ByteString key, CRDT_type type, ValueCoder<T> format) {
        super(container, key, type);
        this.format = format;
    }


    @Override
    T readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.register(format).readResponseToValue(resp);
    }


    public void set(AntidoteTransaction tx, T value) {
        getContainer().update(tx, getType(), getKey(), setOpBuilder(format.encode(value)));
    }


    /**
     * Prepare a set operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder setOpBuilder(ByteString value) {
        ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(value);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        return updateOperation;
    }

    public ValueCoder<T> getFormat() {
        return format;
    }

    public CrdtRegister<T> toMutable() {
        return new CrdtRegister<>(this);
    }
}
