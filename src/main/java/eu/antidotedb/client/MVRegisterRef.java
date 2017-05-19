package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbGetMVRegResp;

import java.util.List;

/**
 * The Class LowLevelMVRegister.
 */
public final class MVRegisterRef<T> extends ObjectRef<List<T>> {


    private ValueCoder<T> format;

    MVRegisterRef(CrdtContainer<?> container, ByteString key, AntidotePB.CRDT_type type, ValueCoder<T> format) {
        super(container, key, type);
        this.format = format;
    }


    public ValueCoder<T> getFormat() {
        return format;
    }

    @Override
    List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.multiValueRegister(format).readResponseToValue(resp);
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
    protected AntidotePB.ApbUpdateOperation.Builder setOpBuilder(ByteString value) {
        AntidotePB.ApbRegUpdate.Builder regUpdateInstruction = AntidotePB.ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(value);
        AntidotePB.ApbUpdateOperation.Builder updateOperation = AntidotePB.ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        return updateOperation;
    }

    public CrdtMVRegister<T> toMutable() {
        return new CrdtMVRegister<>(this);
    }
}
