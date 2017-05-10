package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbGetMVRegResp;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * The Class LowLevelMVRegister.
 */
public final class MVRegisterRef<T> extends ObjectRef {


    private ValueCoder<T> format;

    MVRegisterRef(CrdtContainer container, ByteString key, AntidotePB.CRDT_type type, ValueCoder<T> format) {
        super(container, key, type);
        this.format = format;
    }


    public ValueCoder<T> getFormat() {
        return format;
    }

    @Override
    public List<T> read(TransactionWithReads tx) {
        ApbGetMVRegResp response = getContainer().read(tx, getType(), getKey()).getMvreg();
        return format.decodeList(response.getValuesList());
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
}
