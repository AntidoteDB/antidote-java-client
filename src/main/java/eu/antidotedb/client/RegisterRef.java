package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbRegUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * The Class LowLevelRegister.
 */
public class RegisterRef<T> extends ObjectRef {

    private ValueCoder<T> format;

    RegisterRef(CrdtContainer container, ByteString key, CRDT_type type, ValueCoder<T> format) {
        super(container, key, type);
        this.format = format;
    }

    @Override
    public T read(TransactionWithReads tx) {
        AntidotePB.ApbGetRegResp response = getContainer().read(tx, getType(), getKey()).getReg();
        return format.decode(response.getValue());
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
}
