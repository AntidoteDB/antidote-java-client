package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbSetUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The Class LowLevelSet.
 */
public class SetRef<T> extends ObjectRef {

    private final ValueCoder<T> format;

    SetRef(CrdtContainer container, ByteString key, CRDT_type type, ValueCoder<T> format) {
        super(container, key, type);
        this.format = format;
    }

    @Override
    public List<T> read(TransactionWithReads tx) {
        AntidotePB.ApbGetSetResp set = getContainer().read(tx, getType(), getKey()).getSet();
        return format.decodeList(set.getValueList());
    }

    public final void add(AntidoteTransaction tx, T element) {
        addAll(tx, Collections.singletonList(element));
    }

    public void addAll(AntidoteTransaction tx, Collection<T> ts) {
        getContainer().update(tx, getType(), getKey(), addOpBuilder(ts));
    }

    public final void remove(AntidoteTransaction tx, T element) {
        removeAll(tx, Collections.singletonList(element));
    }

    public void removeAll(AntidoteTransaction tx, Collection<T> ts) {
        getContainer().update(tx, getType(), getKey(), removeOpBuilder(ts));
    }


    /**
     * Prepare the remove operation builder.
     *
     * @param elements the elements
     * @return the apb update operation. builder
     */
    private ApbUpdateOperation.Builder removeOpBuilder(Collection<T> elements) {
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(2);
        setUpdateInstruction.setOptype(opType);
        for (T element : elements) {
            setUpdateInstruction.addRems(format.encode(element));
        }
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        return updateOperation;
    }

    /**
     * Prepare the add operation builder.
     *
     * @param elements the elements
     * @return the apb update operation. builder
     */
    private ApbUpdateOperation.Builder addOpBuilder(Collection<T> elements) {
        ApbSetUpdate.Builder setUpdateInstruction = ApbSetUpdate.newBuilder(); // The specific instruction in update instructions
        ApbSetUpdate.SetOpType opType = ApbSetUpdate.SetOpType.forNumber(1);
        setUpdateInstruction.setOptype(opType);
        for (T element : elements) {
            setUpdateInstruction.addAdds(format.encode(element));
        }
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setSetop(setUpdateInstruction);
        return updateOperation;
    }


    public ValueCoder<T> getFormat() {
        return format;
    }

    public AntidoteOuterSet createAntidoteORSet() {
        return new AntidoteOuterSet<>(this);
    }
}
