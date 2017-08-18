package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SetKey<T> extends Key<List<T>> {
    private final ValueCoder<T> format;

    public SetKey(AntidotePB.CRDT_type type, ByteString key, ValueCoder<T> format) {
        super(type, key);
        this.format = format;
    }

    public final InnerUpdateOpImpl add(T value) {
        return addAll(Collections.singletonList(value));
    }

    @SafeVarargs
    @CheckReturnValue
    public final InnerUpdateOpImpl addAll(T... values) {
        return addAll(Arrays.asList(values));
    }

    @CheckReturnValue
    public InnerUpdateOpImpl addAll(Iterable<? extends T> values) {
        AntidotePB.ApbSetUpdate.Builder op = AntidotePB.ApbSetUpdate.newBuilder();
        for (T value : values) {
            op.addAdds(format.encode(value));
        }
        op.setOptype(AntidotePB.ApbSetUpdate.SetOpType.ADD);
        AntidotePB.ApbUpdateOperation.Builder update = AntidotePB.ApbUpdateOperation.newBuilder();
        update.setSetop(op);
        return new InnerUpdateOpImpl(this, update);
    }

    public final InnerUpdateOpImpl remove(T value) {
        return removeAll(Collections.singletonList(value));
    }

    @SafeVarargs
    public final InnerUpdateOpImpl removeAll(T... values) {
        return removeAll(Arrays.asList(values));
    }

    @CheckReturnValue
    public InnerUpdateOpImpl removeAll(Iterable<? extends T> values) {
        AntidotePB.ApbSetUpdate.Builder op = AntidotePB.ApbSetUpdate.newBuilder();
        for (T value : values) {
            op.addRems(format.encode(value));
        }
        op.setOptype(AntidotePB.ApbSetUpdate.SetOpType.REMOVE);
        AntidotePB.ApbUpdateOperation.Builder update = AntidotePB.ApbUpdateOperation.newBuilder();
        update.setSetop(op);
        return new InnerUpdateOpImpl(this, update);
    }


    @Override
    List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.set(format).readResponseToValue(resp);
    }
}
