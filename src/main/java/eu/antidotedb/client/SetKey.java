package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SetKey<T> extends Key<List<T>> {
    private final ValueCoder<T> format;

    SetKey(AntidotePB.CRDT_type type, ByteString key, ValueCoder<T> format) {
        super(type, key);
        this.format = format;
    }

    /**
     * Creates an update operation which adds a value to the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public final UpdateOpDefaultImpl add(T value) {
        return addAll(Collections.singletonList(value));
    }

    /**
     * Creates an update operation which adds values to the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @SafeVarargs
    @CheckReturnValue
    public final UpdateOpDefaultImpl addAll(T... values) {
        return addAll(Arrays.asList(values));
    }

    /**
     * Creates an update operation which adds values to the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOpDefaultImpl addAll(Iterable<? extends T> values) {
        AntidotePB.ApbSetUpdate.Builder op = AntidotePB.ApbSetUpdate.newBuilder();
        for (T value : values) {
            op.addAdds(format.encode(value));
        }
        op.setOptype(AntidotePB.ApbSetUpdate.SetOpType.ADD);
        AntidotePB.ApbUpdateOperation.Builder update = AntidotePB.ApbUpdateOperation.newBuilder();
        update.setSetop(op);
        return new UpdateOpDefaultImpl(this, update);
    }

    /**
     * Creates an update operation which removes a value from the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public final UpdateOpDefaultImpl remove(T value) {
        return removeAll(Collections.singletonList(value));
    }

    /**
     * Creates an update operation which removes values from the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @SafeVarargs
    @CheckReturnValue
    public final UpdateOpDefaultImpl removeAll(T... values) {
        return removeAll(Arrays.asList(values));
    }

    /**
     * Creates an update operation which removes values from the set.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOpDefaultImpl removeAll(Iterable<? extends T> values) {
        AntidotePB.ApbSetUpdate.Builder op = AntidotePB.ApbSetUpdate.newBuilder();
        for (T value : values) {
            op.addRems(format.encode(value));
        }
        op.setOptype(AntidotePB.ApbSetUpdate.SetOpType.REMOVE);
        AntidotePB.ApbUpdateOperation.Builder update = AntidotePB.ApbUpdateOperation.newBuilder();
        update.setSetop(op);
        return new UpdateOpDefaultImpl(this, update);
    }


    @Override
    List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return ResponseDecoder.set(format).readResponseToValue(resp);
    }
}
