package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.List;

public class MergeRegisterKey<V> extends Key<V> {
    public interface ValueMerger<V> {
        V merge(List<V> concValues);
    }

    private ValueCoder<V> format;
    private ValueMerger<V> merger;

    MergeRegisterKey(ByteString key, ValueCoder<V> format, ValueMerger<V> merger) {
        super(AntidotePB.CRDT_type.MVREG, key);
        this.format = format;
        this.merger = merger;
    }

    @Override
    V readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        return merger.merge(ResponseDecoder.multiValueRegister(format).readResponseToValue(resp));
    }

    /**
     * Creates an update operation which assigns a new value to the register.
     * <p>
     * Use the methods on {@link Bucket} to execute the update.
     */
    @CheckReturnValue
    public UpdateOpDefaultImpl assign(V value) {
        return RegisterKey.buildRegisterUpdate(this, format.encode(value));
    }
}
