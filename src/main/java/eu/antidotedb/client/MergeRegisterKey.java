package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import javax.annotation.CheckReturnValue;
import java.util.List;

public class MergeRegisterKey<V> extends Key<MergeRegisterKey.MergeResult<V>> {
    public static class MergeResult<V> {
        private boolean merged;
        private V value;

        public MergeResult(boolean merged, V value) {
            this.merged = merged;
            this.value = value;
        }

        public boolean isMerged() {
            return merged;
        }

        public V getValue() {
            return value;
        }
    }

    public interface ValueMerger<V> {
        /**
         * Merges the concurrent values of the (possibly empty) list concValues into a single value
         * @param concValues the values concurrently written to the register by different replicas
         * @return the merged result
         */
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
    MergeResult<V> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
        List<V> concValues = ResponseDecoder.multiValueRegister(format).readResponseToValue(resp);
        return new MergeResult<>(concValues.size()>1, merger.merge(concValues));
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
