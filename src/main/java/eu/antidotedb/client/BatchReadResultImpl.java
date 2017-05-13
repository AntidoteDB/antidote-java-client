package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.ApbReadObjectResp;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 */
class BatchReadResultImpl implements BatchReadResult<ApbReadObjectResp> {
    private final BatchRead batchRead;
    // the object to read
    private final AntidotePB.ApbBoundObject.Builder object;
    private ApbReadObjectResp result = null;
    private final List<Consumer<ApbReadObjectResp>> listeners = new ArrayList<>();

    public BatchReadResultImpl(BatchRead batchRead, AntidotePB.ApbBoundObject.Builder object) {
        this.batchRead = batchRead;
        this.object = object;
    }

    @Override
    public ApbReadObjectResp get() {
        if (result == null) {
            // result not computed yet, commit batchRead to get result
            batchRead.commit();
        }
        return result;
    }

    @Override
    public void whenReady(Consumer<ApbReadObjectResp> f) {
        if (result == null) {
            listeners.add(f);
        } else {
            f.accept(result);
        }
    }

    public void setResult(ApbReadObjectResp result) {
        this.result = result;
        for (Consumer<ApbReadObjectResp> callback : listeners) {
            callback.accept(result);
        }
    }

    public AntidotePB.ApbBoundObject.Builder getObject() {
        return object;
    }
}
