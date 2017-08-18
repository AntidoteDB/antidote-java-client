package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import java.util.ArrayList;

/**
 *
 */
public class BatchRead {

    private ArrayList<BatchReadResultImpl> requests = new ArrayList<>();


    /**
     * Performs this batch of reads in the context of another transaction
     */
    public void commit(TransactionWithReads tx) {
        if (requests.isEmpty()) {
            // nothing to do
            return;
        }
        tx.batchReadHelper(requests);
        requests.clear();
    }


    public BatchReadResultImpl readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        AntidotePB.ApbBoundObject.Builder object = AntidotePB.ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(key);
        object.setType(type);
        object.setBucket(bucket);

        BatchReadResultImpl res = new BatchReadResultImpl(this, object);
        requests.add(res);
        return res;
    }
}
