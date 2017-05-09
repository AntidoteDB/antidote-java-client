package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * This class can be used to execute an individual operation without any transactional context
 */
public class NoTransaction extends TransactionWithReads {

    @Override
    void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction) {
        // TODO implement this
        throw new RuntimeException("TODO implement");
    }

    @Override
    AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        // TODO implement this
        throw new RuntimeException("TODO implement");
    }

}
