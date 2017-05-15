package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;

import java.util.List;

public abstract class TransactionWithReads extends AntidoteTransaction {

    abstract AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type);

    abstract void batchReadHelper(List<BatchReadResultImpl> readRequests);
}
