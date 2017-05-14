package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

import java.io.IOException;

public abstract class TransactionWithReads extends AntidoteTransaction {

    abstract AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type);
}
