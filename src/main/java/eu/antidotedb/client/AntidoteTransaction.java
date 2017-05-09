package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * A transaction, either static (batch of updates) or interactive (mixed reads and writes)
 */
public abstract class AntidoteTransaction implements UpdateContext {

    abstract void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction);

}