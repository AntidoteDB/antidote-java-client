package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;

public interface CrdtCreator<V> {

    AntidotePB.CRDT_type type();

    V create(CrdtContainer c, ByteString key);
}
