package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

public interface CrdtCreator<V extends AntidoteCRDT> {

    AntidotePB.CRDT_type type();

    V create(CrdtContainer c, ByteString key);

    V cast(AntidoteCRDT value);
}
