package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

public interface CrdtCreator<V extends AntidoteCRDT> {

    AntidotePB.CRDT_type type();

    <K> V create(CrdtContainer<K> c, K key);

    V cast(AntidoteCRDT value);
}
