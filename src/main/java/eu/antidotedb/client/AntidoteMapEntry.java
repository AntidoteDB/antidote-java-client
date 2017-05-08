package eu.antidotedb.client;

import com.google.protobuf.ByteString;

public class AntidoteMapEntry {
    private final ByteString key;
    private final AntidoteCRDT value;

    public AntidoteMapEntry(ByteString key, AntidoteCRDT value) {
        this.key = key;
        this.value = value;
    }

    public ByteString getKey() {
        return key;
    }

    public AntidoteCRDT getValue() {
        return value;
    }
}
