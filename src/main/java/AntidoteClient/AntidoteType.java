package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;

public class AntidoteType {
	public static final CRDT_type CounterType = CRDT_type.COUNTER;
	public static final CRDT_type IntegerType = CRDT_type.INTEGER;
	public static final CRDT_type ORSetType = CRDT_type.ORSET;
	public static final CRDT_type RWSetType = CRDT_type.RWSET;
	public static final CRDT_type AWMapType = CRDT_type.AWMAP;
	public static final CRDT_type GMapType = CRDT_type.GMAP;
	public static final CRDT_type RegisterType = CRDT_type.LWWREG;
	public static final CRDT_type MVRegisterType = CRDT_type.MVREG;
}
