package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteType.
 */
public class AntidoteType {
	
	/** The Constant CounterType. */
	public static final CRDT_type CounterType = CRDT_type.COUNTER;
	
	/** The Constant IntegerType. */
	public static final CRDT_type IntegerType = CRDT_type.INTEGER;
	
	/** The Constant ORSetType. */
	public static final CRDT_type ORSetType = CRDT_type.ORSET;
	
	/** The Constant RWSetType. */
	public static final CRDT_type RWSetType = CRDT_type.RWSET;
	
	/** The Constant AWMapType. */
	public static final CRDT_type AWMapType = CRDT_type.AWMAP;
	
	/** The Constant GMapType. */
	public static final CRDT_type GMapType = CRDT_type.GMAP;
	
	/** The Constant LWWRegisterType. */
	public static final CRDT_type LWWRegisterType = CRDT_type.LWWREG;
	
	/** The Constant MVRegisterType. */
	public static final CRDT_type MVRegisterType = CRDT_type.MVREG;
}
