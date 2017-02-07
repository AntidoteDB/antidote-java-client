package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapUpdate.
 */
public class AntidoteMapUpdate {
	
	/** The type. */
	private CRDT_type type;
	
	/** The operation. */
	private ApbUpdateOperation operation;
	
	/**
	 * Instantiates a new antidote map update.
	 *
	 * @param type the type
	 * @param operation the operation
	 */
	public AntidoteMapUpdate(CRDT_type type, ApbUpdateOperation operation){
		this.type = type;
		this.operation = operation;
	}
	
	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public CRDT_type getType(){
		return type;
	}
	
	/**
	 * Gets the operation.
	 *
	 * @return the operation
	 */
	public ApbUpdateOperation getOperation(){
		return operation;
	}
}
