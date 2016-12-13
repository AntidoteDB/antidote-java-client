package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

public class AntidoteMapUpdate {
	private CRDT_type type;
	private ApbUpdateOperation operation;
	
	public AntidoteMapUpdate(CRDT_type type, ApbUpdateOperation operation){
		this.type = type;
		this.operation = operation;
	}
	
	public CRDT_type getType(){
		return type;
	}
	
	public ApbUpdateOperation getOperation(){
		return operation;
	}
}
