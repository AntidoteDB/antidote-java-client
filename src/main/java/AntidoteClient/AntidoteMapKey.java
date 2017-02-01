package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

public class AntidoteMapKey {
	
	private ByteString key;
	private CRDT_type type;
	private ApbMapKey apbKey;

	public AntidoteMapKey(CRDT_type type, String key){
		this.key = ByteString.copyFromUtf8(key);
		this.type = type;
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(this.key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	public AntidoteMapKey(CRDT_type type, ByteString key){
		this.key = key;
		this.type = type;
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	protected AntidoteMapKey(ApbMapKey key){
		this.key = key.getKey();
		this.type = key.getType();
		this.apbKey = key;
	}
	
	public String getKey(){
		return key.toStringUtf8();
	}
	
	public ByteString getKeyBS(){
		return key;
	}
	
	public ApbMapKey getApbKey(){
		return apbKey;
	}
	
	public CRDT_type getType(){
		return type;
	}
	
	private void setApbKey(CRDT_type type, ByteString key){
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	public void setType(CRDT_type type){
		this.type = type;
		setApbKey(type, key);
	}
	
	public void setKey(String key){
		this.key = ByteString.copyFromUtf8(key);
		setApbKey(type, this.key);
	}
	
	public void setKey(ByteString key){
		this.key = key;
		setApbKey(type, key);
	}	
}
