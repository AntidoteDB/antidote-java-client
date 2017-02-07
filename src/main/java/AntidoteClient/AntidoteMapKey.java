package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapKey.
 */
public class AntidoteMapKey {
	
	/** The key. */
	private ByteString key;
	
	/** The type. */
	private CRDT_type type;
	
	/** The apb key. */
	private ApbMapKey apbKey;

	/**
	 * Instantiates a new antidote map key.
	 *
	 * @param type the type
	 * @param key the key
	 */
	public AntidoteMapKey(CRDT_type type, String key){
		this.key = ByteString.copyFromUtf8(key);
		this.type = type;
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(this.key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	/**
	 * Instantiates a new antidote map key.
	 *
	 * @param type the type
	 * @param key the key
	 */
	public AntidoteMapKey(CRDT_type type, ByteString key){
		this.key = key;
		this.type = type;
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	/**
	 * Instantiates a new antidote map key.
	 *
	 * @param key the key
	 */
	protected AntidoteMapKey(ApbMapKey key){
		this.key = key.getKey();
		this.type = key.getType();
		this.apbKey = key;
	}
	
	/**
	 * Gets the key.
	 *
	 * @return the key
	 */
	public String getKey(){
		return key.toStringUtf8();
	}
	
	/**
	 * Gets the key as ByteString.
	 *
	 * @return the key BS
	 */
	public ByteString getKeyBS(){
		return key;
	}
	
	/**
	 * Gets the apb key.
	 *
	 * @return the apb key
	 */
	protected ApbMapKey getApbKey(){
		return apbKey;
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
	 * Sets the apb key.
	 *
	 * @param type the type
	 * @param key the key
	 */
	private void setApbKey(CRDT_type type, ByteString key){
		ApbMapKey.Builder apbKeyBuilder = ApbMapKey.newBuilder();
		apbKeyBuilder.setKey(key);
		apbKeyBuilder.setType(type);
		this.apbKey = apbKeyBuilder.build();
	}
	
	/**
	 * Sets the type.
	 *
	 * @param type the new type
	 */
	public void setType(CRDT_type type){
		this.type = type;
		setApbKey(type, key);
	}
	
	/**
	 * Sets the key.
	 *
	 * @param key the new key
	 */
	public void setKey(String key){
		this.key = ByteString.copyFromUtf8(key);
		setApbKey(type, this.key);
	}
	
	/**
	 * Sets the key.
	 *
	 * @param key the new key
	 */
	public void setKey(ByteString key){
		this.key = key;
		setApbKey(type, key);
	}	
}
