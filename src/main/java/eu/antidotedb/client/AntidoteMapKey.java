package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapKey.
 */
public final class AntidoteMapKey {
	
	/** The key. */
	private final ByteString key;
	
	/** The type. */
	private final CRDT_type type;
	
	/** The apb key. */
	private final ApbMapKey apbKey;

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
}
