package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapRegisterEntry.
 */
public class AntidoteMapRegisterEntry extends AntidoteMapEntry {
	
	/** The value. */
	private String value;
	
	/**
	 * Instantiates a new antidote map register entry.
	 *
	 * @param value the value
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapRegisterEntry(String value, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.value = value;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){	
		AntidoteMapRegisterEntry register;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				register = outerMap.getRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());

					}
				}
				register = innerMap.getRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = register.getValue();
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){ 
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			if (getPath().size() == 1){
				register = outerMap.getRegisterEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
				}
				register = innerMap.getRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			value = register.getValue();
		}
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue(){
		return value;
	}
	
	/**
	 * Locally set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setLocal(String value){
		this.value = value;
	}

	/**
	 * Set the register to a new value.
	 *
	 * @param value the value
	 */
	public void set(String value){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createRegisterSet(value));
		updateHelper(registerSet);
	}
}
