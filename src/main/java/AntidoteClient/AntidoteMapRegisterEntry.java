package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapRegisterEntry.
 */
public class AntidoteMapRegisterEntry extends AntidoteMapEntry implements RegisterInterface{
	
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
	
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	public void synchronize(){
		push();
		readDatabase();
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){	
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
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
	
	public ByteString getValueBS(){
		return ByteString.copyFromUtf8(value);
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
	public void setValue(String value){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createRegisterSet(value));
		updateHelper(registerSet);
	}
	
	public void setValue(ByteString value){
		setLocal(value.toStringUtf8());
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createRegisterSet(value.toStringUtf8()));
		updateHelper(registerSet);
	}
}
