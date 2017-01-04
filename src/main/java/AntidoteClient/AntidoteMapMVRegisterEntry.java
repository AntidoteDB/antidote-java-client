package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapMVRegisterEntry.
 */
public class AntidoteMapMVRegisterEntry extends AntidoteMapEntry {

/** The value list. */
private List<String> valueList;
	
	/**
	 * Instantiates a new antidote map MV register entry.
	 *
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapMVRegisterEntry(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
		this.valueList = valueList;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){	
		AntidoteMapMVRegisterEntry mvRegister;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
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
				mvRegister = innerMap.getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = mvRegister.getValueList();
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){ 
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			if (getPath().size() == 1){
				mvRegister = outerMap.getMVRegisterEntry(getPath().get(0).getKey().toStringUtf8());
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
				mvRegister = innerMap.getMVRegisterEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			valueList = mvRegister.getValueList();
		}
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public List<String> getValueList(){
		return valueList;
	}
	
	/**
	 * Locally set the register to a new value.
	 *
	 * @param value the value
	 */
	public void setLocal(String value){
		valueList.clear();
		valueList.add(value);
	}
		
	/**
	 * Set the register to a new value.
	 *
	 * @param value the value
	 */
	public void set(String value){
		setLocal(value);
		List<AntidoteMapUpdate> registerSet = new ArrayList<AntidoteMapUpdate>(); 
		registerSet.add(getClient().createMVRegisterSet(value));
		updateHelper(registerSet);
	}
}
