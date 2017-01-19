package main.java.AntidoteClient;

import java.util.List;

public interface GMapInterface {
	List<AntidoteMapEntry> getEntryList();
	
	void readDatabase();
	
	void rollBack();
	
	void synchronize();
	
	void update(String key, AntidoteMapUpdate update);
	
	void update(String key, List<AntidoteMapUpdate> update);
	
	AntidoteMapORSetEntry getORSetEntry(String key);
	
	AntidoteMapCounterEntry getCounterEntry(String key);
	
	AntidoteMapIntegerEntry getIntegerEntry(String key);
	
	AntidoteMapRWSetEntry getRWSetEntry(String key);
	
	AntidoteMapMVRegisterEntry getMVRegisterEntry(String key);
	
	AntidoteMapRegisterEntry getRegisterEntry(String key);
	
	AntidoteMapAWMapEntry getAWMapEntry(String key);
	
	AntidoteMapGMapEntry getGMapEntry(String key);
	
	void push();
}
