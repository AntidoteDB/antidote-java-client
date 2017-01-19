package main.java.AntidoteClient;

import java.util.List;

public interface AWMapInterface {
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
	
	void removeORSet(List<String> keyList);
	
	void removeRWSet(List<String> keyList);
	
	void removeCounter(List<String> keyList);
	
	void removeInteger(List<String> keyList);
	
	void removeMVRegister(List<String> keyList);
	
	void removeRegister(List<String> keyList);
	
	void removeAWMap(List<String> keyList);
	
	void removeGMap(List<String> keyList);
	
	void removeORSet(String key);
	
	void removeRWSet(String key);
	
	void removeCounter(String key);
	
	void removeInteger(String key);
	
	void removeMVRegister(String key);
	
	void removeRegister(String key);
	
	void removeAWMap(String key);
	
	void removeGMap(String key);
	
	void push();
}
