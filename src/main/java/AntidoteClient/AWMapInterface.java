package main.java.AntidoteClient;

import java.util.List;

/**
 * The Interface AWMapInterface.
 */
public interface AWMapInterface {

/**
 * Gets the entry list.
 *
 * @return the entry list
 */
List<AntidoteMapEntry> getEntryList();
	
	/**
	 * Read database.
	 */
	void readDatabase();
	
	/**
	 * Roll back: delete information about local updates and read database.
	 */
	void rollBack();
	
	/**
	 * Synchronize: first push own changes, then read database.
	 */
	void synchronize();
	
	/**
	 * Update the entry with the given key by applying the given update (update contain type information).
	 *
	 * @param key the key
	 * @param update the update
	 */
	void update(String key, AntidoteMapUpdate update);
	
	/**
	 * Update the entry with the given key by applying the given updates (updates contain type information).
	 *
	 * @param key the key
	 * @param update the update
	 */
	void update(String key, List<AntidoteMapUpdate> updateList);
	
	/**
	 * Gets the OR set entry.
	 *
	 * @param key the key
	 * @return the OR set entry
	 */
	AntidoteMapORSetEntry getORSetEntry(String key);
	
	/**
	 * Gets the counter entry.
	 *
	 * @param key the key
	 * @return the counter entry
	 */
	AntidoteMapCounterEntry getCounterEntry(String key);
	
	/**
	 * Gets the integer entry.
	 *
	 * @param key the key
	 * @return the integer entry
	 */
	AntidoteMapIntegerEntry getIntegerEntry(String key);
	
	/**
	 * Gets the RW set entry.
	 *
	 * @param key the key
	 * @return the RW set entry
	 */
	AntidoteMapRWSetEntry getRWSetEntry(String key);
	
	/**
	 * Gets the MV register entry.
	 *
	 * @param key the key
	 * @return the MV register entry
	 */
	AntidoteMapMVRegisterEntry getMVRegisterEntry(String key);
	
	/**
	 * Gets the register entry.
	 *
	 * @param key the key
	 * @return the register entry
	 */
	AntidoteMapRegisterEntry getRegisterEntry(String key);
	
	/**
	 * Gets the AW map entry.
	 *
	 * @param key the key
	 * @return the AW map entry
	 */
	AntidoteMapAWMapEntry getAWMapEntry(String key);
	
	/**
	 * Gets the g map entry.
	 *
	 * @param key the key
	 * @return the g map entry
	 */
	AntidoteMapGMapEntry getGMapEntry(String key);
	
	/**
	 * Removes the OR sets with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeORSet(List<String> keyList);
	
	/**
	 * Removes the RW sets with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeRWSet(List<String> keyList);
	
	/**
	 * Removes the counters with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeCounter(List<String> keyList);
	
	/**
	 * Removes the integers with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeInteger(List<String> keyList);
	
	/**
	 * Removes the MV registers with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeMVRegister(List<String> keyList);
	
	/**
	 * Removes the registers with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeRegister(List<String> keyList);
	
	/**
	 * Removes the AW maps with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeAWMap(List<String> keyList);
	
	/**
	 * Removes the G maps with the given keys.
	 *
	 * @param keyList the key list
	 */
	void removeGMap(List<String> keyList);
	
	/**
	 * Removes the OR set with the given key.
	 *
	 * @param key the key
	 */
	void removeORSet(String key);
	
	/**
	 * Removes the RW set with the given key.
	 *
	 * @param key the key
	 */
	void removeRWSet(String key);
	
	/**
	 * Removes the counter with the given key.
	 *
	 * @param key the key
	 */
	void removeCounter(String key);
	
	/**
	 * Removes the integer with the given key.
	 *
	 * @param key the key
	 */
	void removeInteger(String key);
	
	/**
	 * Removes the MV register with the given key.
	 *
	 * @param key the key
	 */
	void removeMVRegister(String key);
	
	/**
	 * Removes the register with the given key.
	 *
	 * @param key the key
	 */
	void removeRegister(String key);
	
	/**
	 * Removes the AW map with the given key.
	 *
	 * @param key the key
	 */
	void removeAWMap(String key);
	
	/**
	 * Removes the G map with the given key.
	 *
	 * @param key the key
	 */
	void removeGMap(String key);
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	void push();
}
