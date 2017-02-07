package main.java.AntidoteClient;

import java.util.List;

/**
 * The Interface InterfaceGMap.
 */
public interface InterfaceGMap {
	
	/**
	 * Gets the entry list.
	 *
	 * @return the entry list
	 */
	List<AntidoteInnerObject> getEntryList();
	
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
	 * Update the element with the given key by applying the given update (update contains type information).
	 *
	 * @param key the key
	 * @param update the update
	 */
	void update(String key, AntidoteMapUpdate update);
	
	/**
	 * Update the element with the given key by applying the given updates (update contains type information).
	 *
	 * @param key the key
	 * @param updateList the updateList
	 */
	void update(String key, List<AntidoteMapUpdate> updateList);
	
	/**
	 * Gets the OR set entry with the given key.
	 *
	 * @param key the key
	 * @return the OR set entry
	 */
	AntidoteInnerORSet getORSetEntry(String key);
	
	/**
	 * Gets the counter entry with the given key.
	 *
	 * @param key the key
	 * @return the counter entry
	 */
	AntidoteInnerCounter getCounterEntry(String key);
	
	/**
	 * Gets the integer entry with the given key.
	 *
	 * @param key the key
	 * @return the integer entry
	 */
	AntidoteInnerInteger getIntegerEntry(String key);
	
	/**
	 * Gets the RW set entry with the given key.
	 *
	 * @param key the key
	 * @return the RW set entry
	 */
	AntidoteInnerRWSet getRWSetEntry(String key);
	
	/**
	 * Gets the MV register entry with the given key.
	 *
	 * @param key the key
	 * @return the MV register entry
	 */
	AntidoteInnerMVRegister getMVRegisterEntry(String key);
	
	/**
	 * Gets the register entry with the given key.
	 *
	 * @param key the key
	 * @return the register entry
	 */
	AntidoteInnerLWWRegister getLWWRegisterEntry(String key);
	
	/**
	 * Gets the AW map entry with the given key.
	 *
	 * @param key the key
	 * @return the AW map entry
	 */
	AntidoteInnerAWMap getAWMapEntry(String key);
	
	/**
	 * Gets the g map entry with the given key.
	 *
	 * @param key the key
	 * @return the g map entry
	 */
	AntidoteInnerGMap getGMapEntry(String key);
}
