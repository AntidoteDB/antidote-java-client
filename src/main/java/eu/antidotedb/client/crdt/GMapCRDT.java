package eu.antidotedb.client.crdt;

import java.util.List;

import eu.antidotedb.client.AntidoteInnerAWMap;
import eu.antidotedb.client.AntidoteInnerCounter;
import eu.antidotedb.client.AntidoteInnerGMap;
import eu.antidotedb.client.AntidoteInnerInteger;
import eu.antidotedb.client.AntidoteInnerLWWRegister;
import eu.antidotedb.client.AntidoteInnerMVRegister;
import eu.antidotedb.client.AntidoteInnerORSet;
import eu.antidotedb.client.AntidoteInnerCRDT;
import eu.antidotedb.client.AntidoteInnerRWSet;
import eu.antidotedb.client.AntidoteMapUpdate;
import eu.antidotedb.client.AntidoteTransaction;

/**
 * The Interface InterfaceGMap.
 */
public interface GMapCRDT{
	
	/**
	 * Gets the entry list.
	 *
	 * @return the entry list
	 */
	List<AntidoteInnerCRDT> getEntryList();
	
	/**
	 * Update the element with the given key by applying the given update (update contains type information).
	 */
	void update(String key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction);
	
	/**
	 * Update the element with the given key by applying the given updates (update contains type information).
	 */
	void update(String key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction);
	
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
