package eu.antidotedb.client.crdt;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import eu.antidotedb.client.*;

import java.util.List;

/**
 * The Interface InterfaceAWMap.
 */
public interface AWMapCRDT {

    /**
     * Gets the entry list.
     *
     * @return the entry list
     */
    List<AntidoteInnerCRDT> getEntryList();

    /**
     * Update the entry with the given key by applying the given update (update contain type information).
     */
    void update(String key, AntidoteMapUpdate update, AntidoteTransaction antidoteTransaction);

    /**
     * Update the entry with the given key by applying the given updates (updates contain type information).
     */
    void update(String key, List<AntidoteMapUpdate> updateList, AntidoteTransaction antidoteTransaction);

    /**
     * Gets the OR set entry.
     *
     * @param key the key
     * @return the OR set entry
     */
    AntidoteInnerORSet getORSetEntry(String key);

    /**
     * Gets the counter entry.
     *
     * @param key the key
     * @return the counter entry
     */
    AntidoteInnerCounter getCounterEntry(String key);

    /**
     * Gets the integer entry.
     *
     * @param key the key
     * @return the integer entry
     */
    AntidoteInnerInteger getIntegerEntry(String key);

    /**
     * Gets the RW set entry.
     *
     * @param key the key
     * @return the RW set entry
     */
    AntidoteInnerRWSet getRWSetEntry(String key);

    /**
     * Gets the MV register entry.
     *
     * @param key the key
     * @return the MV register entry
     */
    AntidoteInnerMVRegister getMVRegisterEntry(String key);

    /**
     * Gets the register entry.
     *
     * @param key the key
     * @return the register entry
     */
    AntidoteInnerLWWRegister getLWWRegisterEntry(String key);

    /**
     * Gets the AW map entry.
     *
     * @param key the key
     * @return the AW map entry
     */
    AntidoteInnerAWMap getAWMapEntry(String key);

    /**
     * Gets the g map entry.
     *
     * @param key the key
     * @return the g map entry
     */
    AntidoteInnerGMap getGMapEntry(String key);

    /**
     * Removes the entry.
     */
    void remove(String key, CRDT_type type, AntidoteTransaction antidoteTransaction);

    /**
     * Removes the entries.
     */
    void remove(List<String> keyList, CRDT_type type, AntidoteTransaction antidoteTransaction);

}
