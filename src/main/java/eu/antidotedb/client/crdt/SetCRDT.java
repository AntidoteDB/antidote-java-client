package eu.antidotedb.client.crdt;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.AntidoteTransaction;

import java.util.List;
import java.util.Set;

/**
 * The Interface InterfaceSet.
 */
public interface SetCRDT {

    /**
     * Gets the value list.
     *
     * @return the value list
     */
    Set<String> getValues();

    /**
     * Gets the value list as ByteStrings.
     *
     * @return the value list BS
     */
    Set<ByteString> getValuesBS();

    /**
     * Adds the element.
     */
    void addElement(String element, AntidoteTransaction antidoteTransaction);

    /**
     * Adds the elements.
     */
    void addElement(List<String> elementList, AntidoteTransaction antidoteTransaction);

    /**
     * Removes the element.
     */
    void removeElement(String element, AntidoteTransaction antidoteTransaction);

    /**
     * Removes the elements.
     */
    void removeElement(List<String> elementList, AntidoteTransaction antidoteTransaction);

    /**
     * Adds the element, given as ByteString.
     */
    void addElementBS(ByteString element, AntidoteTransaction antidoteTransaction);

    /**
     * Adds the elements, given as ByteString.
     */
    void addElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction);

    /**
     * Removes the elements, given as ByteString.
     */
    void removeElementBS(ByteString element, AntidoteTransaction antidoteTransaction);

    /**
     * Removes the elements, given as ByteString.
     */
    void removeElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction);
}
