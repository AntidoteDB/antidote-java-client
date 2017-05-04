package eu.antidotedb.client.crdt;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.AntidoteTransaction;

import java.util.List;

/**
 * The Interface InterfaceMVRegister.
 */
public interface MVRegisterCRDT {

    /**
     * Gets the value list.
     *
     * @return the value list
     */
    List<String> getValueList();

    /**
     * Gets the value list as ByteStrings.
     *
     * @return the value list BS
     */
    List<ByteString> getValueListBS();


    /**
     * Sets the value.
     *
     * @param value the new value
     */
    void setValue(String value, AntidoteTransaction antidoteTransaction);

    /**
     * Sets the value, given as ByteString.
     *
     * @param value the new value
     */
    void setValueBS(ByteString value, AntidoteTransaction antidoteTransaction);
}
