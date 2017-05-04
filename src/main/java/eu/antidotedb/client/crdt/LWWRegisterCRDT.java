package eu.antidotedb.client.crdt;

import com.google.protobuf.ByteString;
import eu.antidotedb.client.AntidoteTransaction;

/**
 * The Interface InterfaceLWWRegister.
 */
public interface LWWRegisterCRDT {

    /**
     * Gets the value.
     *
     * @return the value
     */
    String getValue();

    /**
     * Gets the value as ByteString.
     *
     * @return the value BS
     */
    ByteString getValueBS();

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
