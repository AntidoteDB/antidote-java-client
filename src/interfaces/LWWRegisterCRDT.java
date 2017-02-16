package interfaces;

import com.google.protobuf.ByteString;

/**
 * The Interface InterfaceLWWRegister.
 */
public interface LWWRegisterCRDT extends CRDT {
	
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
	void setValue(String value);
	
	/**
	 * Sets the value, given as ByteString.
	 *
	 * @param value the new value
	 */
	void setValueBS (ByteString value);
}
