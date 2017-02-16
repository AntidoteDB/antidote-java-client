package interfaces;

import java.util.List;

import com.google.protobuf.ByteString;

/**
 * The Interface InterfaceMVRegister.
 */
public interface MVRegisterCRDT extends CRDT{
	
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
	void setValue(String value);
	
	/**
	 * Sets the value, given as ByteString.
	 *
	 * @param value the new value
	 */
	void setValueBS(ByteString value);
}
