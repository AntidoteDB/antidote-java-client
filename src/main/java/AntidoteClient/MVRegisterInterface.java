package main.java.AntidoteClient;

import java.util.List;

import com.google.protobuf.ByteString;

/**
 * The Interface MVRegisterInterface.
 */
public interface MVRegisterInterface {
	
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
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	void push();
}
