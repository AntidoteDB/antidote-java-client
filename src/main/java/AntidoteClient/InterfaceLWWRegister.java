package main.java.AntidoteClient;

import com.google.protobuf.ByteString;

/**
 * The Interface InterfaceLWWRegister.
 */
public interface InterfaceLWWRegister {
	
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
	void setValueBS (ByteString value);
}
