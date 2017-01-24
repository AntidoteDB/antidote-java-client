package main.java.AntidoteClient;

import java.util.List;

import com.google.protobuf.ByteString;

/**
 * The Interface SetInterface.
 */
public interface SetInterface {
	
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
	 * Adds the element.
	 *
	 * @param element the element
	 */
	void addElement(String element);
	
	/**
	 * Adds the elements.
	 *
	 * @param elementList the element list
	 */
	void addElement(List<String> elementList);
	
	/**
	 * Removes the element.
	 *
	 * @param element the element
	 */
	void removeElement(String element);
	
	/**
	 * Removes the elements.
	 *
	 * @param elementList the element list
	 */
	void removeElement(List<String> elementList);
	
	/**
	 * Adds the element, given as ByteString.
	 *
	 * @param element the element
	 */
	void addElementBS(ByteString element);
	
	/**
	 * Adds the elements, given as ByteString.
	 *
	 * @param elementList the element list
	 */
	void addElementBS(List<ByteString> elementList);
	
	/**
	 * Removes the elements, given as ByteString.
	 *
	 * @param element the element
	 */
	void removeElementBS(ByteString element);
	
	/**
	 * Removes the elements, given as ByteString.
	 *
	 * @param elementList the element list
	 */
	void removeElementBS(List<ByteString> elementList);
	
	/**
	 * Push locally executed updates to database. Uses a transaction.
	 */
	void push();
}
