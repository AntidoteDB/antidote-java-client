package interfaces;
import java.util.Set;
import java.util.List;
import com.google.protobuf.ByteString;

/**
 * The Interface InterfaceSet.
 */
public interface SetCRDT extends CRDT {
	
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
}
