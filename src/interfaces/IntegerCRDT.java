package interfaces;

/**
 * The Interface InterfaceInteger.
 */
public interface IntegerCRDT extends CRDT {
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	int getValue();
	
	/**
	 * Increment by 1.
	 */
	void increment();
	
	/**
	 * Increment.
	 *
	 * @param inc the inc
	 */
	void increment(int inc);
	
	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	void setValue(int value); 
}
