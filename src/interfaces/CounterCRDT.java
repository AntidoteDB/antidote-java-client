package interfaces;

/**
 * The Interface InterfaceCounter.
 */
public interface CounterCRDT extends CRDT{
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	int getValue();
	
	/**
	 * Increment.
	 */
	void increment();
	
	/**
	 * Increment.
	 *
	 * @param inc the inc
	 */
	void increment(int inc);
}
