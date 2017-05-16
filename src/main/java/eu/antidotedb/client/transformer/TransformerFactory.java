package eu.antidotedb.client.transformer;

import eu.antidotedb.client.Connection;

/**
 * Creates transformers
 */
public interface TransformerFactory {

    /**
     * Creates a new transformer with the given downstream transformer.
     * The created Transformer will be attached to the given connection.
     */
    Transformer newTransformer(Transformer downstream, Connection connection);
}
