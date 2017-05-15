package eu.antidotedb.client.transformer;

/**
 * Creates transformers
 */
public interface TransformerFactory {

    /**
     * Creates a new transformer with the given downstream transformer
     */
    Transformer newTransformer(Transformer downstream);
}
