/**
 * Transformers can be used to integrate middleware into the client.
 * <p>
 * The package contains examples for logging requests ({@link eu.antidotedb.client.transformer.LogTransformer})
 * and for collecting simple simple statistics on the number of requests ({@link eu.antidotedb.client.transformer.CountingTransformer})
 * <p>
 * Transformers can be specified when creating a new {@link eu.antidotedb.client.AntidoteClient} (see corresponding constructors).
 */
package eu.antidotedb.client.transformer;