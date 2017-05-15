package eu.antidotedb.client.transformer;


import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;

/**
 * Transforms an Antidote request. Is again an Antidote request transformer.
 */
public interface Transformer extends AntidoteRequest.Handler<AntidoteResponse> {

}
