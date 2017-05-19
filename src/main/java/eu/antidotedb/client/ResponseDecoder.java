package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

import java.util.List;

/**
 * Can decode a response for a read request
 */
public abstract class ResponseDecoder<Value> {
    abstract Value readResponseToValue(AntidotePB.ApbReadObjectResp resp);
    
    
    public static ResponseDecoder<Integer> counter() {
        return new ResponseDecoder<Integer>() {
            @Override
            Integer readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return resp.getCounter().getValue();
            }
        };
    }


    public static ResponseDecoder<Long> integer() {
        return new ResponseDecoder<Long>() {
            @Override
            Long readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return resp.getInt().getValue();
            }
        };
    }

    public static <T> ResponseDecoder<T> register(ValueCoder<T> format) {
        return new ResponseDecoder<T>() {
            @Override
            T readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return format.decode(resp.getReg().getValue());
            }
        };
    }

    public static ResponseDecoder<String> register() {
        return register(ValueCoder.utf8String);
    }

    public static <T> ResponseDecoder<List<T>> multiValueRegister(ValueCoder<T> format) {
        return new ResponseDecoder<List<T>>() {
            @Override
            List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return format.decodeList(resp.getMvreg().getValuesList());
            }
        };
    }

    public static ResponseDecoder<List<String>> multiValueRegister() {
        return multiValueRegister(ValueCoder.utf8String);
    }

    public static <T> ResponseDecoder<List<T>> set(ValueCoder<T> format) {
        return new ResponseDecoder<List<T>>() {
            @Override
            List<T> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return format.decodeList(resp.getSet().getValueList());
            }
        };
    }

    public static ResponseDecoder<List<String>> set() {
        return set(ValueCoder.utf8String);
    }


    public static <K> ResponseDecoder<MapRef.MapReadResult<K>> map(ValueCoder<K> keyCoder) {
        return new ResponseDecoder<MapRef.MapReadResult<K>>() {
            @Override
            MapRef.MapReadResult<K> readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                return new MapRef.MapReadResult<>(resp.getMap().getEntriesList(), keyCoder);
            }
        };
    }

    public static ResponseDecoder<MapRef.MapReadResult<String>> map() {
        return map(ValueCoder.utf8String);
    }

}
