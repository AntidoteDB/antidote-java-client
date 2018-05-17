package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;

import java.util.Collections;
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
                if (resp == null) {
                    return 0;
                } else if (resp.getCounter() == null) {
                    throw new AntidoteException("Invalid response " + resp);
                }
                return resp.getCounter().getValue();
            }
        };
    }

    public static <T> ResponseDecoder<T> register(ValueCoder<T> format) {
        return new ResponseDecoder<T>() {
            @Override
            T readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                if (resp == null) {
                    return null;
                } else if (!resp.hasReg()) {
                    throw new AntidoteException("Invalid response " + resp);
                }
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
                if (resp == null) {
                    return Collections.emptyList();
                } else if (!resp.hasMvreg()) {
                    throw new AntidoteException("Invalid response " + resp);
                }
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
                if (resp == null) {
                    return Collections.emptyList();
                } else if (!resp.hasSet()) {
                    throw new AntidoteException("Invalid response " + resp);
                }
                return format.decodeList(resp.getSet().getValueList());
            }
        };
    }

    public static ResponseDecoder<List<String>> set() {
        return set(ValueCoder.utf8String);
    }


    public static <K> ResponseDecoder<MapKey.MapReadResult> map() {
        return new ResponseDecoder<MapKey.MapReadResult>() {
            @Override
            MapKey.MapReadResult readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                if (resp == null) {
                    return new MapKey.MapReadResult(Collections.emptyList());
                } else if (!resp.hasMap()) {
                    throw new AntidoteException("Invalid response " + resp);
                }
                return new MapKey.MapReadResult(resp.getMap().getEntriesList());
            }
        };
    }

    public static ResponseDecoder<Boolean> flag() {
        return new ResponseDecoder<Boolean>() {
            @Override
            Boolean readResponseToValue(AntidotePB.ApbReadObjectResp resp) {
                if (resp == null) {
                    return false;
                } else if (!resp.hasFlag()) {
                    throw new AntidoteException("Invalid response " + resp);
                }
                return resp.getFlag().getValue();
            }
        };
    }

}
