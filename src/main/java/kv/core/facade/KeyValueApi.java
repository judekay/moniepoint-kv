package kv.core.facade;

import java.io.IOException;
import java.util.Map;

public interface KeyValueApi {
    void put(String key, String value) throws IOException;
    String read(String key) throws IOException;
    Map<String,String>  readKeyRange(String startKey, String endKey) throws IOException;
}
