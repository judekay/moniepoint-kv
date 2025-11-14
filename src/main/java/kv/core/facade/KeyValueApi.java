package kv.core.facade;

import java.io.IOException;

public interface KeyValueApi {
    void put(String key, String value) throws IOException;
}
