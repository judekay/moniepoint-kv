package kv.api;

import java.io.IOException;

public interface KeyValueServer {
    void start() throws IOException;
    void stop();
}
