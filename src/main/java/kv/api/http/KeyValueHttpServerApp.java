package kv.api.http;

import kv.api.KeyValueServer;
import kv.core.facade.DefaultKeyValueFacade;
import kv.core.facade.KeyValueApi;
import kv.storage.LsmStorageEngine;

import java.io.File;

public class KeyValueHttpServerApp {

    public static void start(int port, String dirPath) throws Exception {
        File dir = new File(dirPath);

        LsmStorageEngine lsmStorageEngine = new LsmStorageEngine(dir);
        KeyValueApi facade = new DefaultKeyValueFacade(lsmStorageEngine);
        KeyValueServer server = new KeyValueHttpServer(facade, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try{
                server.stop();
                lsmStorageEngine.close();
            } catch (Exception exception) { }
        }));
        server.start();
    }
}
