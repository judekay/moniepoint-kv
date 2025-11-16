package kv.api.http;

import kv.api.KeyValueServer;
import kv.core.facade.DefaultKeyValueFacade;
import kv.core.facade.KeyValueApi;
import kv.replication.ReplicationClient;
import kv.replication.ReplicationKeyValueApi;
import kv.storage.LsmStorageEngine;

import java.io.File;
import java.util.List;

public class KeyValueHttpServerApp {

    public static void start(int port, String dirPath) throws Exception {
        File dir = new File(dirPath);

        LsmStorageEngine lsmStorageEngine = new LsmStorageEngine(dir);
        KeyValueApi facade = new DefaultKeyValueFacade(lsmStorageEngine);

        List<String> replicaUrls = List.of(
                "http://localhost:8081",
                "http://localhost:8082"
        );
        KeyValueApi api;

        ReplicationClient replicator = new ReplicationClient(replicaUrls);
        api = new ReplicationKeyValueApi(facade, replicator);
        KeyValueServer server = new KeyValueHttpServer(api, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try{
                server.stop();
                lsmStorageEngine.close();
            } catch (Exception exception) { }
        }));
        server.start();
    }
}
