package kv.replication;

import kv.core.facade.KeyValueApi;

import java.io.IOException;
import java.util.Map;

public class ReplicationKeyValueApi implements kv.core.facade.KeyValueApi {

    private final KeyValueApi keyValueApi;
    private final ReplicationClient replicationClient;

    public ReplicationKeyValueApi(KeyValueApi local, ReplicationClient replicationClient) {
        this.keyValueApi = local;
        this.replicationClient = replicationClient;
    }

    @Override
    public void put(String key, String value) throws IOException {
        keyValueApi.put(key, value);
        replicationClient.replicatePut(key, value);
    }

    @Override
    public String read(String key) throws IOException {
        return keyValueApi.read(key);
    }

    @Override
    public Map<String, String> readKeyRange(String startKey, String endKey) throws IOException {
        return keyValueApi.readKeyRange(startKey, endKey);
    }

    @Override
    public void batchPut(Map<String, String> entries) throws IOException {
        keyValueApi.batchPut(entries);
        replicationClient.replicateBatchPut(entries);
    }

    @Override
    public void delete(String key) throws IOException {
        keyValueApi.delete(key);
        replicationClient.replicateDelete(key);
    }
}
