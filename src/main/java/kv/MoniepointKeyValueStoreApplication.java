package kv;

import kv.api.http.KeyValueHttpServerApp;

public class MoniepointKeyValueStoreApplication {
    public static void main(String[] args) throws Exception {
        //todo move the port to config later on
        int port = Integer.parseInt(System.getProperty("port", "8080"));
        String dir = System.getProperty("dir", "./data-8080");
        System.out.printf("Moniepoint Key Value Storage Server starting on port %d, dir=%s%n", port, dir);
        KeyValueHttpServerApp.start(port, dir);
    }
}
