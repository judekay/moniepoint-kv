package kv.storage;

public record Entry(byte[] value, boolean deleted) {
}
