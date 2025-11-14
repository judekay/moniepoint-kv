# Moniepoint Key Value Store

A lightweight persistent Key/Value database built in Java using only standard libraries.

## Requirements
- Java 17
- Gradle

## Build and Run
```bash
./gradlew clean build
./gradlew run
```

## Features
- PUT, READ, READKEYRANGE, BATCHPUT, DELETE operations
- Focus on low latency and crash safety

## Development Plan
### 1. Project setup & architecture
- Setup project skeleton, Gradle setup, and entrypoint.
- Introduced clean layered structure:
    - `api/` (transport/server)
    - `core/` (facade / LSM engine)
    - `storage/` (storage interfaces)

### 2. LSM write path (PUT) – **Completed**
- Implemented `StorageEngine` interface.
- Implemented core LSM components:
    - `Write Ahead Log` (append-only durability log)
    - `MemTable` (in-memory sorted writes)
    - `SstableWriter` + `SstableHandler` (on-disk immutable segments)
- `LsmStorageEngine.put()` now:
    - writes to WAL → updates MemTable → flushes to SSTable when full.
### 3. API layer – **Completed**
- Added `KeyValueApi` interface (string-friendly contract).
- Added `DefaultKeyValueFacade` to convert `String <-> byte[]` and delegate to storage.

### 4. Transport/server layer – **Completed**
- Introduced `KeyValueServer` interface for pluggable transports.
- Implemented `KeyValueHttpServer` exposing:
    - `PUT /keyvalue?key=...`
- `HttpServerApp` wires storage engine → facade → server.

### Next Steps
- Implement `READ`, `READKEYRANGE`, `BATCHPUT`, `DELETE` (with tombstones).
- Add SSTable read path and simple compaction.
- Add tests, metrics.
- Add optional replication.
- Add design documentation and api doc