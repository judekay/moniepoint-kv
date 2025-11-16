# Moniepoint Key Value Store

A lightweight persistent Key/Value database built in Java using only standard libraries.

## 1 Running the project
## Requirements
- Java 17+
- Gradle(wrapper included ```./gradlew```)
- macOS/Linux/Windows

## Build and Run
```bash
./gradlew clean build
```
### Running a single node
```bash
./gradlew run -Pport=8080 -Pdir=./data-8080
```
The result should be
```terminaloutput
Moniepoint Key Value Storage Server starting on port 8081, dir=./data-8081
```
Test with curl:
```shell
curl -X PUT "http://localhost:8080/keyvalue?key=test1" -d "moniepoint"
curl "http://localhost:8080/keyvalue?key=test1"
curl "http://localhost:8080/keyvalue/range?startKey=s&endKey=v"
curl -X POST "http://localhost:8080/keyvalue/batch" -d $'test2=value2\ntest3=value3\ntest4=value4'
curl -X DELETE "http://localhost:8080/keyvalue?key=test2"
```

### Running a multi node cluster (Replication enabled)
This includes a leader -> replica(s) architecture
You run 3 terminals, each launching a node
### Leader (runs on 8080 leader node and replicates writes to followers)
```bash
./gradlew run -Pport=8080 -Pdir=./data-8080
```
### Replica 1 
```shell
./gradlew run -Pport=8081 -Pdir=./data-8081
```
### Replica 2
```shell
./gradlew run -Pport=8082 -Pdir=./data-8082
```

## Test Replication
### Write to the leader
```shell
curl -X PUT "http://localhost:8080/keyvalue?key=testMultiNode" -d "moniepointMultiNode"
```
### Read from replica
```shell
curl "http://localhost:8081/keyvalue?key=testMultiNode"

Should return moniepointMultiNode if replication is working
```

## 2 HTTP API

## 3 System Architecture
The implementation uses a Log-Structured Merge Tree (LSM-Tree) architecture, a tyoe of design used in:
Cassandra, Elasticsearch segment writes, Kafka Streams state stores

It satisfies the tasks strict constraints:
- high write throughput
- low read latency
- durable persistence
- crash recovery
- datasets larger than RAM
- predictable behavior under load

### 3.1 Write Implementation (PUT, BATCHPUT, DELETE)
#### 1. Write Ahead Log
Every write is first appended to a Write ahead log file. This enables
- append only
- sequential disk writes
- filesync after every N operations(hardcoded but can be made configurable in future iterations)
- enables crash recovery

#### 2. Memtable (in memory sorted tree)
After the write ahead log append, then we have
- key/value inserted into the mem table
- memtable is small and bounded
- extremely fast writes (O(logn))

#### 3. Sstable
When memtable reaches max size:
- sorted entries are flushed into a new immutable SSTable file
- Write ahead log is truncated
- memtable is then cleared

### 3.2 SSTable (Immutable Sorted String Table)
Each SSTable stores:
- entries sorted by key
- sparse index for fast binary lookup (every 100 keys)

Reads check:
1. Memtable
2. Write Ahead Log (replayed memtable)
3. SStables(from newest -> oldest)

### 3.3 Compaction
To keep reads fast and storage bounded, multiple SSTables are periodically merged:
- newest version of each key wins
- tombstones remove deleted entries
- output is a single SSTable
- old SSTables deleted

This ensures:
- predictable read performance
- low disk fragmentation
- efficient range lookups
- datasets larger than RAM are handled gracefully

### 3.4 Crash Recovery
On startup:
1. Load all existing SSTables
2. Replay Write Ahead log (append log)
3. Rebuild memtable from Write Ahead log
4. Continue serving data

## 4 Replication
Replication Model: Leader -> Followers
The leader forwards writes to replicas using:
- Java HttpClient
- non blocking so replication never blocks the client request in the form of fire and forget
Although HttpClient was implemented as the transport mechanism in this project, the interface is open for other transport implementation


## 5 Test
Run unit tests:
```shell
./gradlew test
```

## 6 Future Improvements
### Storage Engine
- Background compaction to keep foreground latencies stable.
- Bloom filters on SSTables to skip unnecessary disk reads.
- Block indexing & caching for lower disk IO.
- Dual memtables (active + immutable) for smoother flush behavior.

### Replication
- Health checks to avoid replicating to unhealthy nodes.
- Configurable write concern (leader-only, leader+1, all).
- Simple failover using heartbeats and replica promotion.

### Resilience & Concurrency
- Backpressure to protect the system under overload.
- Per-request timeouts to prevent long hangs.

### Observability
- Metrics of compaction time, Write ahead log write rate, latency percentile (P95, P99)
- Structured logs
- Rate limiting during peak or overloads

### Configuration
Move all hardcoded values into config

### Deployment
- Docker and Docker compose to run multi-node clusters easily
- Data directory management for safer production operation 

