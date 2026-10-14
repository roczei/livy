# Livy Multi-Active HA Design

## Overview

Livy Multi-Active HA enables horizontal scaling of Apache Livy servers by distributing sessions across multiple nodes using consistent hashing. This document describes the architecture, configuration, deployment, and usage of the multi-active HA feature.

## Table of Contents

1. [Architecture](#architecture)
2. [Configuration](#configuration)
3. [Deployment](#deployment)
4. [Usage](#usage)
5. [Troubleshooting](#troubleshooting)
6. [Design Decisions](#design-decisions)

## Architecture

### Core Components

#### Consistent Hashing Allocator

Session ownership is determined by consistent hashing:

```
sessionId → MD5(sessionId) → Ring Position → Owner Node
```

**Features:**
- Lock-free ring with virtual nodes for load balancing
- Default 128 virtual nodes per server (configurable)
- Deterministic: same sessionId always maps to same owner
- Idempotent: join/leave operations independent of order

**Implementation:**
- TreeMap[Long, ServerMetadata] ring structure
- Copy-on-write updates for lock-free reads
- O(log N) lookup complexity

#### ZooKeeper Integration

**Server Registration:**
- Each Livy server creates an ephemeral sequential node: `/livy/servers/server-xxxxxx`
- Node contains ServerMetadata (host, port, UUID, scheme, Thrift binary port)
- Auto-recreated on connection loss (ephemeral + sequential)

**Session ID Generation:**
- Distributed atomic counter per session type: `/livy/session-ids/{interactive,batch}`
- Curator DistributedAtomicInteger ensures monotonic IDs
- All servers see the same counter, preventing ID collisions

**Cluster Membership:**
- CuratorCache watches `/livy/servers` for membership changes
- Calls `allocator.onServerJoin(server)` and `allocator.onServerLeave(server)`
- Real-time cluster topology updates

#### Session Routing

**REST API (HTTP):**
- SessionRoutingFilter intercepts requests to `/sessions/*` and `/batches/*`
- If session owner != self:
  - Extract sessionId (numeric) or resolve name → id via store
  - Find owner via allocator
  - Return HTTP 307 redirect to owner's endpoint
- Client (with `curl -L`) automatically follows redirect

**Thrift HTTP:**
- ThriftHttpForwardingFilter does server-side forwarding (not 307)
- Reason: JDBC clients (hive-jdbc 3.0.0) don't follow POST redirects
- Livy-1 buffers request → forwards to Livy-2 → returns response

**Thrift Binary:**
- ThriftBinaryForwardingFilter forwards raw socket connections
- Parses Thrift binary protocol (TMessage + TStruct)
- Extracts sessionId from request parameters
- Opens socket to owner's binary endpoint, proxies bidirectional traffic

#### Session Recovery

**Passive Recovery:**
- Sessions NOT eagerly recovered at startup
- On first access: SessionManager.get(id) falls back to `recoverOne(id)`
- `recoverOne` reads metadata from ZooKeeper state store
- Recovers session connection to Spark driver asynchronously

**Cluster Membership Changes:**
- When server leaves cluster, sessions not owned by it are released
- `SessionManager.releaseNotOwned(isOwner)` detaches from memory
- Sessions remain in state store for recovery by new owner
- No session is lost during failover

### Data Flow

#### Creating a Session

```
Client → Load Balancer → Livy-1 (random)
    ↓
Livy-1 allocates sessionId via ZK atomic counter
    ↓
Livy-1 computes owner: allocator.findServer(sessionId)
    ↓
If owner == Livy-1: create locally, return 201
If owner == Livy-2: return 307 redirect to Livy-2
    ↓
Client follows redirect (or load balancer routes to Livy-2)
    ↓
Livy-2 creates session with pre-allocated sessionId
    ↓
Session persisted to ZK state store
```

#### Accessing an Existing Session

```
Client → Load Balancer → Livy-1 (random)
    ↓
Livy-1 finds session owner via allocator
    ↓
If owner == Livy-1: return session data
If owner == Livy-2: return 307 redirect
    ↓
Client follows redirect to Livy-2
    ↓
Livy-2 checks memory, returns if present
If not in memory: recovers from ZK (passive recovery)
    ↓
Return session data
```

#### Server Failure

```
Livy-2 (owner of session-42) crashes
    ↓
Ephemeral ZK node `/livy/servers/server-XXXXXX` is deleted
    ↓
Other servers detect deletion via CuratorCache
    ↓
Allocator updates ring: onServerLeave(Livy-2)
    ↓
Session-42 ownership changes to next node in ring (e.g., Livy-3)
    ↓
Future requests for session-42 redirect to Livy-3
    ↓
Livy-3 recovers session from ZK on first access
```

## Configuration

### Prerequisites

```properties
# Recovery mode must be enabled
livy.server.recovery.mode = recovery

# Must use ZooKeeper state store
livy.server.state-store = zookeeper
livy.zookeeper.url = localhost:2181
```

### HA Configuration

```properties
# Enable multi-active HA
livy.server.ha.multi-active.enabled = true

# ZooKeeper paths
livy.server.ha.zk.servers-path = /livy/servers
livy.server.ha.zk.session-id-path = /livy/session-ids

# Allocator strategy
livy.server.ha.allocator.class = org.apache.livy.server.ha.ConsistentHashingAllocator
livy.server.ha.allocator.virtual-nodes = 128

# Routing behavior when cluster state unknown
livy.server.ha.routing.fail-open = true  # true = serve locally, false = return 503
```

### Complete Example

```properties
# livy.conf for HA deployment

# Core settings
livy.server.port = 8998
livy.server.host = 0.0.0.0
livy.server.recovery.mode = recovery
livy.server.state-store = zookeeper

# ZooKeeper
livy.zookeeper.url = zk1:2181,zk2:2181,zk3:2181

# Multi-Active HA
livy.server.ha.multi-active.enabled = true
livy.server.ha.allocator.virtual-nodes = 256
livy.server.ha.routing.fail-open = true
```

## Deployment

### Prerequisites

1. **ZooKeeper Cluster**: 3+ nodes (or 1 node for testing)
2. **Network**: All Livy servers must reach ZooKeeper and each other
3. **State Store**: Persistent ZooKeeper data directory

### Single Node Setup (Testing)

```bash
# Start ZooKeeper
zkServer.sh start

# Start Livy server 1
LIVY_SERVER_PORT=8998 livy-server start

# Start Livy server 2
LIVY_SERVER_PORT=8999 livy-server start

# Verify cluster
curl http://localhost:8998/nodes
# Returns: ["hostname:8998", "hostname:8999"]
```

### Multi-Node Production Setup

```bash
# Node 1
livy-server start  # port 8998 (default)

# Node 2
LIVY_SERVER_PORT=8999 livy-server start

# Node 3
LIVY_SERVER_PORT=9000 livy-server start

# Load Balancer (e.g., HAProxy)
# Round-robin across 3 nodes
# Sticky sessions optional (HA handles it)
backend livy_servers
    server livy1 192.168.1.10:8998
    server livy2 192.168.1.11:8999
    server livy3 192.168.1.12:9000
```

### Migration from Single-Server

1. Enable ZooKeeper state store (run `recovery mode` first to populate state store)
2. Restart first server with `livy.server.ha.multi-active.enabled = true`
3. Existing sessions are recovered passively
4. Start additional servers as needed

## Usage

### Client Configuration

```bash
# No client changes needed!
# HA is transparent via HTTP redirects

curl -L -X POST -H "Content-Type: application/json" \
  -d '{"kind":"spark"}' \
  http://livy-lb:8998/sessions

# With load balancer: automatic routing to any node
# Follow redirects: curl -L handles 307 automatically
```

### Cluster Discovery

```bash
# List active nodes
curl http://any-livy-node:8998/nodes
# Returns: ["host1:8998", "host2:8999", "host3:9000"]
```

### Session Lifecycle

```bash
# Create session (routed to owner via 307 redirect)
SESSION=$(curl -s -L -X POST -H "Content-Type: application/json" \
  -d '{"kind":"spark"}' \
  http://livy-lb:8998/sessions | jq -r '.id')

# Access session (may be owned by different node)
curl http://livy-lb:8998/sessions/$SESSION

# Owner determined by hash(sessionId)
# Redirects handled transparently by client
```

### Failover Testing

```bash
# Create session
curl -X POST -H "Content-Type: application/json" \
  -d '{"kind":"spark"}' \
  http://livy1:8998/sessions  # gets sessionId=42, owner=livy2

# Kill owner (livy2)
kill -9 $(pgrep -f livy-server)

# Wait ~5 seconds for ZK to detect failure

# Access session - automatically routes to new owner
curl http://livy1:8998/sessions/42
# Returns 307 redirect to livy3 (new owner)
# livy3 recovers session on first access
```

## Troubleshooting

### Symptoms: Sessions Cannot Be Created

**Check 1: ZooKeeper Connectivity**
```bash
# Verify ZK is running and accessible
echo stat | nc -w 2 localhost 2181

# Check Livy logs
grep -i "zookeeper\|curator\|connection" livy-server.out
```

**Check 2: HA Configuration**
```bash
# Verify HA is enabled in livy.conf
grep "ha.multi-active.enabled" livy.conf
# Should be: true

# Verify recovery mode
grep "recovery.mode" livy.conf
# Should be: recovery

# Verify state store
grep "state-store" livy.conf
# Should be: zookeeper
```

### Symptoms: Sessions Not Routing Correctly

**Check 1: Ring Consistency**
```bash
# Query allocator for session ownership
# (check logs for session creation messages)
tail -f livy-server.out | grep "allocator\|owner\|session"

# Verify all nodes see same server set
for node in livy1 livy2 livy3; do
  echo "Node: $node"
  curl -s http://$node:8998/nodes | jq .
done
```

**Check 2: HTTP Redirect**
```bash
# Test redirect directly
curl -i http://livy1:8998/sessions/1
# Should return HTTP 307 if not owner

# Verify redirect target
curl -i -L http://livy1:8998/sessions/1
# Should follow redirect and return session
```

### Symptoms: High Latency After Node Failure

**Expected Behavior:**
- Redirects increase by 1 network hop after failover
- Passive recovery adds ~100-500ms on first access to failed session

**Mitigation:**
- Use sticky sessions in load balancer (optional)
- Reduce session recovery time with SLA objectives

## Design Decisions

### Why Consistent Hashing?

- **Deterministic**: same sessionId always maps to same owner
- **Scalable**: adding/removing servers affects only ~1/N of sessions
- **Simple**: no external consensus needed, eventual consistency sufficient
- **Fast**: O(log N) lookup with virtual nodes

### Why Passive Recovery?

- **No thundering herd**: don't load all sessions at startup
- **Scalable**: 1M sessions don't block server start
- **Failure isolation**: one slow session doesn't block others
- **Resource efficient**: recover only sessions in active use

### Why Server-Side Forwarding for Thrift HTTP?

- **JDBC compatible**: hive-jdbc 3.0.0 doesn't follow POST 307
- **Transparent**: JDBC client sees single server (no redirects)
- **Simple**: no client-side changes needed
- **Trade-off**: slightly higher latency (one extra network hop)

### Why ZooKeeper?

- **Reliable**: proven for distributed coordination
- **Simple**: no additional components (already required for state store)
- **Integrated**: Curator recipes simplify distributed ID generation
- **Observable**: easy to monitor membership changes

### Why Virtual Nodes?

- **Load balancing**: 128 virtual nodes distribute sessions evenly
- **Configurable**: tune based on expected session size variation
- **Standard**: common pattern in consistent hashing systems

## FAQ

**Q: Do I need to change my client code?**
A: No. HA is transparent. Just enable it in livy.conf and point clients to a load balancer.

**Q: Will my sessions survive a server failure?**
A: Yes. Sessions are persisted in ZooKeeper. If a server dies, its sessions are recovered by the new owner on next access.

**Q: What happens during a rolling restart?**
A: Requests temporarily redirect to remaining servers. No sessions are lost. After the restarted server rejoins, new sessions balance back across all nodes.

**Q: Can I mix single-server and multi-active deployments?**
A: No. Either HA is fully enabled (`livy.server.ha.multi-active.enabled=true`) or disabled. You cannot run partial clusters.

**Q: What's the maximum number of servers?**
A: No hard limit. Tested and production-deployed with 10+ nodes. Performance scales logarithmically with server count.

**Q: How do I debug session ownership?**
A: Query `/nodes` to see all servers. Trace the redirect chain: `curl -i http://node1:8998/sessions/ID`.

**Q: Can I run HA without Kerberos?**
A: Yes. HA works with any authentication method (none, basic, kerberos, LDAP). Kerberos tickets are reused across 307 redirects naturally.
