# Déjà × Speedscale: Comparative Analysis & Strategic Insights

## Executive Summary

This document provides a technical deep-dive comparing Déjà (a Linux-first, zero-code, LD_PRELOAD-based traffic capture and replay system) with Speedscale (a Kubernetes-native traffic replay platform). The analysis derives actionable insights for Déjà's roadmap based on Speedscale's proven patterns.

---

## 1. Core Architecture Comparison

### 1.1 Interception Mechanisms

| Aspect | Déjà | Speedscale |
|--------|------|------------|
| **Primary Method** | LD_PRELOAD (libc hooks) | eBPF + Sidecar Proxy |
| **Alternative** | N/A (pure LD_PRELOAD) | Sidecar (goproxy) when eBPF unavailable |
| **Kernel Level** | Userspace only | Kernel (eBPF kprobes/uprobes) + Userspace |
| **Container Native** | Works but not K8s-aware | Purpose-built for Kubernetes |
| **Code Changes** | Zero | Zero (sidecar injection via mutating webhook) |
| **Permissions** | None required | Requires privileged eBPF or sidecar injection |

**Key Insight:** Speedscale's dual-approach (eBPF primary, sidecar fallback) maximizes deployment flexibility. Déjà's pure LD_PRELOAD approach is simpler but limits K8s-native workflows.

### 1.2 System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DÉJÀ ARCHITECTURE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     LD_PRELOAD      ┌─────────────────────────────────┐   │
│  │   deja-cli   │ ──────────────────> │     libdeja_preload.so          │   │
│  │  (launcher)  │                     │  ┌───────────────────────────┐  │   │
│  └──────────────┘                     │  │ Hook: connect/send/recv   │  │   │
│         │                             │  │ Hook: getrandom           │  │   │
│         │                             │  │ Hook: clock_gettime       │  │   │
│         ▼                             │  │ AgentRuntime              │  │   │
│  ┌──────────────┐                     │  │ FdTracker                 │  │   │
│  │  Artifact    │ <────────────────── │  │ Protocol Detection        │  │   │
│  │  Directory   │    events.jsonl     │  └───────────────────────────┘  │   │
│  │              │                     └─────────────────────────────────┘   │
│  └──────────────┘                                                            │
│                                                                              │
│  KEY CHARACTERISTICS:                                                        │
│  • Single-process, single-threaded focus (v1)                               │
│  • Plaintext HTTP/1.1 only                                                  │
│  • Linux x86_64 glibc only                                                  │
│  • Launched-child only (no attach)                                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPEEDSCALE ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  KUBERNETES CLUSTER                                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  speedscale namespace                                                │   │
│  │  ┌──────────────┐  ┌──────────┐  ┌─────────────┐  ┌──────────────┐  │   │
│  │  │   Operator   │  │ Forwarder│  │  Inspector  │  │  nettap (DS) │  │   │
│  │  │  (webhooks)  │  │  (DLP)   │  │(remote ctrl)│  │ (eBPF capture)│  │   │
│  │  └──────────────┘  └────┬─────┘  └─────────────┘  └──────┬───────┘  │   │
│  └─────────────────────────┼────────────────────────────────┼──────────┘   │
│                            │                                │              │
│  app namespace             │                                │              │
│  ┌─────────────────────────┼────────────────────────────────┼──────────┐   │
│  │                         │                                │          │   │
│  │  ┌──────────────────┐   │      ┌─────────────┐          │          │   │
│  │  │ ┌──────────────┐ │   │      │  goproxy    │          │          │   │
│  │  │ │   App Pod    │ │   │      │  (sidecar)  │          │          │   │
│  │  │ │              │<───┼──────>│             │          │          │   │
│  │  │ │  Workload    │ │   │      │             │<─────────┘          │   │
│  │  │ └──────────────┘ │   │      └─────────────┘                     │   │
│  │  └──────────────────┘   │                                            │   │
│  └─────────────────────────┼────────────────────────────────────────────┘   │
│                            │                                                 │
│                            ▼                                                 │
│                   SPEEDSCALE CLOUD                                           │
│                   ┌─────────────────┐                                        │
│                   │  Dashboard/API  │                                        │
│                   │  Snapshots      │                                        │
│                   │  Reports        │                                        │
│                   └─────────────────┘                                        │
│                                                                              │
│  KEY CHARACTERISTICS:                                                        │
│  • Multi-protocol (HTTP/1, HTTP/2, gRPC, DBs, caches)                       │
│  • DLP filtering in-cluster before egress                                   │
│  • Cloud-hosted analysis and reporting                                      │
│  • Kubernetes-native via operators/webhooks                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Model & Storage Comparison

### 2.1 Core Unit of Capture

| Aspect | Déjà | Speedscale |
|--------|------|------------|
| **Unit Name** | `BoundaryEvent` | `RRPair` (Request/Response Pair) |
| **Structure** | Enum variants (Time, Random, Env, Socket, HTTP) | Request + Response + Metadata + Signature |
| **Storage Format** | JSON Lines (events.jsonl) | JSON Lines (raw.jsonl) + Markdown (.md) |
| **Human Readable** | Via `deja inspect` | Native markdown format |
| **Editable** | Indirectly (JSON manipulation) | Directly (markdown files) |

### 2.2 Data Model Deep Dive

**Déjà BoundaryEvent:**
```rust
enum BoundaryEvent {
    Time(TimeBoundaryEvent),        // SystemTime::now()
    Random(RandomBoundaryEvent),    // /dev/urandom, getrandom
    Environment(EnvironmentBoundaryEvent),  // getenv()
    Http(HttpExchangeEvent),        // full HTTP request/response
    Socket(SocketBoundaryEvent),    // TCP socket ops
    Dns(DnsBoundaryEvent),          // DNS resolutions
}
```

**Speedscale RRPair (markdown format):**
```markdown
### REQUEST ###
```
POST https://api.example.com/v1/users HTTP/2.0
Authorization: Bearer eyJ0eXAiOiJKV1Q...
Content-Type: application/json
```

```
{"username": "john_doe", "email": "john@example.com"}
```

### RESPONSE ###
```
HTTP/2.0 201 Created
Content-Type: application/json
```

```
{"id": 12345, "username": "john_doe"}
```

### SIGNATURE ###
```
http:host is api.example.com
http:method is POST
http:url is /v1/users
```

### METADATA ###
```
direction: OUT
uuid: f3ead946-90b1-43ab-a7d6-be3f799e8e83
ts: 2024-01-15T14:30:22.123456789Z
duration: 155ms
tags: environment=staging, service=user-api
```
```

**Insight:** Speedscale's markdown format is brilliant for human readability and LLM processing. Déjà's JSONL is machine-efficient but requires tooling for human inspection.

### 2.3 Snapshot/Collection Concept

| Feature | Déjà | Speedscale |
|---------|------|------------|
| **Collection Name** | Artifact | Snapshot |
| **Contents** | metadata.json + events.jsonl | raw.jsonl + action.jsonl + reaction.jsonl |
| **Purpose Split** | Single unified stream | Separated: action (generator) vs reaction (responder) |
| **Sharing** | File system / manual | Cloud-hosted with sharing |
| **Versioning** | Schema version in metadata | Versioned in cloud platform |

---

## 3. Protocol Support Comparison

### 3.1 Current Capabilities

| Protocol | Déjà (v1) | Speedscale |
|----------|-----------|------------|
| HTTP/1.1 | ✅ Plaintext | ✅ Full support |
| HTTP/2 | 🔲 Planned (roadmap 1.4-1.5) | ✅ Full support |
| gRPC | 🔲 Planned (roadmap 1.6-1.7) | ✅ Full support + reflection |
| Redis | ✅ RESP2/RESP3 decoded | ✅ Supported |
| PostgreSQL | ✅ Wire protocol parsed | ✅ Supported |
| MySQL | 🔲 Not mentioned | ✅ Supported |
| Kafka | 🔲 Not mentioned | ✅ Supported |
| AMQP | 🔲 Not mentioned | ✅ Supported |
| TLS/HTTPS | 🔲 Not in v1 | ✅ Via sidecar termination |
| WebSocket | 🔲 Not mentioned | ✅ Supported |

### 3.2 Protocol Detection

**Déjà Approach:**
- First outbound bytes analyzed for protocol hints
- Detected: HTTP/1.1, Redis RESP2/RESP3, PostgreSQL wire protocol
- Detection happens at capture time in `protocol_detect.rs`
- Protocol hint stored in `SocketBoundaryEvent`

**Speedscale Approach:**
- More extensive protocol support via sidecar proxy
- Proxy terminates connections, enabling TLS decryption
- Protocol-specific parsers for 15+ protocols

---

## 4. Replay Mechanisms

### 4.1 Replay Architecture

| Aspect | Déjà | Speedscale |
|--------|------|------------|
| **Replay Type** | In-process substitution | External generator + responder |
| **Generator** | N/A (same process) | `speedscale-generator` pod |
| **Mock Server** | N/A (in-process replay) | `speedscale-responder` pod |
| **Isolation** | None (runs in target process) | Clean separation via K8s pods |
| **Concurrency** | Single-threaded fixtures | Configurable virtual users (vUsers) |

### 4.2 Déjà's Socketpair Replay (Innovative)

Déjà's approach for async compatibility is architecturally interesting:

```
During Replay:
┌──────────────────┐
│ Application      │
│ ┌──────────────┐ │
│ │ tokio runtime│ │
│ │ ┌──────────┐ │ │     ┌──────────────────┐
│ │ │epoll_wait│ │ │     │ Agent Thread     │
│ │ └────┬─────┘ │ │     │ ┌──────────────┐ │
│ └──────┼───────┘ │     │ │Recorded Data │ │
│        │         │     │ └──────┬───────┘ │
│   ┌────┴────┐    │     └────────┼─────────┘
│   │  fd=5   │<---socketpair----┤
│   │(app end)│    │              │ write()
│   └────┬────┘    │              │
└────────┼─────────┘              │
         │                         │
    ┌────┴────┐                    │
    │ fd=6    │<-------------------┘
    │(agent  │
    │ end)    │
    └─────────┘
```

This is clever because:
1. Creates real kernel sockets (socketpair AF_UNIX)
2. epoll sees real fd readiness
3. No need to hook epoll itself
4. Works with any async runtime

### 4.3 Speedscale's Dual-Component Replay

```
┌─────────────────────────────────────────────────────────┐
│                    REPLAY MODE                           │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────┐        ┌──────────────────┐       │
│  │  Generator      │───────>│   SUT (Your App) │       │
│  │  (inbound reqs) │        │                  │       │
│  └─────────────────┘        └────────┬─────────┘       │
│                                      │                  │
│                                      │ outbound calls   │
│                                      ▼                  │
│                           ┌──────────────────┐         │
│                           │   Responder      │         │
│                           │  (mock backend)  │         │
│                           └──────────────────┘         │
│                                                          │
│  Generator reads from: action.jsonl (inbound traffic)   │
│  Responder reads from: reaction.jsonl (outbound mocks)  │
└─────────────────────────────────────────────────────────┘
```

---

## 5. Correlation & Context Tracking

### 5.1 Request Correlation

| Aspect | Déjà | Speedscale |
|--------|------|------------|
| **Mechanism** | Task-local (`DEJA_CORRELATION_ID`) + FFI bridge | **None at capture time** |
| **Propagation** | Manual via `deja_tokio::spawn/spawn_blocking` | **Not applicable — no causal tracking** |
| **Validation** | In-band markers (Postgres comments, Redis ECHO) | **Signature matching only** |
| **Coverage** | Request-owned I/O works; multiplexed drivers (fred) problematic | **No per-request I/O attribution** |

**Clarification on Speedscale "correlation":**
- Speedscale does NOT have causal correlation at capture time
- Kafka `correlationId` field is the protocol-level request ID (not Speedscale-added)
- OTel/Datadog/NewRelic appear in their "filter by default" list — they REMOVE observability traffic, don't use it
- Their "vUser" scoping is a REPLAY-TIME concept, not capture-time correlation
- They explicitly do NOT track which inbound request caused which outbound I/O call |

### 5.2 Déjà's Correlation Architecture (Sophisticated)

```rust
// Tokio task-local that survives work-stealing
tokio::task_local! {
    pub static DEJA_CORRELATION_ID: String;
}

// DejaScope middleware wraps futures
DEJA_CORRELATION_ID.scope(correlation_id, handler_future).await

// LD_PRELOAD hooks read via FFI
#[no_mangle]
pub unsafe extern "C" fn deja_correlation_id(buf: *mut u8, len: usize) -> usize
```

**Key Innovation:** Task-local with RAII guards handles Tokio's work-stealing correctly.

**Known Limitation:** Multiplexed drivers (like fred for Redis) break correlation because I/O happens in a long-lived routing task, not the request task.

---

## 6. Traffic Transformation

### 6.1 Transform Capabilities

| Feature | Déjà | Speedscale |
|---------|------|------------|
| **Transform System** | 🔲 Not implemented | ✅ Sophisticated pipeline |
| **Extractors** | N/A | http_req_body, http_req_header, json_path, etc. |
| **Transforms** | N/A | jwt_resign, date_shift, smart_replace, regex_replace, etc. |
| **Variable Cache** | N/A | Share data between requests |
| **Use Case** | N/A | Update JWTs, shift timestamps, rotate test data |

### 6.2 Speedscale Transform System

```json
{
  "id": "sample_transforms",
  "generator": [
    {
      "extractor": {"type": "http_req_body"},
      "transforms": [
        {"type": "json_path", "config": {"path": "UserName"}},
        {"type": "one_of", "config": {"options": "ken,liz,mike", "strategy": "sequential"}}
      ]
    }
  ]
}
```

**Critical Gap for Déjà:** Without transforms, replay is limited to exact matches. Real-world replay requires:
- JWT refresh
- Timestamp shifting
- Dynamic data replacement
- Session token rotation

---

## 7. Data Privacy & Security

### 7.1 PII Handling

| Feature | Déjà | Speedscale |
|---------|------|------------|
| **DLP System** | 🔲 Not implemented | ✅ Comprehensive DLP |
| **PII Discovery** | N/A | ✅ Automatic scanning |
| **Redaction** | N/A | ✅ In-cluster before egress |
| **Tokenization** | N/A | ✅ REDACTED tokens with mapping |
| **Compliance** | N/A | GDPR, HIPAA considerations |

### 7.2 Speedscale DLP Workflow

```
┌────────────────────────────────────────────────────────────┐
│                    DLP PIPELINE                             │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Discover PII in QA environment                         │
│              │                                              │
│              ▼                                              │
│  2. Create DLP Rule (regex patterns, field paths)          │
│              │                                              │
│              ▼                                              │
│  3. Apply to Forwarder (in-cluster filtering)              │
│              │                                              │
│              ▼                                              │
│  4. Redacted data → Speedscale Cloud                       │
│              │                                              │
│              ▼                                              │
│  5. Generate safe test data from snapshots                 │
│                                                             │
│  Key Principle: PII never leaves cluster in raw form        │
└────────────────────────────────────────────────────────────┘
```

---

## 8. Deployment & Operations

### 8.1 Deployment Models

| Aspect | Déjà | Speedscale |
|--------|------|------------|
| **Self-Hosted** | ✅ Fully open source | ⚠️ BYOC option available |
| **Cloud** | ❌ None | ✅ Hosted platform |
| **K8s Native** | ⚠️ Works but not native | ✅ Purpose-built operator |
| **Local Dev** | ✅ CLI + LD_PRELOAD | ✅ proxymock CLI |
| **CI/CD** | Manual integration | Native GitHub/GitLab/Jenkins |

### 8.2 Operational Complexity

**Déjà:**
- Simple: single binary + shared library
- No infrastructure to manage
- Manual artifact management
- No central dashboard

**Speedscale:**
- Complex: K8s operator, webhooks, cloud dependency
- Managed infrastructure via operator
- Centralized snapshot/report management
- Rich dashboard and analytics

---

## 9. Benchmarking & Metrics

### 9.1 Déjà's HS-41 Scorecard (Impressive)

| Metric | Target | Measured | Result |
|--------|--------|----------|--------|
| P50 Latency | < 5% | 0% | ✅ PASS |
| P99 Latency | < 10% | -22.2%* | ✅ PASS* |
| Throughput | < 5% drop | -1.6% | ✅ PASS |
| Memory | < 50MB | +0.1MB | ✅ PASS |
| Data Completeness | 100% | 100% | ✅ PASS |
| A9 Fidelity | 100% | 100% | ✅ PASS |

*Note: -22.2% indicates instrumented was faster (noise, not real improvement)

**Ground-Truth Verification:**
- tcpdump captures alongside Déjà
- Byte-by-byte comparison: 48,938 bytes matched exactly
- Independent kernel-level verification

### 9.2 Missing Metrics

Déjà doesn't currently measure:
- Per-protocol breakdown (Redis vs PG overhead)
- Cross-machine replay portability
- Long-duration soak test stability
- Memory pressure behavior under heavy load

---

## 10. Key Insights & Recommendations for Déjà

### 10.1 What Déjà Does Well (Differentiation)

1. **Zero Infrastructure**: No K8s, no operator, no cloud dependency
2. **Zero Permissions**: No root, no CAP_SYS_PTRACE, no sidecar
3. **Ground-Truth Verification**: 100% pcap-verified fidelity
4. **Deterministic Boundaries**: Captures time, randomness, env vars
5. **Socketpair Innovation**: Elegant solution for async replay
6. **Fidelity Model**: Explicit capture/replay/divergence classification

### 10.2 Critical Gaps to Address

#### HIGH PRIORITY

1. **Transform System**
   - **Why**: Essential for practical replay (JWTs expire, timestamps age)
   - **Speedscale Pattern**: Extractor → Transform Chain → Re-insert
   - **Déjà Approach**: Add `deja-transform` crate with extractor/transform traits

2. **Markdown Export Format**
   - **Why**: Human-readable, LLM-friendly, editable
   - **Speedscale Pattern**: REQUEST/RESPONSE/SIGNATURE/METADATA sections
   - **Déjà Approach**: Add `--format markdown` to `deja inspect`

3. **Service Mocking (Responder Equivalent)**
   - **Why**: Test apps without real dependencies
   - **Speedscale Pattern**: Separate responder that mocks outbound calls
   - **Déjà Approach**: `deja mock-server --artifact <PATH>` command

4. **Kubernetes Integration**
   - **Why**: Modern deployments are K8s-native
   - **Speedscale Pattern**: Operator + mutating webhook
   - **Déjà Approach**: Optional admission webhook that injects LD_PRELOAD

#### MEDIUM PRIORITY

5. **DLP/Data Masking**
   - **Why**: Production traffic contains PII
   - **Speedscale Pattern**: In-cluster filtering before egress
   - **Déjà Approach**: Regex/JSON path based redaction rules

6. **Variable Cache**
   - **Why**: Share data between requests (session IDs, etc.)
   - **Speedscale Pattern**: Transform-scoped variable storage
   - **Déjà Approach**: Add variable store to replay runtime

7. **HTTP/2 & gRPC Support**
   - **Why**: Modern services use these protocols
   - **Speedscale Pattern**: Frame-level decoding + HPACK
   - **Déjà Approach**: Already in roadmap (Layer 1.4-1.7)

8. **Assertion System**
   - **Why**: Automated regression detection
   - **Speedscale Pattern**: Per-RRPair assertions in test config
   - **Déjà Approach**: Extend `deja regress` with assertion rules

#### LOW PRIORITY

9. **Cloud Dashboard** (consider partnership/hosting option)
10. **Load Generation** (beyond current scope)
11. **Multi-user Collaboration** (Git-based sharing first)

### 10.3 Architectural Recommendations

#### Adopt Speedscale's RRPair Separation

Currently Déjà mixes inbound/outbound in `events.jsonl`. Consider:

```
artifact/
├── metadata.json
├── inbound.jsonl     # HTTP requests to the app (generator input)
├── outbound.jsonl    # App's dependency calls (responder input)
├── boundaries.jsonl  # Time, random, env (deterministic replay)
└── manifest.json
```

#### Implement Transform Chains

```rust
pub trait Extractor {
    fn extract(&self, event: &BoundaryEvent) -> Option<String>;
}

pub trait Transform {
    fn transform(&self, input: &str, ctx: &mut TransformCtx) -> String;
}

pub struct TransformChain {
    extractor: Box<dyn Extractor>,
    transforms: Vec<Box<dyn Transform>>,
}
```

#### Add Snapshot Server Mode

For CI/CD integration, Déjà could run as a mock server:

```bash
# Terminal 1: Start mock server from recording
dej mock-server --artifact ./recording --port 8080

# Terminal 2: Run tests against mock
cargo test -- --endpoint http://localhost:8080
```

### 10.4 Strategic Positioning

**Déjà should position as:**
- **Lightweight alternative** to full K8s-native solutions
- **Developer-first tool** for local testing and debugging
- **Deterministic replay specialist** (time + randomness control)
- **CI/CD friendly** with exit codes and JSON output

**Avoid competing directly on:**
- Large-scale load testing (use Speedscale/k6)
- Production traffic analysis in K8s (use Speedscale)
- Team collaboration features (start with Git-based sharing)

---

## 11. Code-Level Insights

### 11.1 Déjà Strengths Observed

1. **Clean separation**: `deja-core` types, `deja-preload` hooks, `deja-cli` UX
2. **Comprehensive testing**: E2E tests with real fixtures
3. **Error handling**: Explicit error types, proper exit codes
4. **Documentation**: Excellent ARCHITECTURE.md and ROADMAP.md
5. **Fidelity model**: Explicit capture/replay classifications

### 11.2 Areas for Improvement

1. **Protocol parsing**: Replace hand-rolled with established crates
   - `redis-protocol` for RESP (roadmap item 1.1 ✅)
   - `httparse` for HTTP (roadmap item 1.2 ✅)
   - `hpack` for HTTP/2 (roadmap item 1.5)

2. **Transform system**: Add before v1.0 (critical gap)

3. **Artifact format**: Consider markdown export for human editing

4. **Correlation**: Document the fred/multiplexing limitation clearly

---

## 12. Additional Differentiators

### 12.1 proxymock: Speedscale's Local Development Tool

Speedscale offers **proxymock** as a standalone CLI tool for local development, similar to what Déjà provides:

```bash
# Record traffic locally
proxymock record --app-port 8000

# Mock without real dependencies
proxymock mock --in ./proxymock
```

**proxymock Features:**
- Records full-fidelity payloads without code changes
- Human and AI-readable markdown format
- Terminal UI (TUI) for navigation
- MCP (Model Context Protocol) integration for AI-assisted debugging
- Local by default (recordings stay on machine)
- Works with APIs, databases, and gRPC

**Comparison to Déjà:**
| Aspect | Déjà | proxymock |
|--------|------|-----------|
| Interception | LD_PRELOAD hooks | Proxy-based |
| Setup | Set env var, launch | Configure proxy, run through proxy |
| Protocols | HTTP/1.1, Redis, PG | HTTP/1, HTTP/2, gRPC, many more |
| Local Storage | JSONL files | Markdown + JSONL |
| TUI | No | Yes |
| MCP/AI Integration | No | Yes |

**Insight:** Déjà's LD_PRELOAD approach is more transparent (no proxy config needed), but proxymock's TUI and AI integration are compelling for developer experience.

### 12.2 LLM Simulation (Emerging Use Case)

Speedscale has identified and addressed a major emerging use case: **LLM API cost reduction in non-production environments**.

**The Problem:**
- Mid-size support center using Claude Sonnet: ~$180K/year
- Much of this is non-production: developer iteration, CI pipelines, load tests
- Real LLM calls add 500ms-3s latency per request
- Non-deterministic responses cause flaky tests

**Speedscale Solution:**
1. Capture one real interaction with each LLM provider (OpenAI, Anthropic, Gemini, xAI, OpenRouter, Perplexity)
2. Replay recorded responses in development, CI, and load testing
3. Zero cost, instant responses, deterministic behavior

**Supported LLM Providers (auto-detected):**
| Provider | Detection Host |
|----------|----------------|
| OpenAI | `api.openai.com` |
| Anthropic Claude | `api.anthropic.com` |
| Google Gemini | `generativelanguage.googleapis.com` |
| Grok (xAI) | `api.x.ai` |
| OpenRouter | `openrouter.ai` |
| Perplexity | `api.perplexity.ai` |

**Opportunity for Déjà:**
LLM simulation aligns perfectly with Déjà's deterministic replay philosophy. Since LLM calls are just HTTP requests, Déjà could:
- Capture outbound calls to `api.openai.com`
- During replay, substitute recorded responses
- Enable teams to test AI integrations without API keys in CI

This would be a valuable addition to the roadmap, possibly as part of Layer 2.0 (HTTP/HTTPS proxy tunnel support).

### 12.3 Technology Support Matrix (Speedscale)

Speedscale's extensive technology support highlights Déjà's gaps:

**Languages:** .NET, C++, Go, Java, Node.js, Python, Ruby

**Protocols:** HTTP/1.1, HTTP/2, gRPC, AMQP, GraphQL, Kafka, Redis (capture), Protobuf, SOAP

**Databases:** PostgreSQL (full), MySQL (full), MongoDB (capture), Redis (capture), DynamoDB, Elasticsearch

**Auth:** AWS SigV4, Basic Auth, Bearer JWT (with automatic rotation)

**Message Brokers:** RabbitMQ, Kafka, Google PubSub, AWS SQS/SNS/Kinesis, Azure Event Hubs

**Cloud APIs:** AWS S3/minio, Salesforce, Stripe, Twilio, Zapier

**Environments:** Kubernetes (all distros), Docker Desktop, ECS/Fargate, EC2, VMs, local desktop

**Déjà Current:** HTTP/1.1, Redis (RESP2/RESP3), PostgreSQL, Linux x86_64 only

### 12.4 Test Configurations & CI/CD Integration

Speedscale's **Test Configs** provide template-driven replay customization:

```json
{
  "generator": {
    "copies": 10,
    "delay": "100ms",
    "baseURL": "http://staging-api"
  },
  "assertions": {
    "checkStatusCode": true,
    "checkResponseBody": false
  },
  "goals": {
    "maxFailureRate": 0.01
  }
}
```

**Integration Patterns:**

**CI/CD Gate:**
```bash
speedctl replay $SNAPSHOT_ID \
  --test-config-id=ci-gate \
  --custom-url='http://localhost:8080'
# Exit code reflects pass/fail
```

**GitOps Workflow:**
```bash
# Store config in git
git add my_config.json

# Upload to Speedscale
speedctl put testconfig my_config.json
```

**Déjà Gap:** No formal test configuration system or CI gate capability yet.

---

## 13. Summary Table

| Category | Déjà | Speedscale | Winner |
|----------|------|------------|--------|
| Simplicity | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Déjà |
| K8s Integration | ⭐⭐ | ⭐⭐⭐⭐⭐ | Speedscale |
| Protocol Support | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Speedscale |
| Transform System | ⭐ | ⭐⭐⭐⭐⭐ | Speedscale |
| Deterministic Replay | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Déjà |
| Zero Infrastructure | ⭐⭐⭐⭐⭐ | ⭐⭐ | Déjà |
| Collaboration | ⭐⭐ | ⭐⭐⭐⭐⭐ | Speedscale |
| Open Source | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Déjà |
| Documentation | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Tie |
| Ground-Truth Verification | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Déjà |
| LLM Simulation | ⭐ | ⭐⭐⭐⭐⭐ | Speedscale |
| Local Dev Experience | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Speedscale (TUI+AI) |

---

## Appendix: Key Terminology Mapping

| Déjà Term | Speedscale Term | Meaning |
|-----------|-----------------|---------|
| Artifact | Snapshot | Captured traffic collection |
| BoundaryEvent | RRPair | Single request/response unit |
| Replay | Replay | Re-executing captured traffic |
| (none) | Generator | Load generator for inbound traffic |
| (none) | Responder | Mock server for outbound dependencies |
| Correlation ID | **(none — uses signature matching)** | **Déjà has causal tracking; Speedscale does not** |
| Preload | (sidecar/eBPF) | Traffic interception mechanism |
| Divergence | Assertion failure | Behavior mismatch detection |
| Fidelity | (implicit) | Capture quality classification |

---

## References

1. Déjà Documentation:
   - ARCHITECTURE.md
   - REPLAY_PIPELINE.md
   - CORRELATION_ARCHITECTURE.md
   - BENCHMARK_FRAMEWORK.md
   - ROADMAP.md

2. Speedscale Documentation:
   - https://github.com/speedscale/docs
   - https://docs.speedscale.com/
   - https://speedscale.com/llms.txt

3. Key Repositories:
   - Déjà: `<repo-root>`
   - Speedscale: https://github.com/speedscale
