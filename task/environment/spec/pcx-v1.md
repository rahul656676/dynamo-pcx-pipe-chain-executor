# PCX v1 — Pipe Chain eXecution log format

Normative specification for `.pcx` session archives produced by the PCX collector, revision 1.

All fixed-width integers are unsigned little-endian unless stated otherwise.

## Primitives

`uvarint` — LEB128. Seven payload bits per byte, least-significant group first; bit 7 set
means another byte follows.

`svarint` — a `uvarint` holding a zigzag-mapped signed value. Encoding maps `n` to
`(n << 1) XOR (n >> 63)` (arithmetic shift); decoding maps `u` back to
`(u >> 1) XOR -(u AND 1)`.

`crc32` — CRC-32/ISO-HDLC (polynomial `0xEDB88320`, reflected input and output, initial
value `0xFFFFFFFF`, final XOR `0xFFFFFFFF`); the value produced by `zlib.crc32`.

## File header

Exactly 20 bytes at offset 0.

| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0 | 4 | `magic` | ASCII `PCX1` |
| 4 | 1 | `version` | `1` |
| 5 | 1 | `reserved` | `0` |
| 6 | 2 | `n_threads` | number of initial threads in this session |
| 8 | 4 | `dict_offset` | byte offset of the variable name dictionary |
| 12 | 4 | `events_offset` | byte offset of the first event |
| 16 | 4 | `header_crc32` | `crc32` over bytes 0..15 |

## Variable dictionary

At `dict_offset`: a `uvarint` entry count, then that many entries. Each entry is a
`uvarint` byte length followed by that many UTF-8 bytes. An entry's zero-based position is
its dictionary index. Variable names are unique within a session.

## Events

Events begin at `events_offset`. Each event starts with a 3-field prefix:

| Field | Encoding | Notes |
|-------|----------|-------|
| `type` | `uint8` | event type code (see below) |
| `thread_id` | `uint16 LE` | the thread that emitted this event |
| `timestamp_delta_ms` | `uvarint` | milliseconds since the previous event (or since epoch for the first) |

followed immediately by a type-specific payload.

### Event types

#### 0x00 WRITE — assign or accumulate a variable

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `var_index` | `uvarint` | index into the variable dictionary |
| `value` | `svarint` | the value to write |
| `flags` | `uint8` | bit field (see below) |

Flag bits:
- **bit 0** (`shared`): if set, this variable is accessible to the thread's parent and
  any sibling threads spawned from the same parent.
- **bit 1** (`accumulate`): if set, the variable is **not** replaced with `value`.
  Instead the new effective value is `current_value XOR value`. If the variable has no
  prior value, `current_value` is treated as `0`.

#### 0x01 FORK — spawn a child thread

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `child_thread_id` | `uint16 LE` | ID of the newly spawned thread |
| `n_inherited` | `uvarint` | number of variables copied to the child at spawn time |
| `var_index` × n_inherited | `uvarint` each | variables the child inherits |

The child receives a snapshot of the listed variables from the parent at the moment of the
FORK event. The child starts with no other state. The parent continues executing.

Both threads record their variable state at the FORK point internally; this snapshot is
used later during JOIN conflict resolution (see JOIN semantics below).

#### 0x02 JOIN — merge a child thread back into its parent

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `child_thread_id` | `uint16 LE` | the thread being merged |
| `anchor_var_index` | `uvarint` | dictionary index of the anchor variable |

JOIN semantics (applied in this exact order):

1. **Conflict resolution**: For each variable that **both** parent and child modified after
   the corresponding FORK event:
   - The value from the thread with the **lower `thread_id`** is kept.
   - This is **not** last-write-wins; the lower numeric thread ID always wins conflicts,
     regardless of which write happened more recently.

2. **Non-conflicting writes**: Variables modified only by the child (not the parent) are
   merged into the parent. Variables modified only by the parent are unaffected.

3. **Anchor reset**: Let `A` be the name of the variable at `anchor_var_index`. After
   conflict resolution, for every variable whose name is **lexicographically ≥ A**:
   - If the variable existed in the parent's last SYNC snapshot: reset it to that
     snapshot value.
   - If the variable did not exist in the last SYNC snapshot: remove it from the parent.

4. The child thread is terminated. It no longer appears in the final state output.

#### 0x03 PIPE_WRITE — write a batch of values into a pipe

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `pipe_id` | `uint16 LE` | identifies the pipe (shared across all threads in session) |
| `n_values` | `uint8` | number of values in this batch (1..255) |
| `value` × n_values | `svarint` each | the values |

Each pipe maintains a **write cursor** initialised to `0`. After every PIPE_WRITE of
`n_values` values, the cursor is updated:

```
cursor = cursor XOR n_values
```

The cursor is **not** a count of values written; it is the running XOR of every batch size
written so far.

#### 0x04 PIPE_READ — read one value from a pipe into a variable

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `pipe_id` | `uint16 LE` | the pipe to read from |
| `dest_var_index` | `uvarint` | variable to store the result |

If the pipe is empty, the variable is left unchanged and no value is consumed.

The value read depends on the pipe's current **mode** (see REVERSE below):

- **Normal mode (FIFO)**: read from position `cursor mod len(buffer)` in the pipe
  buffer, then remove that element. The cursor is **not** updated by a read.
- **Reversed mode (LIFO)**: read and remove the **last** element of the buffer (standard
  stack pop). The cursor is **not** updated by a read.

The result is assigned to `dest_var_index` in the reading thread's local state (not
accumulated; always a plain assignment regardless of any prior `accumulate` flag on the
variable).

#### 0x05 REVERSE — flip a pipe's read mode

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `pipe_id` | `uint16 LE` | the pipe whose mode is toggled |

Toggles the pipe between normal (FIFO) mode and reversed (LIFO) mode. A pipe starts in
normal mode. Successive REVERSE events on the same pipe alternate between the two modes.

#### 0x06 SYNC — synchronisation and checkpoint

Payload:
| Field | Encoding | Notes |
|-------|----------|-------|
| `checkpoint_id` | `uvarint` | monotonically increasing checkpoint identifier |
| `anchor_var_index` | `uvarint` | variable whose name serves as the anchor for JOIN resets |

A SYNC event does two things:

1. **Snapshot**: the emitting thread saves a copy of its current variable state. This
   snapshot replaces any prior snapshot and is used as the baseline for JOIN anchor resets
   (see JOIN semantics above). The anchor variable recorded here is also the default anchor
   for any subsequent JOIN that this thread's parent initiates.

2. **Pipe reset**: every pipe in the session is returned to **normal (FIFO) mode**,
   regardless of which thread issued the SYNC or how many REVERSE events had been applied.

## Reconciliation and final state

Process events in the order they appear in the file. Threads and pipes are identified by
their IDs; IDs are session-local (not global across files). Each `.pcx` file is an
independent session.

The final output covers only **alive threads** (threads that have not been terminated by
a JOIN). For each alive thread, output every variable in its current state.

Variables assigned by PIPE_READ are part of the thread's state like any other variable.
