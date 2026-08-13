import os
import json
import zlib
import struct
import random
from pathlib import Path

def encode_uvarint(value):
    out = bytearray()
    while True:
        b = value & 0x7f
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def encode_svarint(value):
    mapped = (value << 1) ^ (value >> 63)
    return encode_uvarint(mapped)

class Simulator:
    def __init__(self, n_threads, n_pipes, n_vars, var_names):
        self.n_threads = n_threads
        self.n_pipes = n_pipes
        self.var_names = var_names
        
        self.threads = {i: {} for i in range(n_threads)}
        self.alive = {i: True for i in range(n_threads)}
        self.sync_snapshots = {i: {} for i in range(n_threads)}
        
        self.fork_events = {}
        self.modified_since_fork = {}
        
        self.pipes_buffer = {i: [] for i in range(n_pipes)}
        self.pipes_cursor = {i: 0 for i in range(n_pipes)}
        self.pipes_reversed = {i: False for i in range(n_pipes)}
        self.pipes_total_written = {i: 0 for i in range(n_pipes)}
        self.pipes_total_read = {i: 0 for i in range(n_pipes)}

    def get_var_name(self, idx):
        return self.var_names[idx]

    def mark_modified(self, thread_id, var_name):
        if thread_id in self.modified_since_fork:
            self.modified_since_fork[thread_id][1].add(var_name)
        for c_id, (p_id, _) in self.fork_events.items():
            if p_id == thread_id and self.alive[c_id]:
                self.modified_since_fork[c_id][0].add(var_name)

    def process_event(self, event_type, thread_id, payload):
        if event_type == 0x00: # WRITE
            var_idx, value, flags = payload
            var_name = self.get_var_name(var_idx)
            accumulate = bool(flags & 2)
            
            if accumulate:
                curr = self.threads[thread_id].get(var_name, 0)
                self.threads[thread_id][var_name] = curr ^ value
            else:
                self.threads[thread_id][var_name] = value
                
            self.mark_modified(thread_id, var_name)

        elif event_type == 0x01: # FORK
            child_id, inherited_idxs = payload
            if child_id >= self.n_threads:
                self.n_threads = child_id + 1
            self.threads[child_id] = {}
            for idx in inherited_idxs:
                vname = self.get_var_name(idx)
                if vname in self.threads[thread_id]:
                    self.threads[child_id][vname] = self.threads[thread_id][vname]
            self.alive[child_id] = True
            if child_id not in self.sync_snapshots:
                self.sync_snapshots[child_id] = {}
                
            self.fork_events[child_id] = (thread_id, [self.get_var_name(i) for i in inherited_idxs])
            self.modified_since_fork[child_id] = (set(), set())

        elif event_type == 0x02: # JOIN
            child_id, anchor_idx = payload
            anchor_var = self.get_var_name(anchor_idx)
            parent_id = self.fork_events[child_id][0]
            
            p_mod, c_mod = self.modified_since_fork[child_id]
            conflicts = p_mod.intersection(c_mod)
            
            for v in conflicts:
                if child_id < parent_id:
                    if v in self.threads[child_id]:
                        self.threads[parent_id][v] = self.threads[child_id][v]
                    else:
                        self.threads[parent_id].pop(v, None)
            
            c_only = c_mod - p_mod
            for v in c_only:
                if v in self.threads[child_id]:
                    self.threads[parent_id][v] = self.threads[child_id][v]
                else:
                    self.threads[parent_id].pop(v, None)
                    
            vars_to_reset = [v for v in self.threads[parent_id].keys() if v >= anchor_var]
            vars_in_sync = [v for v in self.sync_snapshots[parent_id].keys() if v >= anchor_var]
            
            for v in vars_to_reset:
                if v not in self.sync_snapshots[parent_id]:
                    self.threads[parent_id].pop(v, None)
                else:
                    self.threads[parent_id][v] = self.sync_snapshots[parent_id][v]
            for v in vars_in_sync:
                if v not in self.threads[parent_id]:
                    self.threads[parent_id][v] = self.sync_snapshots[parent_id][v]

            self.alive[child_id] = False
            
            if parent_id in self.modified_since_fork:
                self.modified_since_fork[parent_id][1].update(c_mod)
            for c_id, (p_id, _) in self.fork_events.items():
                if p_id == parent_id and self.alive[c_id] and c_id != child_id:
                    self.modified_since_fork[c_id][0].update(c_mod)

        elif event_type == 0x03: # PIPE_WRITE
            pipe_id, values = payload
            self.pipes_buffer[pipe_id].extend(values)
            self.pipes_cursor[pipe_id] ^= len(values)
            self.pipes_total_written[pipe_id] += len(values)

        elif event_type == 0x04: # PIPE_READ
            pipe_id, dest_var_idx = payload
            dest_var = self.get_var_name(dest_var_idx)
            
            if len(self.pipes_buffer[pipe_id]) > 0:
                if self.pipes_reversed[pipe_id]:
                    val = self.pipes_buffer[pipe_id].pop()
                else:
                    pos = self.pipes_cursor[pipe_id] % len(self.pipes_buffer[pipe_id])
                    val = self.pipes_buffer[pipe_id].pop(pos)
                    
                self.threads[thread_id][dest_var] = val
                self.pipes_total_read[pipe_id] += 1
                self.mark_modified(thread_id, dest_var)

        elif event_type == 0x05: # REVERSE
            pipe_id = payload[0]
            self.pipes_reversed[pipe_id] = not self.pipes_reversed[pipe_id]

        elif event_type == 0x06: # SYNC
            checkpoint_id, anchor_var_idx = payload
            self.sync_snapshots[thread_id] = self.threads[thread_id].copy()
            for i in range(self.n_pipes):
                self.pipes_reversed[i] = False

def generate_session(rng, session_idx):
    n_threads = rng.randint(3, 5)
    n_pipes = rng.randint(2, 3)
    n_vars = rng.randint(15, 25)
    var_names = [f"var_{i:03d}" for i in range(n_vars)]
    
    events = []
    
    sim = Simulator(n_threads, n_pipes, n_vars, var_names)
    active_threads = list(range(n_threads))
    
    # ensure at least 1 fork+join and 1 reverse+sync
    n_events = rng.randint(300, 500)
    checkpoint_counter = 0
    
    for i in range(n_threads):
        sim.process_event(0x06, i, (checkpoint_counter, 0)) # Initial SYNC
        checkpoint_counter += 1
    
    next_child_id = n_threads
    
    forked_children = {}
    
    for _ in range(n_events):
        thread_id = rng.choice(active_threads)
        
        choices = [0x00, 0x03, 0x04, 0x05, 0x06]
        if active_threads:
            choices.append(0x01) # FORK
        if thread_id in forked_children and forked_children[thread_id]:
            choices.append(0x02) # JOIN
            
        evt_type = rng.choice(choices)
        
        payload = None
        
        if evt_type == 0x00:
            var_idx = rng.randint(0, n_vars - 1)
            val = rng.randint(-1000, 1000)
            flags = rng.choice([0, 1, 2, 3])
            payload = (var_idx, val, flags)
        elif evt_type == 0x01:
            child_id = next_child_id
            next_child_id += 1
            n_inh = rng.randint(0, min(5, n_vars))
            inh = random.sample(range(n_vars), n_inh)
            payload = (child_id, inh)
            active_threads.append(child_id)
            if thread_id not in forked_children:
                forked_children[thread_id] = []
            forked_children[thread_id].append(child_id)
        elif evt_type == 0x02:
            child_id = rng.choice(forked_children[thread_id])
            forked_children[thread_id].remove(child_id)
            active_threads.remove(child_id)
            anchor = rng.randint(0, n_vars - 1)
            payload = (child_id, anchor)
        elif evt_type == 0x03:
            pipe_id = rng.randint(0, n_pipes - 1)
            n_vals = rng.choice([1, 3, 5, 7])
            vals = [rng.randint(-100, 100) for _ in range(n_vals)]
            payload = (pipe_id, vals)
        elif evt_type == 0x04:
            pipe_id = rng.randint(0, n_pipes - 1)
            var_idx = rng.randint(0, n_vars - 1)
            payload = (pipe_id, var_idx)
        elif evt_type == 0x05:
            pipe_id = rng.randint(0, n_pipes - 1)
            payload = (pipe_id,)
        elif evt_type == 0x06:
            anchor = rng.randint(0, n_vars - 1)
            payload = (checkpoint_counter, anchor)
            checkpoint_counter += 1
            
        sim.process_event(evt_type, thread_id, payload)
        events.append((evt_type, thread_id, payload))

    checksum = 0
    MASK64 = 0xFFFFFFFFFFFFFFFF
    for t_id in sim.threads:
        if sim.alive[t_id]:
            for v, val in sim.threads[t_id].items():
                checksum = (checksum ^ (val & MASK64)) & MASK64

    final_state = []
    for t_id in sim.threads:
        if sim.alive[t_id]:
            for v, val in sim.threads[t_id].items():
                final_state.append({"thread_id": t_id, "var": v, "value": val})
    
    pipes_info = []
    for p_id in range(n_pipes):
        pipes_info.append({
            "pipe_id": p_id,
            "remaining": sim.pipes_buffer[p_id],
            "total_written": sim.pipes_total_written[p_id],
            "total_read": sim.pipes_total_read[p_id]
        })
        
    session_name = f"session-{session_idx:03d}.pcx"
    
    meta = {
        "name": session_name,
        "total_events": len(events),
        "n_threads": n_threads,
        "n_vars": n_vars,
        "n_pipes": n_pipes,
        "alive_threads": sorted([t for t in sim.alive if sim.alive[t]]),
        "state": sorted(final_state, key=lambda x: (x["thread_id"], x["var"])),
        "pipes": pipes_info,
        "checksum": checksum
    }
    
    return session_name, meta, events, var_names, n_threads

def encode_session(events, var_names, n_threads):
    dict_payload = bytearray()
    dict_payload.extend(encode_uvarint(len(var_names)))
    for name in var_names:
        encoded_name = name.encode('utf-8')
        dict_payload.extend(encode_uvarint(len(encoded_name)))
        dict_payload.extend(encoded_name)
        
    events_payload = bytearray()
    for evt_type, t_id, payload in events:
        events_payload.append(evt_type)
        events_payload.extend(struct.pack("<H", t_id))
        events_payload.extend(encode_uvarint(random.randint(1, 100))) # ts_delta
        
        if evt_type == 0x00:
            events_payload.extend(encode_uvarint(payload[0]))
            events_payload.extend(encode_svarint(payload[1]))
            events_payload.append(payload[2])
        elif evt_type == 0x01:
            events_payload.extend(struct.pack("<H", payload[0]))
            events_payload.extend(encode_uvarint(len(payload[1])))
            for v in payload[1]:
                events_payload.extend(encode_uvarint(v))
        elif evt_type == 0x02:
            events_payload.extend(struct.pack("<H", payload[0]))
            events_payload.extend(encode_uvarint(payload[1]))
        elif evt_type == 0x03:
            events_payload.extend(struct.pack("<H", payload[0]))
            events_payload.append(len(payload[1]))
            for v in payload[1]:
                events_payload.extend(encode_svarint(v))
        elif evt_type == 0x04:
            events_payload.extend(struct.pack("<H", payload[0]))
            events_payload.extend(encode_uvarint(payload[1]))
        elif evt_type == 0x05:
            events_payload.extend(struct.pack("<H", payload[0]))
        elif evt_type == 0x06:
            events_payload.extend(encode_uvarint(payload[0]))
            events_payload.extend(encode_uvarint(payload[1]))
            
    header_start = b"PCX1\x01\x00" + struct.pack("<H", n_threads)
    
    dict_offset = 20
    events_offset = 20 + len(dict_payload)
    
    header = header_start + struct.pack("<II", dict_offset, events_offset)
    header_crc32 = zlib.crc32(header)
    header += struct.pack("<I", header_crc32)
    
    return header + dict_payload + events_payload

def main():
    rng = random.Random(20260813)
    here = Path(__file__).resolve().parent
    task_dir = here.parent
    data_dir = task_dir / "environment" / "data"
    tests_dir = here
    data_dir.mkdir(parents=True, exist_ok=True)
    
    ground_truth = {"sessions": []}
    
    for i in range(1, 5):
        s_name, s_meta, s_events, var_names, n_threads = generate_session(rng, i)
        ground_truth["sessions"].append(s_meta)
        
        encoded = encode_session(s_events, var_names, n_threads)
        
        with open(data_dir / s_name, "wb") as f:
            f.write(encoded)
            
    with open(tests_dir / "ground_truth.json", "w") as f:
        json.dump(ground_truth, f, indent=2)
        
    print("Dataset generated successfully.")

if __name__ == "__main__":
    main()
