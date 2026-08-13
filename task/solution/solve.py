import os
import json
import struct
import zlib

def decode_uvarint(data, offset):
    result = 0
    shift = 0
    while True:
        b = data[offset]
        offset += 1
        result |= (b & 0x7f) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, offset

def decode_svarint(data, offset):
    val, offset = decode_uvarint(data, offset)
    decoded = (val >> 1) ^ -(val & 1)
    return decoded, offset

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
            
            if pipe_id not in self.pipes_buffer:
                self.pipes_buffer[pipe_id] = []
                self.pipes_cursor[pipe_id] = 0
                self.pipes_reversed[pipe_id] = False
                self.pipes_total_written[pipe_id] = 0
                self.pipes_total_read[pipe_id] = 0
                if pipe_id >= self.n_pipes:
                    self.n_pipes = pipe_id + 1

            self.pipes_buffer[pipe_id].extend(values)
            self.pipes_cursor[pipe_id] ^= len(values)
            self.pipes_total_written[pipe_id] += len(values)

        elif event_type == 0x04: # PIPE_READ
            pipe_id, dest_var_idx = payload
            dest_var = self.get_var_name(dest_var_idx)
            
            if pipe_id not in self.pipes_buffer:
                self.pipes_buffer[pipe_id] = []
                self.pipes_cursor[pipe_id] = 0
                self.pipes_reversed[pipe_id] = False
                self.pipes_total_written[pipe_id] = 0
                self.pipes_total_read[pipe_id] = 0
                if pipe_id >= self.n_pipes:
                    self.n_pipes = pipe_id + 1

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
            if pipe_id not in self.pipes_reversed:
                self.pipes_buffer[pipe_id] = []
                self.pipes_cursor[pipe_id] = 0
                self.pipes_reversed[pipe_id] = False
                self.pipes_total_written[pipe_id] = 0
                self.pipes_total_read[pipe_id] = 0
                if pipe_id >= self.n_pipes:
                    self.n_pipes = pipe_id + 1

            self.pipes_reversed[pipe_id] = not self.pipes_reversed[pipe_id]

        elif event_type == 0x06: # SYNC
            checkpoint_id, anchor_var_idx = payload
            self.sync_snapshots[thread_id] = self.threads[thread_id].copy()
            for i in range(self.n_pipes):
                if i in self.pipes_reversed:
                    self.pipes_reversed[i] = False

def solve_session(filepath):
    with open(filepath, "rb") as f:
        data = f.read()
        
    magic = data[0:4]
    version = data[4]
    n_threads = struct.unpack("<H", data[6:8])[0]
    dict_offset = struct.unpack("<I", data[8:12])[0]
    events_offset = struct.unpack("<I", data[12:16])[0]
    
    offset = dict_offset
    n_vars, offset = decode_uvarint(data, offset)
    var_names = []
    for _ in range(n_vars):
        vlen, offset = decode_uvarint(data, offset)
        vname = data[offset:offset+vlen].decode('utf-8')
        var_names.append(vname)
        offset += vlen
        
    sim = Simulator(n_threads, 0, n_vars, var_names)
    
    offset = events_offset
    events_processed = 0
    while offset < len(data):
        evt_type = data[offset]
        offset += 1
        t_id = struct.unpack("<H", data[offset:offset+2])[0]
        offset += 2
        ts_delta, offset = decode_uvarint(data, offset)
        
        payload = None
        if evt_type == 0x00:
            var_idx, offset = decode_uvarint(data, offset)
            val, offset = decode_svarint(data, offset)
            flags = data[offset]
            offset += 1
            payload = (var_idx, val, flags)
        elif evt_type == 0x01:
            child_id = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
            n_inh, offset = decode_uvarint(data, offset)
            inh = []
            for _ in range(n_inh):
                idx, offset = decode_uvarint(data, offset)
                inh.append(idx)
            payload = (child_id, inh)
        elif evt_type == 0x02:
            child_id = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
            anchor_idx, offset = decode_uvarint(data, offset)
            payload = (child_id, anchor_idx)
        elif evt_type == 0x03:
            pipe_id = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
            n_vals = data[offset]
            offset += 1
            vals = []
            for _ in range(n_vals):
                val, offset = decode_svarint(data, offset)
                vals.append(val)
            payload = (pipe_id, vals)
        elif evt_type == 0x04:
            pipe_id = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
            var_idx, offset = decode_uvarint(data, offset)
            payload = (pipe_id, var_idx)
        elif evt_type == 0x05:
            pipe_id = struct.unpack("<H", data[offset:offset+2])[0]
            offset += 2
            payload = (pipe_id,)
        elif evt_type == 0x06:
            checkpoint_id, offset = decode_uvarint(data, offset)
            anchor_idx, offset = decode_uvarint(data, offset)
            payload = (checkpoint_id, anchor_idx)
            
        sim.process_event(evt_type, t_id, payload)
        events_processed += 1
        
    return sim, events_processed, n_threads

def main():
    data_dir = os.environ.get("PCX_DATA_DIR", "/app/data")
    out_dir = os.environ.get("PCX_OUT_DIR", "/app/out")
    os.makedirs(out_dir, exist_ok=True)
    
    state_out = open(os.path.join(out_dir, "state.jsonl"), "w")
    pipes_out = open(os.path.join(out_dir, "pipes.jsonl"), "w")
    
    summary = []
    
    for filename in sorted(os.listdir(data_dir)):
        if filename.endswith(".pcx"):
            sim, total_events, n_threads = solve_session(os.path.join(data_dir, filename))
            
            final_state = []
            for t_id in sim.threads:
                if sim.alive[t_id]:
                    for v, val in sim.threads[t_id].items():
                        final_state.append({"thread_id": t_id, "var": v, "value": val})
                        
            final_state.sort(key=lambda x: (x["thread_id"], x["var"]))
            for item in final_state:
                state_out.write(json.dumps({"session": filename, **item}) + "\n")
                
            pipes_info = []
            for p_id in sorted(sim.pipes_buffer.keys()):
                p_info = {
                    "session": filename,
                    "pipe_id": p_id,
                    "remaining": sim.pipes_buffer[p_id],
                    "total_written": sim.pipes_total_written[p_id],
                    "total_read": sim.pipes_total_read[p_id]
                }
                pipes_out.write(json.dumps(p_info) + "\n")
                
                pipes_info.append({
                    "pipe_id": p_id,
                    "remaining": sim.pipes_buffer[p_id],
                    "total_written": sim.pipes_total_written[p_id],
                    "total_read": sim.pipes_total_read[p_id]
                })
                
            checksum = 0
            MASK64 = 0xFFFFFFFFFFFFFFFF
            for item in final_state:
                val = item["value"]
                checksum = (checksum ^ (val & MASK64)) & MASK64

            summary.append({
                "name": filename,
                "total_events": total_events,
                "n_threads": n_threads,
                "n_vars": len(sim.var_names),
                "n_pipes": sim.n_pipes,
                "alive_threads": sorted([t for t in sim.alive if sim.alive[t]]),
                "state": final_state,
                "pipes": pipes_info,
                "checksum": checksum
            })
            
    state_out.close()
    pipes_out.close()
    
    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
        
    print("Solve complete.")

if __name__ == "__main__":
    main()
