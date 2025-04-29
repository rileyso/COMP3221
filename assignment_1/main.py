import sys
import os
import config_parser as config

# Node imports
import threading
import socket
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import math

class Neighbour:
    def __init__(self, id, cost, port):
        self.id = id
        self.cost = cost
        self.port = port
    def __str__(self):
        return f"{self.id} {self.weight} {self.port}"

class Node:
    def __init__(self, id, port, config_file, 
                 routing_delay, update_interval):
        # members 
        self.id = id
        self.port_no = port
        self.routing_delay = routing_delay
        self.update_interval = update_interval
        self.ADDR = "127.0.0.1"
        self.config_file = config_file

        self.lock = threading.Lock()
        self.running = True
        self.downed = False
        self.routing_table = {}
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.routing_event = threading.Event()

        # Don't need only in query atm
        self.received_routing_updates = {}
        self.broadcast_last_update_msg = ""
        self.graph = {}
        self.neighbours = self.parse_neighbours(config_file)

    def __str__(self):
        return f"{self.id} {self.port_no} {self.routing_delay} {self.update_interval}"
    
    
    '''
    LISTEN
    '''
    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind((self.ADDR, self.port_no))
            server_sock.settimeout(1)
            while self.running:
                try:
                    data, addr = server_sock.recvfrom(1024)
                    decoded_msg = data.decode()
                    if decoded_msg.startswith("UPDATE"):
                        self.process_update(decoded_msg)
                    elif decoded_msg.startswith("QUERY_PATH"):
                        split = decoded_msg.split()
                        # print(split)
                        target = split[2]
                        return_id = split[1]
                        return_port = int(self.routing_table[return_id][2])
                        self.process_return_query_path(target, return_port)
                    elif decoded_msg.startswith("QUERY_RES"):
                        queried_path = decoded_msg.removeprefix("QUERY_RES ")
                        print(f"{queried_path}", flush=True)
                    elif decoded_msg.startswith("DEL"):
                        # print("going to delete ...")
                        self.process_delete(decoded_msg)
                        time.sleep(1)
                        self.routing_event.set()
                    else:
                        print("Error: Invalid update packet format.")
                        continue
                except socket.timeout:
                    continue
        pass
    
    '''
    STDIN LISTENER
    '''
    
    def handle_command_thread(self):
        while self.running:
            command = sys.stdin.readline().strip()
            if not command:
                continue
            self.process_command(command)
        pass
    
    # DYNAMIC COMMANDS
    def process_command(self, command):
        if command == "RESET" or command == "RESET\n":
            # self.broadcast_last_update_msg = ""
            time.sleep(self.update_interval)
            self.process_reset()
        elif command == "SPLIT" or command == "SPLIT\n":
            self.process_split_graph()
        else:
            try:
                parts = command.split()
                if parts[0] == "FAIL":
                    if len(parts) != 2:
                        print("Error: Invalid command format. Expected: FAIL <Node-ID>.",flush=True)
                        return
                    time.sleep(self.update_interval)
                    self.process_fail(parts[1])
                elif parts[0] == "RESET":
                    if len(parts) != 1:
                        print("Error: Invalid command format. Expected exactly: RESET.",flush=True)
                        return
            
                elif parts[0] == "SPLIT":
                    if len(parts) != 1:
                        print("Error: Invalid command format. Expected exactly: SPLIT.",flush=True)
                elif parts[0] == "RECOVER":
                    if len(parts) != 2:
                        print("Error: Invalid command format. Expected: RECOVER <Node-ID>.",flush=True)
                        return
                    self.process_recover(parts[1])
                    
                elif parts[0] == "CHANGE" and len(parts) == 3:
                    if len(parts) != 3:
                        print("Error: Invalid command format. Expected numeric cost value.",flush =True)
                        return
                    time.sleep(self.update_interval)
                    self.process_change(parts[1], parts[2])
                elif parts[0] == "QUERY":
                    if parts[1] != "PATH":
                        print(self.process_query(parts[1]))
                    elif parts[1] == "PATH":
                        if len(parts) == 4:
                            self.process_query_path(parts[2], parts[3])
                        else:
                            print("Error: Invalid command format. Expected valid identifiers for Source and Destination.",flush =True)
                            return
                    else:
                        print("Error: Invalid command format. Expected a valid Destination.",flush =True)
                        return
                elif parts[0] == "MERGE":
                    if len(parts) != 3:
                        print("Error: Invalid command format. Expected two valid identifiers for MERGE.",flush =True)
                        return
                    self.process_merge_nodes(parts[1], parts[2])
                elif parts[0] == "route":
                    print(f"{self.routing_table}")
                elif parts[0] == "graph":
                    print(f"{self.print_graph()}")
                    print(self.graph)
                elif parts[0] == "re":
                    self.routing_event.set()
                    print("rerouting")
                elif parts[0] == "ping":
                    print("[not locked]")
                elif parts[0] == "update":
                    print(f"[last update message {self.broadcast_last_update_msg}")
            except Exception as e:
                print(f"[]Error: Invalid {parts[0]}", flush =True)
        pass
    '''
    CHANGE
    '''
    def process_change(self, change_node, new_cost):
        try:
            new_cost = float(new_cost)
        except ValueError:
            print("Error: Invalid command format. Expected numeric cost value.")
            return
        # lock because upon changing the graph
        with self.lock:
            if change_node not in self.graph:
                self.graph[change_node] = {} # change it to nothing
            self.graph[self.id][change_node] = new_cost
            self.graph[change_node][self.id] = new_cost
            self.routing_event.set() # start routing again after the change
        
    '''
    FAIL
    '''
    def process_fail(self, failed_node):
        if failed_node not in self.routing_table and failed_node != self.id:
            print("Error: Invalid command format. Expected a valid Node-ID.",flush=True)
            return
        with self.lock:
            if failed_node == self.id:
                print(f"Node {self.id} is now DOWN.", flush=True)
                self.downed = True
            else:
                if failed_node in self.graph[self.id]:
                    self.graph[self.id][failed_node] = math.inf
                if self.id in self.graph.get(failed_node, {}):
                    self.graph[failed_node][self.id] = math.inf
                self.routing_event.set()
        return

    def print_graph(self):
        for u in self.graph:
            for v in self.graph[u]:
                print(f"{u} -- {v} = {self.graph[u][v]}")
    '''
    RECOVER
    '''
    def process_recover(self, recover_node):
        with self.lock:
            if recover_node == self.id:
                print(f"Node {self.id} is now UP.", flush=True)
                self.downed = False
            else:
                # Restore original cost from neighbours
                if recover_node in self.neighbours:
                    original_cost = self.neighbours[recover_node].cost
                    self.graph[self.id][recover_node] = original_cost
                    self.graph[recover_node][self.id] = original_cost
                    print(f"Recovered link between {self.id} and {recover_node}", flush=True)
                else:
                    print(f"Error: Invalid command format. Expected a valid Node-ID.", flush=True)
                    return
            self.routing_event.set()
    
    '''
    QUERY
    '''
    def process_query(self, target):
        if target not in self.routing_table:
            print("Error: Invalid command format. Expected a valid Node-ID.")
            return
        with self.lock:
            updated = False
            for neighbour_id, neighbour_routes in self.received_routing_updates.items():
                if neighbour_id not in self.routing_table:
                    continue  # Unknown neighbour, skip
                cost_to_neighbour = self.routing_table[neighbour_id][1]
                for dest, neighbour_cost_to_dest in neighbour_routes.items():
                    if dest == self.id:
                        continue
                    new_cost = cost_to_neighbour + neighbour_cost_to_dest
                    if (dest not in self.routing_table or 
                        new_cost < self.routing_table[dest][1]):
                        if dest in self.neighbours:
                            port = self.neighbours[dest].port
                        else:
                            port = self.neighbours[neighbour_id].port
                        self.routing_table[dest] = [neighbour_id, new_cost, port]

            for dest, (next_hop, cost, _) in sorted(self.routing_table.items()):
                if dest == target:
                    if dest == self.id:
                        continue
                    path = f"{self.id}{next_hop}{dest}" if next_hop != dest else f"{self.id}{dest}"
                    # print(f"Least cost path from {self.id} to {dest}: {path}, link cost: {cost:.1f}", flush=True)
                    return f"Least cost path from {self.id} to {dest}: {path}, link cost: {cost:.1f}"


    '''
    QUERY PATH
    '''
    def process_query_path(self, src, dest):
        if src not in self.routing_table or dest not in self.routing_table:
            print("Error: Invalid command format. Expected two valid identifiers for Source and Destination")
            return
        port = self.routing_table[src][2]
        msg = f"QUERY_PATH {self.id} {dest}"
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(msg.encode(), (self.ADDR, port))
        return
    
    '''
    RETURN QUERY_PATH
    '''
    def process_return_query_path(self, target, return_port):
        msg = self.process_query(target)
        return_msg = "QUERY_RES " + msg
        
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(return_msg.encode(), (self.ADDR, return_port))
        return
    
    '''
    MERGE
    '''
    def process_merge_nodes(self, node_target, node_aquire):
        # self.print_graph()
        with self.lock:
            if node_target not in self.graph or node_aquire not in self.graph:
                print("Error: Invalid command format. Expected two valid identifiers for Source and Destination.")
                return

            for neighbour, cost in self.graph[node_aquire].items():
                # print(neighbour)
                if neighbour == node_target:
                    continue

                # Fetch existing edge cost from node_target to neighbour
                existing_cost = self.graph[node_target].get(neighbour, float('inf'))
                min_cost = min(existing_cost, cost)

                # print(f"Checking edge {node_target} -- {neighbour}: existing={existing_cost}, new={cost} -> using={min_cost}", flush=True)

                self.graph[node_target][neighbour] = min_cost
                self.graph[neighbour][node_target] = min(self.graph[neighbour].get(node_target, float('inf')), cost)
                
                port = self.routing_table[neighbour][2]
                
                self.routing_table[neighbour] = [neighbour, min_cost, port]
            del self.graph[node_aquire]
            for neighbour in list(self.graph.keys()):
                self.graph[neighbour].pop(node_aquire, None)
            if node_aquire in self.routing_table:
                del self.routing_table[node_aquire]
            # broadcast who is deleted

            delete_req = f"DEL {node_aquire}"
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_sock:
                for dest, (id, cost, port) in self.routing_table.items():
                    client_sock.sendto(delete_req.encode(), (self.ADDR, port))

            print("Graph merged successfully.")
            # generate update
            update_msg = f"UPDATE {self.id} "
            for dest, (id, cost, port) in self.routing_table.items():
                update_msg += f"{dest}:{cost:.1f}:{port},"
            update_msg.strip(',')
            self.send_broadcast(update_msg)

            self.broadcast_last_update_msg = update_msg
            print(f"{update_msg}", flush=True)
            self.routing_event.set()

    def broadcast_quiet(self, initial_event=None, quiet=False):
        # Initial broadcast
        if initial_event is not None:
            update_msg = self.no_lock_generate_update_message()
            self.send_broadcast(update_msg)
            self.broadcast_last_update_msg = update_msg
            # initial_event.set()
        
        while self.running:
            time.sleep(self.update_interval)
            if self.downed:
                continue
                
            update_msg = self.no_lock_generate_update_message()
            if update_msg != self.broadcast_last_update_msg:
                self.send_broadcast(update_msg)
                self.broadcast_last_update_msg = update_msg
                if not quiet:
                    print(f"{update_msg}", flush=True)

    def no_lock_generate_update_message(self):
        update_msg = f"UPDATE {self.id} "
        for dest, (id, cost, port) in self.routing_table.items():
            update_msg += f"{dest}:{cost:.1f}:{port},"
        return update_msg.strip(',')
          
    
    
    '''
    RESET
    reset should open the file reset its graph and routing table. Immediately broadcast a new packet out
    '''
    def process_reset(self):
        self.graph.clear()
        self.routing_table.clear()
        self.neighbours.clear()
        self.neighbours = self.parse_neighbours(self.config_file)   # this uses the lock, so func calling a func that uses the lock makes a deadlock
        print(f"Node {self.id} has been reset.", flush=True)
        
        self.routing_event.set()
        time.sleep(self.update_interval)
        self.broadcast_last_update_msg =""
        # self.broadcast_quiet(quiet=True)
        # UPDATE NEIGHBOURS should send an update
        return
    
    '''
    SPLIT
    '''
    def process_split_graph(self):
        with self.lock:
            nodes = sorted(self.graph.keys())
            k = len(nodes) // 2
            V1 = set(nodes[:k])
            V2 = set(nodes[k:])
            for u in list(self.graph.keys()):
                for v in list(self.graph[u].keys()):
                    if (u in V1 and v in V2) or (u in V2 and v in V1):
                        del self.graph[u][v]
            print("Graph paritioned successfully.", flush =True)
            self.routing_event.set()
    
    '''
    UPDATES: CHANGING IMMEDIATE NEIGHBOURS ACCORDINGLY
    Recieve update, proces the contents, if it has updated then start the routing table
    '''
    def process_update(self, message):
        with self.lock:
            if not self.broadcast_last_update_msg:  # If we haven't broadcasted yet
                # Queue the update for later processing
                if not hasattr(self, 'pending_updates'):
                    self.pending_updates = []
                self.pending_updates.append(message)
                return
                
            # Normal update processing
            parts = message.strip().split()
            print(f"[received] {parts}",flush=True)
            if len(parts) < 3:
                return
            sender_id = parts[1]
            route_str = ' '.join(parts[2:])
            routes_entries = route_str.split(',')
            updated = False
            if sender_id not in self.graph:
                self.graph[sender_id] = {}
            for entry in routes_entries:
                
                try:
                    node_id, cost_str, _ = entry.split(':')
                    cost = float(cost_str)
                    if node_id not in self.graph:
                        
                        self.graph[node_id] = {}
                        updated = True
                    
                    old_cost = self.graph[sender_id].get(node_id, float('inf'))
                    self.graph[sender_id][node_id] = cost
                    self.graph[node_id][sender_id] = cost

                    # print(f"[process update] {node_id} new {cost} old {old_cost} ", flush=True)
                    
                    # self.routing_table[sender_id] = [sender_id, cost, port]
                    if node_id == self.id and cost != old_cost:
                        # port = self.routing_table[sender_id][2]
                        # self.routing_table[sender_id] = [sender_id, cost, port]
                        updated = True
                except ValueError:
                    continue
            
            if updated:
                self.routing_event.set()
    
    def process_pending_updates(self):
        if hasattr(self, 'pending_updates'):
            with self.lock:
                for message in self.pending_updates:
                    self.process_update(message)
                del self.pending_updates
                
    def process_delete(self, msg):
        # remove this from its graph
        merged_node = msg.split()[1]
        # print(f"deleting node from merge {merged_node}",flush=True)
        if merged_node not in self.routing_table:
            return
        if merged_node == self.id:
            self.downed=True
            return
        del self.graph[merged_node]
        del self.routing_table[merged_node]
        # print("re routing")
        # self.routing_event.set()
        return
        

    '''
    BROADCASTING/UPDATE
    '''
    def broadcast(self, initial_event=None, quiet=False):
        # Initial broadcast
        if initial_event is not None:
            update_msg = self.generate_update_message()
            self.send_broadcast(update_msg)
            self.broadcast_last_update_msg = update_msg
            initial_event.set()
        
        while self.running:
            time.sleep(self.update_interval)
            if self.downed:
                continue
                
            update_msg = self.generate_update_message()
            if update_msg != self.broadcast_last_update_msg:
                self.send_broadcast(update_msg)
                self.broadcast_last_update_msg = update_msg
                if not quiet:
                    print(f"{update_msg}", flush=True)

    def generate_update_message(self):
        with self.lock:
            update_msg = f"UPDATE {self.id} "
            for dest, (id, cost, port) in self.routing_table.items():
                update_msg += f"{dest}:{cost:.1f}:{port},"
            return update_msg.strip(',')
        
    def send_broadcast(self, update_msg):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_sock:
            for dest, (id, cost, port) in self.routing_table.items():
                client_sock.sendto(update_msg.encode(), (self.ADDR, port))
    '''
    ROUTING CALCULATIONS (RIP)
    '''
    def routing_calculate(self):
        with self.lock:
            dist = {node: math.inf for node in self.graph}
            prev = {node:None for node in self.graph}
            dist[self.id] = 0
            
            for _ in range(len(self.graph) - 1):
                for u in self.graph:
                    for v in self.graph[u]:
                        if dist[u] + self.graph[u][v] < dist[v]:
                            dist[v] = dist[u] + self.graph[u][v]
                            prev[v] = u
            for node in dist:
                if node == self.id:
                    continue
                path = []
                curr = node
                while curr and curr != self.id:
                    path.append(curr)
                    curr = prev[curr]
                if curr == self.id:
                    path.append(self.id)
                    path.reverse()
                    next_hop = path[1] if len(path) > 1 else node
                    port = self.routing_table[next_hop][2] if next_hop in self.routing_table else -1
                    
                    self.routing_table[node] = [next_hop, dist[node], port]

                    # print(f"next hop {next_hop}")
            print(f"I am Node {self.id}", flush=True)
            for dest in sorted(self.routing_table):
                if dest == self.id:
                    continue
                next_hop, cost, _ = self.routing_table[dest]
                
                path_str = f"{self.id}{next_hop}{dest}" if next_hop != dest else f"{self.id}{dest}"
                print(f"Least cost path from {self.id} to {dest}: {path_str}, link cost: {cost:.1f}", flush=True)             
                    
    '''
    LISTENER for ROUTE
    '''
    def routing_listener(self):
        time.sleep(self.routing_delay)
        self.executor.submit(self.routing_calculate)
        while self.running:
            self.routing_event.wait()
            if not self.running:
                break
            self.executor.submit(self.routing_calculate)
            self.routing_event.clear()
            
    def start(self):

        # self.routing_calculate()
        routing_thread = threading.Thread(target=self.routing_listener, daemon=True)
        listener_thread = threading.Thread(target=self.listen, daemon=True)
        command_thread = threading.Thread(target=self.handle_command_thread, daemon=True)
        broadcast_thread = threading.Thread(target=self.broadcast, daemon=True)
        self.process_pending_updates()
        initial_broadcast_event = threading.Event()
        routing_thread.start()

        self.process_pending_updates()
        # initial_broadcast_event.wait()
        listener_thread.start()
        command_thread.start()
        broadcast_thread.start()

    def stop(self):
        self.running = False
        self.executor.shutdown(wait=True)
        print(f"Node {self.id} is now DOWN.")
        
    def parse_neighbours(self, config_file):
        neighbours = {}
        try:
            with open(config_file, "r") as f:
                content = f.readlines()
        except FileNotFoundError:
            print("Error: File Not Found")
            sys.exit(1)
        n = int(content[0])

        # lock because access to the graph and routing table
        with self.lock:
            for i in range(1, n+1):
                line = content[i]
                line = line.split(' ')
                id = line[0]
                cost = float(line[1])
                port = int(line[2].strip('\n'))
                neighbour = Neighbour(id, cost, port)
                neighbours[id] = neighbour
                self.routing_table[neighbour.id] = [id, cost, port]
                
                # undirected graph
                if self.id not in self.graph:
                    self.graph[self.id] = {}
                if id not in self.graph:
                    self.graph[id] = {}
                self.graph[self.id][id] = cost
                self.graph[id][self.id] = cost
        return neighbours
    
def main():
    if len(sys.argv) != 6:
        print("Error: Insufficient arguments provided.")
        print("Usage: ./Routing.sh <Node-ID> <Port-NO> <Node-Config-File>")
        sys.exit(1)
    input = config.ArgumentLine(sys.argv)
    node_f = Node(input.node_id, input.port_no, 
                  input.config_file, input.routing_delay, input.update_interval)

    node_f.start()
    time.sleep(600)
    node_f.stop()        
    
if __name__ == "__main__":
    main()
