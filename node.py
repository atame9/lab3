"""Node for 2-Phase Commit, acting as Coordinator or Participant."""

import json  # For serializing/deserializing state to/from disk
import os  # For file system operations (checking file existence)
import sys  # For command-line argument parsing
import threading  # For concurrent operations and lock-based synchronization
import time  # For delays and timeouts in test scenarios
from multiprocessing.connection import Client  # For establishing RPC connections
import rpc_tools  # Custom RPC utilities for remote procedure calls

# Configuration - must match client.py exactly for proper communication
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),  # Node 1: IP address and port
    2: ('10.128.0.4', 17002),  # Node 2: IP address and port
    3: ('10.128.0.6', 17003),  # Node 3: IP address and port
}
AUTHKEY = b'2pc_lab_secret'  # Shared secret for authenticated connections
COORDINATOR_ID = 1
PARTICIPANT_IDS = [2, 3]
RPC_TIMEOUT = 5.0  # Seconds to wait for an RPC response before considering it failed


class TwoPCNode:
    """A node in a 2PC cluster, can be a Coordinator or Participant."""

    def __init__(self, node_id):
        # Node identity and network configuration
        self.node_id = node_id  # Unique identifier for this node (1, 2, or 3)
        self.address = PEERS_CONFIG[node_id]  # This node's (IP, port) tuple
        self.peers = {}  # Dictionary of connected peer nodes {peer_id: RPCProxy}
        
        # Thread-safe lock for protecting shared state from race conditions
        self.lock = threading.RLock()

        # Participant-specific state
        if self.node_id in PARTICIPANT_IDS:
            self.account_file = f"account_{'A' if node_id == 2 else 'B'}.txt"
            self.balance = 0
            # State for the current transaction (for recovery)
            self.tx_state_file = f"node_{self.node_id}_tx_state.json"
            self.current_tx = None # e.g., {'id': tx_id, 'state': 'PREPARED'}
            self.crash_pending = False
            self.crash_duration = 0

        # Coordinator-specific state
        if self.node_id == COORDINATOR_ID:
            self.transaction_id_counter = 0
            # In-memory log of final decisions for recovery purposes
            self.decision_log_file = "coordinator_log.json"
            self.transaction_decisions = {} # {tx_id: 'GLOBAL-COMMIT' | 'GLOBAL-ABORT'}

        # Load any previously saved state from disk
        self._load_state()

        if self.node_id in PARTICIPANT_IDS and self.current_tx:
            self._recover_transaction()

        print(f"Node {node_id} initialized. Role: {'Coordinator' if node_id == COORDINATOR_ID else 'Participant'}")

    # ========================================================================
    # Persistence
    # ========================================================================

    def _load_state(self):
        """Load persistent state from disk."""
        # This method is now more specific to loading account balance
        self._load_balance_from_file()

        if self.node_id in PARTICIPANT_IDS:
            # On startup, check for a transaction state file. This indicates the
            # node may have crashed while in the "UNCERTAIN" state for a transaction.
            # Loading this state is the first step in the recovery process.
            if os.path.exists(self.tx_state_file):
                try:
                    with open(self.tx_state_file, 'r') as f:
                        self.current_tx = json.load(f)
                    print(f"  [PARTICIPANT {self.node_id}] Recovered transaction state: {self.current_tx}")
                except (IOError, json.JSONDecodeError) as e:
                    print(f"[ERROR] Could not load transaction state from {self.tx_state_file}: {e}")
                    self.current_tx = None
                    self._clear_tx_state() # Clear corrupted file

        if self.node_id == COORDINATOR_ID:
            # On startup, load the log of previous transaction decisions. This
            # is critical for allowing the coordinator to answer recovery
            # requests from participants after a crash.
            if os.path.exists(self.decision_log_file):
                try:
                    with open(self.decision_log_file, 'r') as f:
                        self.transaction_decisions = {int(k): v for k, v in json.load(f).items()}
                    print(f"  [COORDINATOR] Recovered {len(self.transaction_decisions)} transaction decisions from log.")
                except (IOError, json.JSONDecodeError) as e:
                    print(f"[ERROR] Could not load coordinator decision log from {self.decision_log_file}: {e}")

    def _load_balance_from_file(self):
        """Load account balance from this node's file."""
        if self.node_id not in PARTICIPANT_IDS:
            return  # Only participants have accounts
        try:
            if not os.path.exists(self.account_file):
                # If the account file doesn't exist, create it with a balance of 0.
                with open(self.account_file, 'w') as f:
                    f.write('0')
            with open(self.account_file, 'r') as f:
                self.balance = float(f.read())
        except (IOError, ValueError) as e:
            print(f"[ERROR] Could not load balance from {self.account_file}: {e}. Defaulting to 0.")
            self.balance = 0

    def _persist_tx_state(self):
        """Persist the participant's current transaction state to disk (the 'log')."""
        if self.node_id not in PARTICIPANT_IDS: return
        try:
            with open(self.tx_state_file, 'w') as f:
                json.dump(self.current_tx, f)
        except Exception as e:
            print(f"[ERROR] Failed to persist transaction state: {e}")

    def _persist_coordinator_decisions(self):
        """Persist the coordinator's decision log to disk."""
        if self.node_id != COORDINATOR_ID: return
        try:
            with open(self.decision_log_file, 'w') as f:
                json.dump(self.transaction_decisions, f)
        except Exception as e:
            print(f"[ERROR] Failed to persist coordinator decisions: {e}")

    def _clear_tx_state(self):
        """Clear the transaction state from memory and disk."""
        self.current_tx = None
        if self.node_id in PARTICIPANT_IDS and os.path.exists(self.tx_state_file):
            os.remove(self.tx_state_file)

    def _persist_balance(self):
        """Internal helper to write the current in-memory balance to disk."""
        if self.node_id not in PARTICIPANT_IDS: return
        try:
            with open(self.account_file, 'w') as f:
                f.write(str(self.balance))
            print(f"  [PARTICIPANT {self.node_id}] New balance of {self.balance} persisted to disk.")
        except Exception as e:
            print(f"[ERROR] Failed to persist balance: {e}")



    # ========================================================================
    # Networking
    # ========================================================================

    def connect_to_peers(self):
        """Connect to all other nodes in the cluster."""
        print(f"Node {self.node_id}: Connecting to peers...")
        
        # Iterate through all configured peers
        for peer_id, addr in PEERS_CONFIG.items():
            # Don't try to connect to ourselves
            if peer_id == self.node_id:
                continue
            
            # Keep retrying until connection succeeds
            while peer_id not in self.peers:
                try:
                    # Establish authenticated connection to peer
                    conn = Client(addr, authkey=AUTHKEY)
                    
                    # Create RPC proxy for remote method invocation
                    self.peers[peer_id] = rpc_tools.RPCProxy(conn)
                    print(f"Node {self.node_id}: Connected to peer {peer_id}")
                except Exception:
                    # Connection failed, wait before retrying
                    time.sleep(3)
        
        print(f"--- Node {self.node_id}: All peers connected! ---\n")

    def _broadcast_rpc(self, method_name, *args):
        """Broadcast RPC call to all PARTICIPANT peers in parallel with a timeout."""
        responses = {pid: None for pid in PARTICIPANT_IDS}
        threads = []

        def rpc_worker(peer_id, proxy):
            """Worker function to make a single RPC call."""
            try:
                # Make the actual RPC call
                response = getattr(proxy, method_name)(*args)
                responses[peer_id] = response
            except Exception as e:
                # This will catch explicit connection errors, but not hangs.
                # The timeout is handled by the join() call below.
                print(f"[ERROR] RPC {method_name} to {peer_id} failed with exception: {e}")
                responses[peer_id] = None

        # Create and start a thread for each participant RPC call
        for peer_id in PARTICIPANT_IDS:
            if peer_id in self.peers:
                proxy = self.peers[peer_id]
                thread = threading.Thread(target=rpc_worker, args=(peer_id, proxy))
                threads.append(thread)
                thread.start()

        # Wait for all threads to complete, with a timeout
        for t in threads:
            t.join(timeout=RPC_TIMEOUT)

        # The responses dictionary will contain results from successful calls
        # and None for failed or timed-out calls.

        return responses

    # ========================================================================
    # Coordinator RPCs (Client-facing and internal)
    # ========================================================================

    def rpc_start_transaction(self, transaction_type, scenario_config=None):
        """Entry point for the client to start a transaction."""
        if self.node_id != COORDINATOR_ID:
            return {"status": "error", "message": "I am not the coordinator."}

        with self.lock:
            self.transaction_id_counter += 1
            tx_id = self.transaction_id_counter

        print(f"\n[COORDINATOR] Starting Transaction #{tx_id}: {transaction_type}")

        # PHASE 1: VOTE-REQUEST
        # The coordinator needs to know the initial balance of Account A for T2
        # to ensure both participants calculate the bonus based on the same
        # initial value, thus preserving the 'Consistency' property of the
        # transaction.
        initial_balance_A = None
        if transaction_type == 'T2':
            try:
                initial_balance_A = self.peers[2].rpc_get_balance()
                print(f"[COORDINATOR] TXN-{tx_id}: Fetched initial balance of Account A: ${initial_balance_A}")
            except Exception as e:
                print(f"[COORDINATOR] TXN-{tx_id}: Failed to get balance from Node 2. ABORTING. Error: {e}")
                return {"status": "error", "decision": "GLOBAL-ABORT", "reason": "Could not contact Node 2"}

        transaction_details = {'initial_A': initial_balance_A, 'scenario_config': scenario_config}

        print(f"[COORDINATOR] TXN-{tx_id}: Sending VOTE-REQUEST to participants.")
        vote_responses = self._broadcast_rpc('rpc_vote_request', tx_id, transaction_type, transaction_details)

        # Collect votes and check for failures
        votes = []
        failed_participants = []
        for pid in PARTICIPANT_IDS:
            response = vote_responses.get(pid)
            if response and response.get('vote'):
                votes.append(response['vote'])
            else:
                # This handles participant crash or network failure (response is None)
                failed_participants.append(pid)
                votes.append('VOTE-ABORT') # Treat failure as an ABORT vote
                print(f"[COORDINATOR] TXN-{tx_id}: Did not receive a valid vote from Participant {pid}. Assuming ABORT.")

        # Check if all participants voted to commit and none failed
        all_commit = all(v == "VOTE-COMMIT" for v in votes) and not failed_participants

        # PHASE 2: GLOBAL-COMMIT / GLOBAL-ABORT
        if all_commit:
            decision = "GLOBAL-COMMIT"
            print(f"[COORDINATOR] TXN-{tx_id}: All votes are COMMIT. Sending GLOBAL-COMMIT.")
        else:
            decision = "GLOBAL-ABORT"
            print(f"[COORDINATOR] TXN-{tx_id}: Received an ABORT vote or a participant failed. Sending GLOBAL-ABORT.")

        # Log the final decision for recovery purposes
        self.transaction_decisions[tx_id] = decision
        self._persist_coordinator_decisions()

        # Broadcast the final decision to all participants
        self._broadcast_rpc('rpc_decision', tx_id, decision, transaction_details)

        print(f"[COORDINATOR] TXN-{tx_id}: Transaction finished with decision: {decision}")
        return {"status": "success", "decision": decision}

    # ========================================================================
    # Participant RPCs (Called by Coordinator)
    # ========================================================================

    def rpc_vote_request(self, tx_id, transaction_type, transaction_details):
        """Participant receives a vote request from the coordinator."""
        
        # --- LOGIC FOR 1c.i (Crash BEFORE voting) ---
        # We sleep immediately, causing the Coordinator to timeout waiting for us.
        config = transaction_details.get('scenario_config', {})
        if (config and config.get('crash_node') == self.node_id and
                config.get('crash_point') == 'BEFORE_VOTE'):
            duration = config.get('crash_duration', 10)
            print(f"  [CRASH SIM] Node {self.node_id} is crashing for {duration}s BEFORE voting...")
            time.sleep(duration)
            print(f"  [CRASH SIM] Node {self.node_id} woke up, but Coordinator has likely timed out.")
            # We don't return a vote here because we were "dead". 
            # Or we return it late, but the Coord has already moved on.
            return {"vote": "VOTE-ABORT"} 

        # --- NORMAL LOGIC (1a, 1b) ---
        self._load_balance_from_file() # Ensure balance is up-to-date

        # 1. Validation for Isolation (Fixing the "Gap")
        expected_balance = transaction_details.get('initial_A')
        if transaction_type == 'T2' and self.node_id == 2 and expected_balance is not None:
             if self.balance != expected_balance:
                 print(f"  [PARTICIPANT {self.node_id}] Isolation Violation! Voting ABORT.")
                 return {"vote": "VOTE-ABORT"}

        # 2. Check Constraints
        can_commit = False
        if transaction_type == 'T1':
            if self.node_id == 2: can_commit = (self.balance >= 100)
            elif self.node_id == 3: can_commit = True
        elif transaction_type == 'T2':
            can_commit = True

        if can_commit:
            print(f"  [PARTICIPANT {self.node_id}] Constraints met. Voting VOTE-COMMIT.")
            # Log "PREPARED" state (Required for 1c.ii Recovery)
            self.current_tx = {'id': tx_id, 'state': 'PREPARED', 'type': transaction_type, 'details': transaction_details}
            self._persist_tx_state()
            print(f"  [PARTICIPANT {self.node_id}] Logged PREPARED state for TXN-{tx_id}.")

            # --- LOGIC FOR 1c.ii (Crash AFTER voting) ---
            # We set the trap here, but we DO NOT SLEEP yet. We return the vote first.
            if (config and config.get('crash_node') == self.node_id and
                    config.get('crash_point') == 'AFTER_VOTE'):
                print(f"  [CRASH SIM] Node {self.node_id} will crash AFTER returning this vote.")
                self.crash_pending = True
                self.crash_duration = config.get('crash_duration', 10)

            return {"vote": "VOTE-COMMIT"}
        else:
            print(f"  [PARTICIPANT {self.node_id}] Constraints NOT met. Voting VOTE-ABORT.")
            self._clear_tx_state()
            return {"vote": "VOTE-ABORT"}

    def rpc_decision(self, tx_id, decision, transaction_details):
        """Participant receives the final decision from the coordinator."""
        
        # --- TRIGGER FOR 1c.ii (Crash AFTER voting) ---
        # The Coordinator thinks we are alive, but we simulate a crash now.
        if getattr(self, 'crash_pending', False):
            print(f"  [CRASH SIM] Node {self.node_id} is crashing now! Ignoring decision: {decision}")
            self.crash_pending = False # Reset flag
            
            # Sleep longer than the Coordinator's timeout (5s)
            time.sleep(self.crash_duration)
            
            print(f"  [CRASH SIM] Node {self.node_id} recovered. Executing recovery protocol...")
            self._recover_transaction() # Fetch the decision we missed
            return {"status": "recovered"}

        # --- NORMAL LOGIC (1a, 1b) ---
        with self.lock:
            print(f"  [PARTICIPANT {self.node_id}] Received final decision for TXN-{tx_id}: {decision}")

            if decision == "GLOBAL-COMMIT" and self.current_tx and self.current_tx['id'] == tx_id:
                print(f"  [PARTICIPANT {self.node_id}] Committing transaction.")
                
                # Calculate the change in balance based on the transaction type
                balance_change = 0
                if self.current_tx['type'] == 'T1':
                    balance_change = -100 if self.node_id == 2 else 100
                elif self.current_tx['type'] == 'T2':
                    balance_change = 0.2 * transaction_details['initial_A']
                
                # Apply the change and log it
                self.balance += balance_change
                print(f"  [PARTICIPANT {self.node_id}] Applied {self.current_tx['type']}. New Balance: {self.balance}")
                    
                # Persist
                self._persist_balance()
                self._clear_tx_state()
            else: # GLOBAL-ABORT or inconsistent state
                print(f"  [PARTICIPANT {self.node_id}] Aborting transaction.")
                self._clear_tx_state()
        return {"status": "acknowledged"}

    def _recover_transaction(self):
        """Called on startup or after a crash to resolve an uncertain transaction."""
        if not self.current_tx:
            return

        print(f"  [RECOVERY] Node {self.node_id} is in UNCERTAIN state for TXN-{self.current_tx['id']}.")
        print(f"  [RECOVERY] Contacting coordinator to get final decision...")

        decision = None
        while not decision:
            try:
                # Connect to coordinator and ask for the decision
                coord_proxy = self.peers[COORDINATOR_ID]
                tx_id = self.current_tx['id']
                response = coord_proxy.rpc_get_transaction_decision(tx_id)
                decision = response.get('decision')
            except Exception as e:
                print(f"  [RECOVERY] Could not contact coordinator: {e}. Retrying in 3s...")
                time.sleep(3)

        print(f"  [RECOVERY] Coordinator's final decision was: {decision}")
        # Now that we have the decision, we can call the standard decision logic
        self.rpc_decision(self.current_tx['id'], decision, self.current_tx['details'])


    # ========================================================================
    # Utility RPCs
    # ========================================================================

    def rpc_get_balance(self):
        """Get the current account balance."""
        if self.node_id not in PARTICIPANT_IDS:
            return {"status": "error", "message": "I do not have an account."}
        with self.lock:
            self._load_balance_from_file() # Ensure we have the latest from disk
            return self.balance

    def rpc_set_balance(self, value):
        """Set the account balance (for test setup)."""
        if self.node_id not in PARTICIPANT_IDS:
            return {"status": "error", "message": "I do not have an account."}
        with self.lock:
            self.balance = float(value)
            self._persist_balance() # Use the internal helper to write to disk
            print(f"  [PARTICIPANT {self.node_id}] Manually set balance to: {value}")
            return {"status": "success"}

    def rpc_get_transaction_decision(self, tx_id):
        """Coordinator RPC for participants to query a transaction's final decision."""
        if self.node_id != COORDINATOR_ID:
            return {"status": "error", "message": "I am not the coordinator."}
        
        decision = self.transaction_decisions.get(tx_id)
        return {"tx_id": tx_id, "decision": decision}

# ============================================================================
# Main
# ============================================================================

def main():
    # Validate command-line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 node.py <node_id>")
        print("Example: python3 node.py 1")
        sys.exit(1)

    try:
        # Parse node_id from command line
        node_id = int(sys.argv[1])
        
        # Verify it's a valid node ID from our configuration
        if node_id not in PEERS_CONFIG:
            raise ValueError
    except ValueError:
        print(f"Invalid node_id. Must be one of {list(PEERS_CONFIG.keys())}")
        sys.exit(1)

    # Create 2PC node instance
    node = TwoPCNode(node_id)

    # Connect to peers in background thread (non-blocking)
    peer_thread = threading.Thread(target=node.connect_to_peers)
    peer_thread.daemon = True  # Allow main to exit even if thread is running
    peer_thread.start()

    # Register all RPC methods that can be called remotely
    handler = rpc_tools.RPCHandler()
    handler.register_function(node.rpc_start_transaction) # Coordinator entry point
    handler.register_function(node.rpc_vote_request)      # Participant Phase 1 (Vote)
    handler.register_function(node.rpc_decision)          # Participant Phase 2 (Commit/Abort)
    handler.register_function(node.rpc_get_balance)       # Utility to check state
    handler.register_function(node.rpc_set_balance)       # Utility to setup scenarios
    handler.register_function(node.rpc_get_transaction_decision) # For recovery

    # Start RPC server (blocks forever, handling incoming requests)
    try:
        rpc_tools.rpc_server(handler, node.address, authkey=AUTHKEY)
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Server error: {e}")


if __name__ == "__main__":
    main()