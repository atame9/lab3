"""Client for running 2-Phase Commit test scenarios."""

import sys  # For command-line argument parsing
import time  # For delays between operations
from multiprocessing.connection import Client  # For establishing RPC connections to nodes
import rpc_tools  # Custom RPC utilities for remote procedure calls

# Configuration - must match node.py exactly for proper communication
PEERS_CONFIG = {
    1: ('10.128.0.3', 17001),  # Node 1: Coordinator
    2: ('10.128.0.4', 17002),  # Node 2: Participant (Account A)
    3: ('10.128.0.6', 17003),  # Node 3: Participant (Account B)
}
AUTHKEY = b'2pc_lab_secret'  # Shared secret for authenticated connections
COORDINATOR_ID = 1


# ============================================================================
# Helper Functions
# ============================================================================

def _rpc_call(node_id, method_name, *args):
    """Generic RPC call helper."""
    # Get the network address (IP, port) for the target node
    addr = PEERS_CONFIG[node_id]
    
    # Establish connection to the node using authentication key
    conn = Client(addr, authkey=AUTHKEY)
    try:
        # Create RPC proxy for remote method invocation
        proxy = rpc_tools.RPCProxy(conn)
        
        # Call the remote method with provided arguments
        return getattr(proxy, method_name)(*args)
    finally:
        # Always close connection to free resources
        conn.close()


def _trigger_transaction(transaction_type, scenario_config=None):
    """Send a transaction request to the coordinator."""
    print(f"Requesting transaction '{transaction_type}' from Coordinator (Node {COORDINATOR_ID})")
    result = _rpc_call(COORDINATOR_ID, 'rpc_start_transaction', transaction_type, scenario_config)
    print(f" -> {result}")
    return result


def _show_cluster_state():
    """Display the account balances on all participant nodes."""
    print("\n=== Cluster state ===")
    try:
        balance_a = _rpc_call(2, 'rpc_get_balance')
        print(f"Account A (Node 2) Balance: {balance_a}")
    except Exception as e:
        print(f"Could not get balance for Account A: {e}")

    try:
        balance_b = _rpc_call(3, 'rpc_get_balance')
        print(f"Account B (Node 3) Balance: {balance_b}")
    except Exception as e:
        print(f"Could not get balance for Account B: {e}")
    print("=====================\n")


def _setup_accounts(balance_a, balance_b):
    """Set the initial balances for the accounts."""
    print(f"\n=== Setting up accounts: A=${balance_a}, B=${balance_b} ===")
    try:
        _rpc_call(2, 'rpc_set_balance', balance_a)
        _rpc_call(3, 'rpc_set_balance', balance_b)
        print("=== Setup complete ===\n")
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)
    time.sleep(0.5)


# ============================================================================
# Scenario Implementations
# ============================================================================

def scenario_1a():
    """Req 1.a: Successful transactions with no failures."""
    print("\n" + "="*70)
    print("SCENARIO 1a: Successful transactions (A=200, B=300)")
    print("="*70 + "\n")

    # --- Ordering 1: T1, then T2 ---
    print("--- Running Ordering 1: T1 -> T2 ---")
    _setup_accounts(200, 300)
    print("Step 1: Triggering T1 (Transfer $100 from A to B)")
    _trigger_transaction('T1')
    time.sleep(1)
    print("Intermediate state (after T1): A=100, B=400")
    _show_cluster_state()

    print("Step 2: Triggering T2 (20% bonus on current A)")
    _trigger_transaction('T2')
    time.sleep(1)
    print("Expected final state (A=120, B=420)")
    _show_cluster_state()

    # --- Ordering 2: T2, then T1 ---
    print("\n--- Running Ordering 2: T2 -> T1 ---")
    _setup_accounts(200, 300)
    print("Step 1: Triggering T2 (20% bonus on initial A)")
    _trigger_transaction('T2')
    time.sleep(1)
    print("Intermediate state (after T2): A=240, B=340")
    _show_cluster_state()

    print("Step 2: Triggering T1 (Transfer $100 from A to B)")
    _trigger_transaction('T1')
    time.sleep(1)
    print("Expected final state (A=140, B=440)")
    _show_cluster_state()


def scenario_1b():
    """Req 1.b: Abort due to business logic."""
    print("\n" + "="*70)
    print("SCENARIO 1b: Abort due to insufficient funds (A=90, B=50)")
    print("="*70 + "\n")

    # --- Ordering 1: T1, then T2 ---
    print("--- Running Ordering 1: T1 -> T2 ---")
    _setup_accounts(90, 50)
    print("Step 1: Triggering T1 (Transfer $100 from A to B) -> Should ABORT")
    _trigger_transaction('T1')
    time.sleep(1)
    print("Intermediate state (after T1 abort): A=90, B=50")
    _show_cluster_state()

    print("Step 2: Triggering T2 (20% bonus on current A) -> Should COMMIT")
    _trigger_transaction('T2')
    time.sleep(1)
    print("Expected final state (A=108, B=68)")
    _show_cluster_state()

    # --- Ordering 2: T2, then T1 ---
    print("\n--- Running Ordering 2: T2 -> T1 ---")
    _setup_accounts(90, 50)
    print("Step 1: Triggering T2 (20% bonus on initial A) -> Should COMMIT")
    _trigger_transaction('T2')
    time.sleep(1)
    print("Intermediate state (after T2): A=108, B=68")
    _show_cluster_state()

    print("Step 2: Triggering T1 (Transfer $100 from A to B) -> Should COMMIT")
    _trigger_transaction('T1')
    time.sleep(1)
    print("Expected final state (A=8, B=168)")
    _show_cluster_state()

def scenario_1c_i():
    """Req 1.c.i: Participant crashes BEFORE voting.
    
    The coordinator should time out waiting for the crashed node's vote and
    safely abort the transaction.
    """
    print("\n" + "="*70)
    print("SCENARIO 1c.i: Participant (Node 2) crashes BEFORE voting")
    print("="*70 + "\n")
    _setup_accounts(200, 300)

    print("--- Triggering T1 (Transfer $100) ---")
    print("Node 2 will receive the VOTE-REQUEST and then hang, simulating a crash.")
    print("The Coordinator should time out and ABORT the transaction.")
    config = {'crash_node': 2, 'crash_point': 'BEFORE_VOTE', 'crash_duration': 15}
    _trigger_transaction('T1', scenario_config=config)

    time.sleep(1)
    print("\nExpected final state: A=200, B=300 (Aborted, no change)")
    _show_cluster_state()

def scenario_1c_ii():
    """Req 1.c.ii: Participant crashes AFTER voting and then recovers.
    
    The participant logs its 'PREPARED' state, votes commit, and then crashes.
    Upon recovery, it contacts the coordinator to get the final decision and
    completes the transaction, ensuring durability."""
    print("\n" + "="*70)
    print("SCENARIO 1c.ii: Participant (Node 2) crashes AFTER voting, then RECOVERS")
    print("="*70 + "\n")
    _setup_accounts(200, 300)

    print("--- Triggering T1 (Transfer $100) ---")
    print("Node 2 will VOTE-COMMIT, then hang, simulating a crash before receiving the final decision.")
    config = {'crash_node': 2, 'crash_point': 'AFTER_VOTE', 'crash_duration': 10}
    _trigger_transaction('T1', scenario_config=config)

# ============================================================================
# Command-Line Interface
# ============================================================================

# Map user-friendly scenario names to their implementation functions
SCENARIOS = {
    '1a': scenario_1a,
    '1b': scenario_1b,
    '1ci': scenario_1c_i,
    '1cii': scenario_1c_ii,
    'state': _show_cluster_state,
}


def main():
    # Check if user provided a scenario argument
    if len(sys.argv) < 2:
        # Display usage instructions if no argument provided
        print("Usage: python3 client.py <scenario>")
        print("\nAvailable scenarios:")
        print("  1a                     - Successful transactions")
        print("  1b                     - Abort due to insufficient funds")
        print("  1ci                    - Participant crashes before voting")
        print("  1cii                   - Participant crashes after voting and recovers")
        print("  state                  - Show cluster state")
        sys.exit(1)
    
    # Get the scenario name from command line (case-insensitive)
    scenario_name = sys.argv[1].lower()
    
    # Look up the corresponding scenario function
    scenario_func = SCENARIOS.get(scenario_name)
    
    # Handle invalid scenario names
    if scenario_func is None:
        print(f"Unknown scenario: {scenario_name}")
        print("\nAvailable scenarios:")
        
        # Display all unique scenario names
        print("  " + "\n  ".join(sorted(SCENARIOS.keys())))
        sys.exit(1)
    
    # Execute the requested scenario
    scenario_func()


if __name__ == "__main__":
    main()