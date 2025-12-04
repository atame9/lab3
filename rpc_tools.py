# rpc_tools.py helper module
# Based on the provided " 2"

import pickle  # Provides object serialization for RPC payloads
from multiprocessing.connection import Listener, Client  # Supplies authenticated IPC primitives
from threading import Thread  # Enables per-connection worker threads


class RPCProxy:  # Client-side helper that turns attribute lookups into remote calls
    """
    A proxy object that forwards method calls to a remote server
    via a multiprocessing.connection.Connection.
    """

    def __init__(self, connection):  # Accept the Connection object that underpins RPC calls
        self._connection = connection  # Store the underlying connection for later RPC use

    def __getattr__(self, name):  # get attribute function
        def do_rpc(*args, **kwargs):
            # Send the RPC request (function name, args, kwargs)
            self._connection.send(pickle.dumps((name, args, kwargs)))  # Serialize call metadata

            # Receive the response
            result = pickle.loads(self._connection.recv())  # Deserialize remote response

            # Raise exceptions if the remote call failed
            if isinstance(result, Exception):  # Remote side already wrapped error
                raise result  # Propagate exception to caller

            # Return the successful result
            return result  # Forward normal return value

        return do_rpc  # Provide callable bound to requested attribute


class RPCHandler:  # Server-side dispatcher that executes registered RPC functions
    """
    Handles incoming RPC requests on the server side.
    """

    def __init__(self):  # Initialize an empty registry for callable endpoints
        self._functions = {}  # Registry mapping function names to callables

    def register_function(self, func):  # Expose a callable so remote clients may invoke it
        """Register a function to make it available for RPC."""
        self._functions[func.__name__] = func  # Store callable by its __name__

    def handle_connection(self, connection):  # Process all requests arriving on a single connection
        """
        Loop to receive and handle all RPC requests from a single client.
        """
        try:
            while True:  # Process messages until client disconnects
                # Receive a message
                func_name, args, kwargs = pickle.loads(connection.recv())  # Unpack request payload

                # Run the RPC and send a response
                try:
                    # Look up the function and call it
                    r = self._functions[func_name](*args, **kwargs)  # Invoke registered callable
                    connection.send(pickle.dumps(r))  # Send serialized return value
                except Exception as e:
                    # Send the exception back to the client
                    connection.send(pickle.dumps(e))  # Report failure to caller
        except EOFError:
            # Client disconnected
            pass  # Loop exits quietly when connection closes cleanly
        except Exception as e:
            print(f"Error handling connection: {e}")  # Log unexpected server-side errors
        finally:
            connection.close()  # Ensure resources freed on exit


def rpc_server(handler, address, authkey):  # Start a threaded RPC server at the desired address
    """
    Starts a multi-threaded RPC server that listens at the given
    address.
    """
    sock = Listener(address, authkey=authkey)  # Bind listener socket with auth key
    print(f"RPC Server listening on {address[0]}:{address[1]}")  # Announce listening endpoint
    while True:
        try:
            client = sock.accept()  # Wait for next incoming connection
            # Handle each client in a new thread
            t = Thread(target=handler.handle_connection, args=(client,))  # Spawn worker for connection
            t.daemon = True  # Allow process exit when only daemon threads remain
            t.start()  # Begin processing the client asynchronously
        except Exception as e:
            print(f"Server accept error: {e}")  # Log accept loop errors while continuing
