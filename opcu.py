from opcua import ua, Server

def start_server():
    # Set up the server
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840")
    server.set_server_name("MyOPCUAServer")

    # Register namespace
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)
    print(f"Registered namespace '{uri}' with index {idx}")

    # Get Objects node (root of the address space)
    objects = server.get_objects_node()

    # Add a custom object (node)
    device1 = objects.add_object(idx, "Device1")

    # Add variables to the node with specific NodeIds
    status_var = device1.add_variable(ua.NodeId("device1_status", idx), "device1_status", "OK")
    status_var.set_writable()
    command_var = device1.add_variable(ua.NodeId("device1_command", idx), "device1_command", "")
    command_var.set_writable()

    # Print out the NodeIds
    print("NodeIds in server:")
    print(f"Status Variable NodeId: {status_var.nodeid}")
    print(f"Command Variable NodeId: {command_var.nodeid}")

    try:
        # Start the server
        server.start()
        print("OPC UA Server is running...")
        while True:
            pass  # Keep the server running
    except KeyboardInterrupt:
        print("Server stopped by user")
    finally:
        server.stop()

if __name__ == "__main__":
    start_server()
