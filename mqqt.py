import asyncio
from opcua import Client as OPCUAClient
import paho.mqtt.client as mqtt

# MQTT Configuration
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_USERNAME = ''
MQTT_PASSWORD = ''
MQTT_SUBSCRIBE_TOPIC = 'device1/+/command'
MQTT_PUBLISH_TOPIC_TEMPLATE = 'device1/{device_id}/status'

# OPC UA Configuration
OPCUA_SERVER_ENDPOINT = 'opc.tcp://localhost:4840'

# Namespace index from server output
NAMESPACE_INDEX = 2

# NodeId mappings matching the server's NodeIds
NODE_ID_TO_TOPIC = {
    'ns=2;s=device1_status': 'device1/{device_id}/status',
}

TOPIC_TO_NODE_ID = {
    'device1/+/command': 'ns=2;s=device1_command',
}

class SubHandler:
    def __init__(self, mqtt_client):
        self.mqtt_client = mqtt_client

    def datachange_notification(self, node, val, data):
        print(f"OPC UA DataChange event: Node: {node}, Value: {val}")
        node_id = node.nodeid.to_string()
        topic_template = NODE_ID_TO_TOPIC.get(node_id)
        if topic_template:
            device_id = '1'  # Replace with actual logic
            topic = topic_template.format(device_id=device_id)
            self.mqtt_client.publish(topic, str(val))
            print(f"Published MQTT message: Topic: {topic}, Payload: {val}")
        else:
            print(f"No matching MQTT topic for node_id {node_id}")

class ControlCenter:
    def __init__(self):
        # Initialize MQTT client
        self.mqtt_client = mqtt.Client()
        # Initialize OPC UA client
        self.opcua_client = OPCUAClient(OPCUA_SERVER_ENDPOINT)
        # Subscription handler
        self.subscription = None

    def start(self):
        # Setup MQTT client
        self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message

        # Connect to MQTT broker
        self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.mqtt_client.loop_start()

        # Connect to OPC UA server
        self.opcua_client.connect()
        print("Connected to OPC UA server")

        # Get and print namespace array
        namespace_array = self.opcua_client.get_namespace_array()
        print("Namespace Array:", namespace_array)

        # Check if namespace index 2 corresponds to the expected namespace
        if len(namespace_array) > NAMESPACE_INDEX:
            print(f"Namespace at index {NAMESPACE_INDEX}: {namespace_array[NAMESPACE_INDEX]}")
        else:
            print(f"Namespace index {NAMESPACE_INDEX} is out of range.")
            return

        # Create a subscription to monitor variables
        handler = SubHandler(self.mqtt_client)
        self.subscription = self.opcua_client.create_subscription(100, handler)

        # Subscribe to variables of interest
        for node_id in NODE_ID_TO_TOPIC.keys():
            node = self.opcua_client.get_node(node_id)
            # Check if node exists
            try:
                value = node.get_value()
                print(f"Node {node.nodeid} exists with value {value}")
                self.subscription.subscribe_data_change(node)
            except Exception as e:
                print(f"Error accessing node {node.nodeid}: {e}")

        try:
            # Keep the script running
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            print("Control Center stopped by user")
        finally:
            self.cleanup()

    def cleanup(self):
        # Disconnect from OPC UA server
        self.opcua_client.disconnect()
        # Disconnect from MQTT broker
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def on_mqtt_connect(self, client, userdata, flags, rc):
        print("Connected to MQTT broker with result code " + str(rc))
        # Subscribe to MQTT topics
        client.subscribe(MQTT_SUBSCRIBE_TOPIC)
        print(f"Subscribed to topic: {MQTT_SUBSCRIBE_TOPIC}")

    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        print(f"Received MQTT message: Topic: {topic}, Payload: {payload}")
        # Map topic to OPC UA node ID
        node_id = None
        for pattern, nid in TOPIC_TO_NODE_ID.items():
            if mqtt.topic_matches_sub(pattern, topic):
                node_id = nid
                break
        if node_id is None:
            print("No matching OPC UA node for topic")
            return
        # Write value to OPC UA variable
        try:
            node = self.opcua_client.get_node(node_id)
            value = self.convert_payload(payload)
            node.set_value(value)
            print(f"Updated OPC UA node {node_id} with value {value}")
        except Exception as e:
            print(f"Error writing to OPC UA node: {e}")

    @staticmethod
    def convert_payload(payload):
        # Implement conversion logic as needed
        try:
            return float(payload)
        except ValueError:
            return payload

if __name__ == '__main__':
    control_center = ControlCenter()
    control_center.start()
