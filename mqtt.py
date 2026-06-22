import paho.mqtt.client as mqtt
import threading
import time
import zmq

# ---------------- MQTT CONFIG ----------------
BROKER = "localhost"
PORT = 1883
SUB_TOPIC = "data/machine/1/readings"
PUB_TOPIC = "rpi/topic"

# ---------------- ZMQ CONFIG ----------------
ZMQ_ADDR = "tcp://127.0.0.1:5555"
ZMQ_TOPIC = b"CAM"

# Thread-safe stop flag
stop_flag = threading.Event()

# ---------------- MQTT CALLBACKS ----------------
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"\n[Laptop -> Pi]: {payload}")

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT broker:", rc)
    client.subscribe(SUB_TOPIC)

# ---------------- MQTT SETUP ----------------
client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

# ---------------- ZMQ SUB THREAD ----------------
def zmq_subscriber():
    print("[ZMQ] Subscriber thread started")

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(ZMQ_ADDR)
    socket.setsockopt(zmq.SUBSCRIBE, ZMQ_TOPIC)

    while not stop_flag.is_set():
        try:
            topic, cam_id, ts, img_bytes = socket.recv_multipart()
            timestamp = ts.decode()
            cam = cam_id.decode()

            # ---------------- Removed local saving ----------------
            client.publish(
                PUB_TOPIC,
                img_bytes
            )

            print(f"[ZMQ] Image from cam_{cam} sent via MQTT at {timestamp}")

        except Exception as e:
            print("[ZMQ] Error:", e)
            time.sleep(0.1)

    socket.close()
    context.term()
    print("[ZMQ] Subscriber stopped")

# ---------------- START ZMQ THREAD ----------------
zmq_thread = threading.Thread(target=zmq_subscriber, daemon=True)
zmq_thread.start()

# ---------------- MQTT PUBLISH LOOP ----------------
try:
    while not stop_flag.is_set():
        msg = input("[Pi -> Laptop]: ")
        client.publish(PUB_TOPIC, msg)

except KeyboardInterrupt:
    print("\nStopping manually...")

finally:
    stop_flag.set()
    zmq_thread.join()
    client.loop_stop()
    client.disconnect()
    print("MQTT + ZMQ stopped cleanly")

