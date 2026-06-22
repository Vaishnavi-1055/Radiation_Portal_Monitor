#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import zmq
import json
import time

# ---------------- MQTT CONFIG ----------------
BROKER = "192.168.0.126"   # Laptop IP
PORT = 1883
PUB_TOPIC = "rpi/cps"

# ---------------- ZMQ CONFIG ----------------
ZMQ_ADDR = "tcp://localhost:6002"

print("📡 Starting CPS monitor - Reading from hardware sensors...\n")

# ==================================================
# MQTT SETUP
# ==================================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT broker")
    else:
        print("❌ MQTT connection failed:", rc)

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect

client.connect(BROKER, PORT, 60)
client.loop_start()

# ==================================================
# ZMQ SETUP
# ==================================================
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect(ZMQ_ADDR)

# Subscribe to ALL topics
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print("⏳ Waiting for real-time data from ZMQ Modbus...\n")

# ==================================================
# MAIN LOOP
# ==================================================
try:
    while True:
        # ==========================================
        # RECEIVE FROM ZMQ (NON-BLOCKING)
        # ==========================================
        try:
            topic, message = socket.recv_multipart(flags=zmq.NOBLOCK)

            print(f"📥 ZMQ Topic: {topic.decode()}")
            print(f"📥 Raw Data: {message.decode()}")

            modbus_data = json.loads(message.decode())
            print(f"✅ Parsed {len(modbus_data)} fields")

        except zmq.Again:
            # No data available yet, wait and retry
            time.sleep(0.1)
            continue

        except Exception as e:
            print(f"❌ ZMQ Error: {e}")
            time.sleep(0.1)
            continue

        # ==========================================
        # MAP ALL SENSOR POSITIONS → MQTT FORMAT
        # ==========================================
        cps_data = {
            "device_id": "RPM 1",
            "timestamp": modbus_data.get("timestamp"),
            "pmt_data": [
                {
                    "sensor_id": "LEFT_PMT",
                    "cps": modbus_data.get("Left"),
                    "lld": modbus_data.get("Left_LLD"),
                    "uld": modbus_data.get("Left_ULD"),
                    "hv": modbus_data.get("Left_HV")
                },
                {
                    "sensor_id": "TOP_PMT",
                    "cps": modbus_data.get("Top"),
                    "lld": modbus_data.get("Top_LLD"),
                    "uld": modbus_data.get("Top_ULD"),
                    "hv": modbus_data.get("Top_HV")
                },
                {
                    "sensor_id": "RIGHT_PMT",
                    "cps": modbus_data.get("Right"),
                    "lld": modbus_data.get("Right_LLD"),
                    "uld": modbus_data.get("Right_ULD"),
                    "hv": modbus_data.get("Right_HV")
                }
            ]
        }

        # Convert to JSON
        json_string = json.dumps(cps_data)

        # ==========================================
        # SEND TO MQTT
        # ==========================================
        client.publish(PUB_TOPIC, json_string)

        print("📤 Sent to MQTT:")
        print(json.dumps(cps_data, indent=2))
        print("-" * 60)

        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopped by user")

finally:
    socket.close()
    context.term()
    client.loop_stop()
    print("🔌 Disconnected from ZMQ and MQTT")
