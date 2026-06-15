#!/usr/bin/env python3
import time
import os
import RPi.GPIO as GPIO
from picamera2 import Picamera2
import zmq
import tempfile

# ---------------- ZMQ Publisher Setup ----------------
ZMQ_ADDR = "tcp://127.0.0.1:5555"
TOPIC = b"CAM"

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind(ZMQ_ADDR)
time.sleep(0.2)

# ---------------- PIR Setup ----------------
PIR_LEFT  = 20   # Entry  → cam_0
PIR_TOP   = 26   # Mid   → cam_1
PIR_RIGHT = 21   # Exit → cam_1

PIR_PINS = [PIR_LEFT, PIR_TOP, PIR_RIGHT]

GPIO.setmode(GPIO.BCM)
for pin in PIR_PINS:
    GPIO.setup(pin, GPIO.IN)

# ---------------- Camera Setup ----------------
try:
    cam_left = Picamera2(0)   # Used by Entry PIR
    cam_left.configure(cam_left.create_still_configuration(main={"size": (1280, 720)}))
    cam_left.start()

    cam_right = Picamera2(1)  # Used by Mid and Exit PIR
    cam_right.configure(cam_right.create_still_configuration(main={"size": (1280, 720)}))
    cam_right.start()

except Exception as e:
    print("Camera initialization failed:", e)
    GPIO.cleanup()
    exit(1)

# ---------------- Helper: Capture & Publish ----------------
def capture_and_read_bytes(camera):
    """Capture image to temp file, read bytes, delete temp file"""
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        camera.capture_file(tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
        return data
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def publish_image(cam_index, pir_name, img_bytes):
    """Publish image with PIR name for proper naming in UI"""
    if img_bytes:
        msg = [
            TOPIC, 
            str(cam_index).encode("ascii"),
            pir_name.encode("ascii"),  # PIR name (Entry/Mid/Exit)
            str(time.time()).encode("ascii"), 
            img_bytes
        ]
        try:
            socket.send_multipart(msg, flags=zmq.NOBLOCK)
        except zmq.Again:
            pass 

def on_pir_left():
    print("PIR LEFT  triggered → capturing image (cam_0)")
    img = capture_and_read_bytes(cam_left)
    publish_image(0, "LEFT", img)

def on_pir_top():
    print("PIR TOP   triggered → capturing image (cam_1)")
    img = capture_and_read_bytes(cam_right)
    publish_image(1, "TOP", img)

def on_pir_right():
    print("PIR RIGHT triggered → capturing image (cam_1)")
    img = capture_and_read_bytes(cam_right)
    publish_image(1, "RIGHT", img)

PIR_MAP = [
    (PIR_LEFT,  on_pir_left),
    (PIR_TOP,   on_pir_top),
    (PIR_RIGHT, on_pir_right),
]

# ---------------- Main Loop ----------------
time.sleep(2) 

last_states = {pin: 0 for pin, _ in PIR_MAP}

try:
    while True:
        for pin, action in PIR_MAP:
            state = GPIO.input(pin)
            if state == 1 and last_states[pin] == 0:
                action()
            last_states[pin] = state

        time.sleep(0.05)

except KeyboardInterrupt:
    print("\nShutting down...")
    cam_left.stop()
    cam_right.stop()
    GPIO.cleanup()
    socket.close()
    context.term()