#!/usr/bin/env python3

import os, sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.getcwd())

import minimalmodbus
import serial
import zmq
import time
import threading
from datetime import datetime
import json
from alarm import AlarmManager

# ================= ZMQ CPS PUBLISHER =================
ZMQ_CPS_PORT = 6002
context = zmq.Context()
cps_pub = context.socket(zmq.PUB)
cps_pub.bind(f"tcp://*:{ZMQ_CPS_PORT}")
print(f"ZMQ CPS PUB bound to port {ZMQ_CPS_PORT}")

# ================= ZMQ PMT SUBSCRIBER =================
ZMQ_PMT_PORT = 6005
pmt_sub = context.socket(zmq.SUB)
pmt_sub.connect(f"tcp://localhost:{ZMQ_PMT_PORT}")
pmt_sub.setsockopt_string(zmq.SUBSCRIBE, "")
print(f"ZMQ PMT SUB connected to port {ZMQ_PMT_PORT}")

# ================= ALARM SETUP =================
CPS_THRESHOLD = 1000 
alarm_mgr = AlarmManager(threshold=CPS_THRESHOLD)

# ================= PMT DATA =================
pmt_data = {
    "Left": {"hv": 0, "lld": 0, "uld": 0},
    "Top":  {"hv": 0, "lld": 0, "uld": 0},
    "Right":{"hv": 0, "lld": 0, "uld": 0}
}
pmt_lock = threading.Lock()

def pmt_listener():
    while True:
        try:
            msg = pmt_sub.recv_json()
            slave = msg.get("slave")
            if slave not in pmt_data:
                continue
            with pmt_lock:
                for k in ("hv", "lld", "uld"):
                    if k in msg:
                        pmt_data[slave][k] = msg[k]
        except Exception:
            time.sleep(0.2)

threading.Thread(target=pmt_listener, daemon=True).start()

# ================= MODBUS CONFIG =================
baud_rate = 115200
com_port = "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0"
RECONNECT_DELAY = 2
SLAVE_DELAY = 0.02

# How long (seconds) without valid data before a slave is considered "stale"
STALE_TIMEOUT = 1.5

slave_map = {
    1: "Left",
    2: "Top",
    3: "Right"
}

instrument = None
instrument_lock = threading.Lock()   

# Maximum registers to read for each slave
MAX_REGS = {"Left": 75, "Top": 75, "Right": 75}

# 32-bit CPS high/low word mapping
CPS_REGS = {
    "Left":  (68, 69),
    "Top":   (68, 69),
    "Right": (68, 69)
}

# ================= PER-SLAVE STATE =================
slave_state = {
    name: {
        "last_success_time": 0.0,
        "cps":   0,
        "hv":    0,
        "lld":   0,
        "uld":   0,
        "alarm": False,
        "stale": True,
    }
    for name in slave_map.values()
}

# ================= MODBUS CONNECT =================
def connect_modbus():

    global instrument
    while True:
        try:
            if not os.path.exists(com_port):
                print(f"Waiting for {com_port}")
                time.sleep(RECONNECT_DELAY)
                continue

            instr = minimalmodbus.Instrument(com_port, 1)
            instr.serial.baudrate = baud_rate
            instr.serial.bytesize = 8
            instr.serial.stopbits = 1
            instr.serial.parity   = serial.PARITY_NONE
            instr.serial.timeout  = 0.3
            instr.mode = minimalmodbus.MODE_RTU
            instr.clear_buffers_before_each_transaction = True

            instrument = instr
            print(f"Modbus connected on {com_port}")
            return

        except Exception as e:
            print(f"Modbus connect error: {e}")
            instrument = None
            time.sleep(RECONNECT_DELAY)

# ================= READ SLAVE =================
def read_slave(slave_id, name):

    instrument.address = slave_id
    try:
        return instrument.read_registers(0, MAX_REGS[name])
    except Exception:
        time.sleep(0.1)
        instrument.address = slave_id
        return instrument.read_registers(0, MAX_REGS[name])

# ================= HANDLE COMM ERROR =================
def handle_comm_error(name, e):

    global instrument
    print(f"Modbus error ({name}): {e}")
    try:
        instrument.serial.close()
    except Exception:
        pass
    instrument = None
    connect_modbus()

# ================= START =================
with instrument_lock:
    connect_modbus()
print(f"ZMQ Modbus CPS Publisher Started @ tcp://*:{ZMQ_CPS_PORT}")

# ================= MAIN LOOP =================
while True:
    loop_start = time.time()
    timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Poll every slave independently; one failing never skips the others ---
    for slave_id, name in slave_map.items():
        try:
            with instrument_lock:
                if instrument is None:
                    raise IOError("Not connected")
                data = read_slave(slave_id, name)

            # ---- Valid data ----
            hv  = data[0]
            lld = data[1]
            uld = data[2]
            hi, lo = CPS_REGS[name]
            cps = (data[hi] << 16) | data[lo]
            alarm_status = alarm_mgr.check(cps)

            slave_state[name].update({
                "last_success_time": time.time(),
                "cps":   cps,
                "hv":    hv,
                "lld":   lld,
                "uld":   uld,
                "alarm": alarm_status,
                "stale": False,
            })

            print(
                f"{timestamp} | {name} CPS={cps}, "
                f"ALARM={'YES' if alarm_status else 'NO'}, "
                f"HV={hv}, LLD={lld}, ULD={uld}"
            )

        except (serial.SerialException,
                minimalmodbus.NoResponseError,
                minimalmodbus.InvalidResponseError,
                IOError) as e:
            # Reconnect but continue to next slave — no break!
            with instrument_lock:
                handle_comm_error(name, e)

        except Exception as e:
            print(f"Unexpected error ({name}): {e}")

        # ---- Stale check: mark if no valid data within STALE_TIMEOUT ----
        age = time.time() - slave_state[name]["last_success_time"]
        if age > STALE_TIMEOUT:
            if not slave_state[name]["stale"]:
                print(f"  {name}: no valid data for {age:.1f}s — marking stale")
            slave_state[name]["stale"] = True

        time.sleep(SLAVE_DELAY)

    # --- Build ZMQ payload (last known values + stale flag per slave) ---
    cps_result = {"timestamp": timestamp}
    for name in slave_map.values():
        s = slave_state[name]
        cps_result[name]            = s["cps"]
        cps_result[f"{name}_ALARM"] = s["alarm"]
        cps_result[f"{name}_HV"]    = s["hv"]
        cps_result[f"{name}_LLD"]   = s["lld"]
        cps_result[f"{name}_ULD"]   = s["uld"]
        cps_result[f"{name}_STALE"] = s["stale"]

    # Publish
    cps_pub.send_multipart([b"cps", json.dumps(cps_result).encode("utf-8")])

    # Measure total loop time
    elapsed = time.time() - loop_start

    print(
        f"TOTAL LOOP TIME = {elapsed:.3f} sec",
        flush=True
    )

    # Maintain ~1 second loop
    time.sleep(max(0, 1.0 - elapsed))
