#!/usr/bin/env python3

import zmq
import time
import logging
from datetime import datetime
from enum import Enum, auto

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ── ZMQ Config ───────────────────────────────────────────────────
PIR_ZMQ_ADDR     = "tcp://127.0.0.1:5555"
VISITOR_ZMQ_PORT = 6008

# ── Config ───────────────────────────────────────────────────────
TIMEOUT_SECONDS = 10     
MAX_VISITORS    = 100

# ── State Machine ────────────────────────────────────────────────
class State(Enum):
    IDLE       = auto()
    SEQ_LR_P1  = auto()   # LEFT,  waiting for TOP   (L→T→R)
    SEQ_LR_P12 = auto()   # TOP,   waiting for RIGHT (L→T→R)
    SEQ_RL_P1  = auto()   # RIGHT, waiting for TOP   (R→T→L)
    SEQ_RL_P12 = auto()   # TOP,   waiting for LEFT  (R→T→L)


class VisitorPortal:

    def __init__(self):
        self.state              = State.IDLE
        self.state_time         = None
        self.visitor_count      = 0
        self.entry_seq          = 0
        self.current_visitor_id = ""
        self._setup_zmq()
        print(f"ZMQ PUB bound on port {VISITOR_ZMQ_PORT}")
        print("Visitor portal ready.")

    def _setup_zmq(self):
        self._ctx = zmq.Context()

        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.connect(PIR_ZMQ_ADDR)
        self._sub.setsockopt(zmq.SUBSCRIBE, b"CAM")
        self._sub.setsockopt(zmq.RCVTIMEO, 100)

        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.bind(f"tcp://*:{VISITOR_ZMQ_PORT}")

    def _publish_visitor(self, visitor_id, event):
        try:
            self._pub.send_json({"visitor_id": visitor_id, "event": event})
        except Exception as e:
            log.error(f"Publish error: {e}")

    def _make_visitor_id(self):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"visitor_{self.entry_seq:03d}_{ts}"

    def _on_entry(self):
        if self.current_visitor_id:
            return   # already someone inside
        if MAX_VISITORS and self.visitor_count >= MAX_VISITORS:
            print("CAPACITY FULL — Entry Blocked")
            return
        self.entry_seq         += 1
        self.visitor_count     += 1
        vid                     = self._make_visitor_id()
        self.current_visitor_id = vid
        print(f"Visitor ID: {vid} Person: Entered")
        self._publish_visitor(vid, "entry")

    def _on_exit(self):
        if not self.current_visitor_id:
            return   # no active visitor, ignore
        vid = self.current_visitor_id
        if self.visitor_count > 0:
            self.visitor_count -= 1
        print(f"Visitor ID: {vid} Person: Exited")
        self._publish_visitor(vid, "exit")
        self.current_visitor_id = ""

    def _reset_state(self):
        self.state      = State.IDLE
        self.state_time = None

    def run(self):
        try:
            while True:
                if (self.state != State.IDLE
                        and self.state_time is not None
                        and time.time() - self.state_time > TIMEOUT_SECONDS):
                    self._reset_state()

                try:
                    parts = self._sub.recv_multipart()
                    if len(parts) < 3:
                        continue
                    pir_name = parts[2].decode("ascii")
                except zmq.Again:
                    continue

                # ── IDLE ─────────────────────────────────────────
                # Either direction (L→T→R or R→T→L) can be entry or exit
                if self.state == State.IDLE:
                    if pir_name == "LEFT":
                        self.state      = State.SEQ_LR_P1
                        self.state_time = time.time()
                    elif pir_name == "RIGHT":
                        self.state      = State.SEQ_RL_P1
                        self.state_time = time.time()
                    # TOP in IDLE → ignore

                # ── L → T → R sequence ───────────────────────────
                elif self.state == State.SEQ_LR_P1:
                    if pir_name == "TOP":
                        self.state      = State.SEQ_LR_P12
                        self.state_time = time.time()
                    # LEFT re-trigger → ignore; RIGHT → ignore

                elif self.state == State.SEQ_LR_P12:
                    if pir_name == "RIGHT":
                        # No visitor inside → entry; visitor inside → exit
                        if not self.current_visitor_id:
                            self._on_entry()
                        else:
                            self._on_exit()
                        self._reset_state()
                    # TOP re-trigger → ignore; LEFT → ignore

                # ── R → T → L sequence ───────────────────────────
                elif self.state == State.SEQ_RL_P1:
                    if pir_name == "TOP":
                        self.state      = State.SEQ_RL_P12
                        self.state_time = time.time()
                    # RIGHT re-trigger → ignore; LEFT → ignore

                elif self.state == State.SEQ_RL_P12:
                    if pir_name == "LEFT":
                        # No visitor inside → entry; visitor inside → exit
                        if not self.current_visitor_id:
                            self._on_entry()
                        else:
                            self._on_exit()
                        self._reset_state()
                    # TOP re-trigger → ignore; RIGHT → ignore

        except KeyboardInterrupt:
            print("\nStopped")
            print(f"Final Count: {self.visitor_count}")
        finally:
            self._sub.close()
            self._pub.close()
            self._ctx.term()


if __name__ == "__main__":
    portal = VisitorPortal()
    portal.run()
