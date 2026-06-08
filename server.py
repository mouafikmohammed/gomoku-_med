"""
NetFive — Online Connect-5 Game Server
========================================

Distributed Systems assignment.

This server is implemented directly on the Berkeley stream-socket API
(SOCK_STREAM / TCP), following the socket lifecycle from the course slides:

    s = socket(AF_INET, SOCK_STREAM, 0)   # create
    bind(s, addr)                         # bind to port
    listen(s, backlog)                    # mark as passive
    conn, addr = accept(s)                # block until client connects
    recv(conn) / send(conn)               # exchange data
    close(conn)                           # tear down connection

Because browsers cannot open a raw TCP socket, the WebSocket upgrade
(RFC 6455) is performed by hand in this file so the underlying
socket calls stay fully visible. All game data travels over the same
SOCK_STREAM (TCP) connection — the connection-oriented, reliable,
in-order service described in the course material.

Concurrency: each accepted connection is dispatched to its own thread,
allowing multiple independent game rooms to run simultaneously.

Game JSON protocol
------------------
Client  →  Server:
    {"type": "join",  "name": <str>}
    {"type": "move",  "row": <int>, "col": <int>}
    {"type": "reset"}

Server  →  Client:
    {"type": "waiting"}
    {"type": "start", "color": "black"|"white", "you": <str>,
                      "opponent": <str>, "turn": "black"}
    {"type": "move",  "row": r, "col": c, "color": ..., "turn": ...}
    {"type": "over",  "result": "win"|"draw", "winner": ..., "line": [...]}
    {"type": "opponent_left"}
    {"type": "error", "message": <str>}
"""

import socket
import threading
import hashlib
import base64
import struct
import json
import os
import itertools

BOARD_SIZE = 10        # 10 × 10 grid
WIN_LENGTH = 5         # five in a row to win
WS_GUID    = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # RFC 6455 §1.3


# ============================================================================
#  SECTION 1 — WebSocket framing layer
#  (hand-written so the raw socket calls below remain unobscured)
# ============================================================================

class Connection:
    """
    Wraps a single SOCK_STREAM socket accepted from a client.
    Performs the WebSocket HTTP-upgrade handshake, then encodes/decodes
    WebSocket frames — all via the raw socket's recv() and send().
    """

    def __init__(self, sock):
        self.sock  = sock        # the connected socket returned by accept()
        self._buf  = b""
        self.alive = False

    # ------------------------------------------------------------------
    # Opening handshake — HTTP/1.1 Upgrade request over the TCP stream
    # ------------------------------------------------------------------
    def do_handshake(self):
        raw = self._recv_until(b"\r\n\r\n")
        if raw is None:
            return False
        headers = {}
        for line in raw.split(b"\r\n")[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.strip().lower()] = v.strip()
        key = headers.get(b"sec-websocket-key")
        if not key:
            return False
        digest  = hashlib.sha1(key + WS_GUID.encode()).digest()
        accept  = base64.b64encode(digest).decode()
        upgrade = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        self.sock.sendall(upgrade.encode())   # send() on the stream socket
        self.alive = True
        return True

    # ------------------------------------------------------------------
    # Internal helpers — buffered reads from the TCP stream
    # ------------------------------------------------------------------
    def _recv_until(self, delimiter):
        while delimiter not in self._buf:
            chunk = self.sock.recv(4096)      # recv() on the stream socket
            if not chunk:
                return None
            self._buf += chunk
        end = self._buf.index(delimiter) + len(delimiter)
        result, self._buf = self._buf[:end], self._buf[end:]
        return result

    def _recv_bytes(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                return None
            self._buf += chunk
        data, self._buf = self._buf[:n], self._buf[n:]
        return data

    # ------------------------------------------------------------------
    # Receive one WebSocket frame from the client
    # ------------------------------------------------------------------
    def recv_frame(self):
        header = self._recv_bytes(2)
        if header is None:
            return None
        b0, b1 = header[0], header[1]
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F

        if length == 126:
            ext = self._recv_bytes(2)
            if ext is None:
                return None
            length = struct.unpack(">H", ext)[0]
        elif length == 127:
            ext = self._recv_bytes(8)
            if ext is None:
                return None
            length = struct.unpack(">Q", ext)[0]

        mask_key = self._recv_bytes(4) if masked else b"\x00\x00\x00\x00"
        if mask_key is None:
            return None
        payload  = self._recv_bytes(length) if length else b""
        if payload is None:
            return None

        if masked:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

        if opcode == 0x8:   # close frame
            return None
        if opcode == 0x9:   # ping → send pong
            self._write_frame(payload, opcode=0xA)
            return self.recv_frame()
        if opcode == 0xA:   # pong (ignore)
            return self.recv_frame()

        return payload.decode("utf-8", "replace")

    # ------------------------------------------------------------------
    # Send one WebSocket text frame to the client (server frames unmasked)
    # ------------------------------------------------------------------
    def _write_frame(self, data, opcode=0x1):
        if isinstance(data, str):
            data = data.encode("utf-8")
        hdr = bytearray([0x80 | opcode])
        n   = len(data)
        if n < 126:
            hdr.append(n)
        elif n < (1 << 16):
            hdr.append(126)
            hdr += struct.pack(">H", n)
        else:
            hdr.append(127)
            hdr += struct.pack(">Q", n)
        try:
            self.sock.sendall(bytes(hdr) + data)   # send() on the stream socket
        except OSError:
            self.alive = False

    def send_text(self, text):
        self._write_frame(text, opcode=0x1)

    def close(self):
        try:
            self._write_frame(b"", opcode=0x8)
            self.sock.close()                      # close() the stream socket
        except OSError:
            pass
        self.alive = False


# ============================================================================
#  SECTION 2 — Game state, board logic, and server-side win detection
# ============================================================================

class GameRoom:
    _counter = itertools.count(1)

    def __init__(self):
        self.room_id = next(GameRoom._counter)
        self.board   = [[None] * BOARD_SIZE for _ in range(BOARD_SIZE)]
        self.conns   = {}      # "black" | "white"  →  Connection
        self.names   = {}      # "black" | "white"  →  display name
        self.turn    = "black"
        self.finished = False
        self.lock    = threading.Lock()

    # ------------------------------------------------------------------
    def full(self):
        return len(self.conns) == 2

    def color_for(self, conn):
        for color, c in self.conns.items():
            if c is conn:
                return color
        return None

    def add(self, conn, name):
        color = "black" if "black" not in self.conns else "white"
        self.conns[color] = conn
        self.names[color] = name or color
        return color

    def remove(self, conn):
        color = self.color_for(conn)
        if color:
            del self.conns[color]
        return color

    # ------------------------------------------------------------------
    def place(self, conn, row, col):
        """Validate and apply a move. Returns (ok, error_message)."""
        if self.finished:
            return False, "The game has ended."
        if not self.full():
            return False, "Still waiting for an opponent."
        color = self.color_for(conn)
        if color != self.turn:
            return False, "It is not your turn."
        if not (0 <= row < BOARD_SIZE and 0 <= col < BOARD_SIZE):
            return False, "Move is outside the board."
        if self.board[row][col] is not None:
            return False, "That cell is already occupied."
        self.board[row][col] = color
        return True, None

    def check_win(self, row, col):
        """Return the winning line (list of [r,c] pairs) or None."""
        color = self.board[row][col]
        for dr, dc in [(0,1),(1,0),(1,1),(1,-1)]:
            run = [(row, col)]
            r, c = row + dr, col + dc
            while 0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE and self.board[r][c] == color:
                run.append((r, c)); r += dr; c += dc
            r, c = row - dr, col - dc
            while 0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE and self.board[r][c] == color:
                run.insert(0, (r, c)); r -= dr; c -= dc
            if len(run) >= WIN_LENGTH:
                return run[:WIN_LENGTH]
        return None

    def board_full(self):
        return all(
            self.board[r][c] is not None
            for r in range(BOARD_SIZE) for c in range(BOARD_SIZE)
        )

    def reset(self):
        self.board    = [[None] * BOARD_SIZE for _ in range(BOARD_SIZE)]
        self.turn     = "black"
        self.finished = False


# ============================================================================
#  SECTION 3 — Matchmaking and per-connection game loop
# ============================================================================

_lobby      = None          # GameRoom waiting for a second player
_lobby_lock = threading.Lock()


def emit(conn, payload):
    if conn and conn.alive:
        conn.send_text(json.dumps(payload))


def broadcast(room, payload):
    for c in list(room.conns.values()):
        emit(c, payload)


def handle_join(conn, name, state):
    global _lobby
    with _lobby_lock:
        if _lobby is None:
            room = GameRoom()
            _lobby = room
            room.add(conn, name)
            state["room"] = room
            emit(conn, {"type": "waiting"})
        else:
            room = _lobby
            room.add(conn, name)
            state["room"] = room
            _lobby = None
            for color in ("black", "white"):
                opponent = "white" if color == "black" else "black"
                emit(room.conns[color], {
                    "type": "start",
                    "color": color,
                    "you": room.names[color],
                    "opponent": room.names[opponent],
                    "turn": room.turn,
                })


def handle_move(conn, room, row, col):
    with room.lock:
        ok, err = room.place(conn, row, col)
        if not ok:
            emit(conn, {"type": "error", "message": err})
            return
        color = room.board[row][col]
        win_line = room.check_win(row, col)
        if win_line:
            room.finished = True
            broadcast(room, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": None})
            broadcast(room, {"type": "over", "result": "win",
                             "winner": color, "line": win_line})
        elif room.board_full():
            room.finished = True
            broadcast(room, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": None})
            broadcast(room, {"type": "over", "result": "draw",
                             "winner": None, "line": []})
        else:
            room.turn = "white" if color == "black" else "black"
            broadcast(room, {"type": "move", "row": row, "col": col,
                             "color": color, "turn": room.turn})


def handle_reset(room):
    if room and room.full():
        with room.lock:
            room.reset()
        for color in ("black", "white"):
            opponent = "white" if color == "black" else "black"
            emit(room.conns[color], {
                "type": "start",
                "color": color,
                "you": room.names[color],
                "opponent": room.names[opponent],
                "turn": room.turn,
                "reset": True,
            })


def serve_client(conn_sock, addr):
    """Entry point for each client thread (one thread per accepted socket)."""
    global _lobby
    conn  = Connection(conn_sock)
    state = {"room": None}

    try:
        if not conn.do_handshake():
            conn_sock.close()
            return

        while True:
            raw = conn.recv_frame()
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                emit(conn, {"type": "error", "message": "Malformed message."})
                continue

            mtype = msg.get("type")
            room  = state["room"]

            if mtype == "join":
                handle_join(conn, msg.get("name", "Player"), state)
            elif mtype == "move" and room is not None:
                handle_move(conn, room,
                            int(msg.get("row", -1)),
                            int(msg.get("col", -1)))
            elif mtype == "reset" and room is not None:
                handle_reset(room)
            else:
                emit(conn, {"type": "error", "message": "Unknown command."})

    except OSError:
        pass
    finally:
        room = state["room"]
        if room is not None:
            with _lobby_lock:
                room.remove(conn)
                if _lobby is room and not room.conns:
                    _lobby = None
            for c in list(room.conns.values()):
                emit(c, {"type": "opponent_left"})
        conn.close()


# ============================================================================
#  SECTION 4 — Server entry point (Berkeley socket lifecycle)
# ============================================================================

def main():
    port = int(os.environ.get("PORT", "8765"))

    # Step 1: create a TCP stream socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Step 2: bind to all interfaces on the chosen port
    server_sock.bind(("0.0.0.0", port))

    # Step 3: start listening for incoming connections
    server_sock.listen(10)
    print(f"NetFive server listening on 0.0.0.0:{port}")

    try:
        while True:
            # Step 4: block until a client connects, then accept
            client_sock, client_addr = server_sock.accept()
            # Each connection runs in its own thread → supports concurrent games
            t = threading.Thread(
                target=serve_client,
                args=(client_sock, client_addr),
                daemon=True,
            )
            t.start()
    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        # Step 5: close the listening socket
        server_sock.close()


if __name__ == "__main__":
    main()
