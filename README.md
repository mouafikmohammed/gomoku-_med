# NetFive — Online Connect-5

A real-time, two-player Connect-5 game for the Distributed Systems assignment.
Two players in separate browsers play live: moves appear instantly on both
screens, the **server** records every move and decides the winner, and
**multiple pairs** can play at the same time.

## How it maps to the assignment

| Requirement | Implementation |
|---|---|
| Graphical client | `index.html` — draws the board, sends moves, renders state |
| Server-side language | `server.py` — Python, no external libraries |
| Stream-socket communication | Raw `SOCK_STREAM` (TCP) socket; WebSocket framing added by hand |
| Server records & relays moves | Server owns the board; clients never trust each other |
| Server judges the result | Win/draw detection is server-side only |
| Concurrent pairs | One thread per accepted socket; each pair has its own `GameRoom` |
| Invalid move handling | Out-of-turn, occupied, off-board, and post-game moves are rejected |
| 10×10 board, 5-in-a-row | `BOARD_SIZE = 10`, `WIN_LENGTH = 5` |

## Architecture

```
  Browser (Cyan)               Browser (Pink)
    index.html                   index.html
        |                             |
        +------- WebSocket (TCP) -----+----> server.py
                                              ├─ GameRoom per pair
                                              ├─ records every move
                                              ├─ relays to opponent
                                              └─ judges win / draw
```

The **client** is a static file hosted on **GitHub Pages**.  
The **server** runs as a live process on **Render** (free tier).

---

## Step 1 — Deploy the server on Render

1. Push this folder to a GitHub repository.
2. Go to [render.com](https://render.com) and sign in with GitHub.
3. Click **New → Web Service** and select your repo.
4. Configure:
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `python server.py`
   - **Plan:** Free
5. Wait for the log to show `NetFive server listening on 0.0.0.0:...`
6. Your WebSocket address is your service URL with `wss://`:
   ```
   wss://your-service-name.onrender.com
   ```

> Free Render services sleep after inactivity. If the first connection
> times out (~30 s), click **Connect & Find Match** again.

---

## Step 2 — Host the client on GitHub Pages

```bash
git init
git add .
git commit -m "NetFive"
git branch -M main
git remote add origin https://github.com/<you>/<repo>.git
git push -u origin main
```

On GitHub: **Settings → Pages → Deploy from branch → main / (root)**.

Your client will be live at:
```
https://<you>.github.io/<repo>/
```

---

## Step 3 — Play

1. Open the GitHub Pages URL in two browser windows.
2. Paste your `wss://...onrender.com` address in the **Server address** field.
3. Enter a name and click **Connect & Find Match** in both windows.
4. The first to connect plays as **Cyan** and moves first.
   Get five in a row — horizontal, vertical, or diagonal — to win.

---

## Local development

```bash
python server.py   # listens on ws://localhost:8765
```

Open `index.html` in two browser tabs and use `ws://localhost:8765`.

## Files

| File | Purpose |
|---|---|
| `server.py` | Stream-socket game server (raw TCP + WebSocket framing, rules, judging) |
| `index.html` | Browser client — the graphical interface |
| `requirements.txt` | Empty — only the Python standard library is used |
| `render.yaml` | Render deployment config |
