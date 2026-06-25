# -*- coding: utf-8 -*-
"""
FastAPI Dashboard Server — Echtzeit-Denkprozess-Visualisierung
================================================================
Engine: FastAPI + Uvicorn (async, native WebSockets)
"""
from __future__ import annotations
import asyncio, json, math, os, time, uuid
from collections import deque
from pathlib import Path
from typing import Literal
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi import Request
from pydantic import BaseModel

BASE = Path(__file__).parent
app = FastAPI(title="Denkprozess Monitor")

# In-Memory Event Stream
MAX_EVENTS = 500
events: deque[dict] = deque(maxlen=MAX_EVENTS)
subscribers: list[WebSocket] = []

class EventIn(BaseModel):
    type: str = "info"
    msg: str = ""
    count: int | None = None

class WalletUpdate(BaseModel):
    amount: float = 0

WALLET_FILE = BASE / "static" / "wallet.json"
WALLET_CAP = 10000

def load_wallet() -> dict:
    if WALLET_FILE.exists():
        try:
            return json.loads(WALLET_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"balance": 0.0, "cap": WALLET_CAP}

def save_wallet(data: dict) -> None:
    WALLET_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

wallet = load_wallet()

def emit(event: dict):
    event["ts"] = time.time()
    event["id"] = str(uuid.uuid4())[:8]
    events.append(event)
    dead = []
    for ws in subscribers:
        try:
            asyncio.get_event_loop().create_task(ws.send_json(event))
        except Exception:
            dead.append(ws)
    for ws in dead:
        subscribers.remove(ws)

@app.post("/api/events")
async def post_event(payload: EventIn):
    emit({"type": payload.type, "msg": payload.msg, **({"count": payload.count} if payload.count is not None else {})})
    return {"status": "queued", "id": events[-1]["id"]}

@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(BASE / "templates" / "index.html")

@app.get("/heroic", response_class=HTMLResponse)
async def heroic():
    return FileResponse(BASE / "templates" / "heroic.html")

@app.get("/about", response_class=HTMLResponse)
async def about():
    return FileResponse(BASE / "templates" / "about.html")

@app.get("/api/wallet")
async def get_wallet():
    return wallet

@app.post("/api/wallet")
async def update_wallet(payload: WalletUpdate):
    global wallet
    wallet["balance"] = max(0.0, wallet.get("balance", 0.0) + payload.amount)
    save_wallet(wallet)
    return wallet

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    subscribers.append(ws)
    # Send current state on connect
    await ws.send_json({"type": "welcome", "event_count": len(events)})
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            if msg.get("action") == "ping":
                await ws.send_json({"type": "pong"})
            elif msg.get("action") == "clear":
                events.clear()
                emit({"type": "system", "msg": "Event-Log geleert"})
    except WebSocketDisconnect:
        if ws in subscribers:
            subscribers.remove(ws)

# ---------- Static ----------
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")

# ---------- CLI ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
