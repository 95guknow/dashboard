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

@app.get("/api/events")
async def get_events(limit: int = 100):
    return {"events": list(events)[-limit:]}

@app.delete("/api/events")
async def clear_events():
    events.clear()
    emit({"type": "system", "msg": "Event-Log geleert"})
    return {"status": "cleared"}

@app.get("/api/stats")
async def get_stats():
    if not events:
        return {"total": 0, "by_type": {}, "rate_per_min": 0}
    by_type: dict[str, int] = {}
    for e in events:
        t = e.get("type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    cutoff = time.time() - 60
    recent = sum(1 for e in events if e["ts"] > cutoff)
    return {"total": len(events), "by_type": by_type, "rate_per_min": recent}

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
