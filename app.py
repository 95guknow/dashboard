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

# ---------- Hardware Monitoring (parallel graphical) ----------
def collect_hardware() -> dict:
    """Collect parallel hardware metrics using psutil.
    Focus on CPU (per core), memory, top resource-heavy processes.
    GPU: best-effort (pynvml if NVIDIA present). AMD systems will show limited GPU info.
    """
    try:
        import psutil
    except Exception as e:
        return {"error": f"psutil not available: {e}"}

    ts = time.time()

    # CPU
    try:
        cpu_total = psutil.cpu_percent(interval=None)
        cpu_per_core = psutil.cpu_percent(percpu=True)
    except Exception:
        cpu_total, cpu_per_core = 0.0, []

    # RAM
    try:
        ram = psutil.virtual_memory()
        ram_pct = ram.percent
        ram_used = round(ram.used / (1024**3), 2)
        ram_total = round(ram.total / (1024**3), 2)
    except Exception:
        ram_pct, ram_used, ram_total = 0, 0, 0

    # Top hardware processes (CPU + RAM consumers)
    procs = []
    for p in psutil.process_iter(attrs=['pid', 'name', 'cpu_percent', 'memory_percent', 'memory_info']):
        try:
            info = p.info
            cpu_p = info.get('cpu_percent') or 0.0
            mem_p = info.get('memory_percent') or 0.0
            if cpu_p > 0.5 or mem_p > 0.5:
                mem_mb = 0
                if info.get('memory_info'):
                    mem_mb = round(info['memory_info'].rss / (1024*1024), 1)
                procs.append({
                    "pid": info['pid'],
                    "name": info.get('name', '?')[:28],
                    "cpu": round(cpu_p, 1),
                    "mem_pct": round(mem_p, 1),
                    "mem_mb": mem_mb
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    procs.sort(key=lambda x: (x["cpu"] or 0) + (x["mem_pct"] or 0), reverse=True)
    top_procs = procs[:10]

    # GPU - best effort
    gpu = {"available": False, "note": "GPU monitoring (NVIDIA pynvml or AMD tools)"}
    try:
        import pynvml
        pynvml.nvmlInit()
        h = pynvml.nvmlDeviceGetHandleByIndex(0)
        name = pynvml.nvmlDeviceGetName(h)
        if isinstance(name, bytes):
            name = name.decode()
        mem = pynvml.nvmlDeviceGetMemoryInfo(h)
        util = pynvml.nvmlDeviceGetUtilizationRates(h)
        try:
            temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
        except:
            temp = None
        gpu = {
            "available": True,
            "name": name,
            "util": util.gpu,
            "mem_util": util.memory,
            "vram_used_mb": round(mem.used / (1024*1024)),
            "vram_total_mb": round(mem.total / (1024*1024)),
            "temp": temp
        }
        pynvml.nvmlShutdown()
    except Exception:
        # AMD or no NVIDIA — user can extend with amd-smi / wmi / ctypes if needed
        gpu["note"] = "No NVIDIA (pynvml). For AMD use external tools or extend collector."

    return {
        "ts": ts,
        "cpu_total": cpu_total,
        "cpu_per_core": cpu_per_core,
        "ram_pct": ram_pct,
        "ram_used_gb": ram_used,
        "ram_total_gb": ram_total,
        "top_processes": top_procs,
        "gpu": gpu
    }

async def hardware_monitor():
    """Background parallel hardware monitoring loop. Emits via WS."""
    while True:
        try:
            stats = collect_hardware()
            emit({"type": "hardware", "data": stats})
        except Exception as e:
            emit({"type": "system", "msg": f"Hardware monitor error: {str(e)[:80]}"})
        await asyncio.sleep(1.8)  # ~0.55 Hz, parallel to other events

def start_hardware_monitor():
    try:
        asyncio.create_task(hardware_monitor())
    except Exception:
        pass  # may be called outside loop

@app.on_event("startup")
async def on_startup():
    start_hardware_monitor()

# ---------- End Hardware ----------

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

@app.get("/api/hardware")
async def api_hardware():
    return collect_hardware()

@app.get("/foundation", response_class=HTMLResponse)
async def foundation():
    # Serve the Heroic Core Foundation (Layer 0) spec
    from pathlib import Path as _P
    candidates = [
        BASE.parent / "heroic-core-foundation" / "FOUNDATION.md",
        _P("heroic-core-foundation") / "FOUNDATION.md",
        BASE / ".." / "heroic-core-foundation" / "FOUNDATION.md",
    ]
    for c in candidates:
        if c.exists():
            content = c.read_text(encoding="utf-8")
            html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Heroic Core Foundation</title>
<style>body{{background:#0a0a0f;color:#e2e8f0;font-family:ui-sans-serif,system-ui;margin:40px;line-height:1.6}}pre{{background:#11121a;padding:20px;border-radius:12px;overflow:auto;border:1px solid #1e1e2e}}a{{color:#40e0d0}}</style>
</head><body><h1>Heroic Core Foundation — Layer 0</h1>
<p><a href="/">← Dashboard</a> · <a href="/heroic">Heroische Philosophie</a> · <a href="/highest-layer">Highest Layer (4)</a></p>
<pre>{content}</pre>
<p style="color:#94a3b8;font-size:12px">Served from heroic-core-foundation/FOUNDATION.md</p>
</body></html>"""
            return HTMLResponse(html)
    return HTMLResponse("<h1>Foundation not found</h1><p>Expected at ../heroic-core-foundation/FOUNDATION.md</p>", status_code=404)


@app.get("/highest-layer", response_class=HTMLResponse)
async def highest_layer():
    """Serve the Highest Layer (Layer 4) spec + live load status (ohne VR default)."""
    from pathlib import Path as _P
    import json
    candidates = [
        BASE.parent / "heroic-highest-layer" / "HIGHEST_LAYER.md",
        _P("heroic-highest-layer") / "HIGHEST_LAYER.md",
        BASE / ".." / "heroic-highest-layer" / "HIGHEST_LAYER.md",
    ]
    content = "Highest Layer spec not found."
    for c in candidates:
        if c.exists():
            content = c.read_text(encoding="utf-8")
            break

    # Attempt live load (ohne VR)
    live_status = "Highest layer module not importable from here."
    try:
        import sys as _sys
        hl_path = str((BASE.parent / "heroic-highest-layer").resolve())
        if hl_path not in _sys.path:
            _sys.path.insert(0, hl_path)
        from highest_layer import load as _load
        hl = _load()
        live_status = json.dumps(hl.status(), indent=2, default=str)
    except Exception as e:
        live_status = f"Load error: {e}"

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Heroic Highest Layer (Layer 4)</title>
<style>body{{background:#0a0a0f;color:#e2e8f0;font-family:ui-sans-serif,system-ui;margin:40px;line-height:1.6}}pre{{background:#11121a;padding:20px;border-radius:12px;overflow:auto;border:1px solid #1e1e2e}}a{{color:#40e0d0}}.status{{background:#0f111a}}</style>
</head><body>
<h1>Heroic Highest Layer — Layer 4 (ohne VR)</h1>
<p><a href="/">← Dashboard</a> · <a href="/heroic">Heroic</a> · <a href="/foundation">Foundation (Layer 0)</a> · <a href="/highest-layer-vr">Mit VR</a></p>
<p><strong>Mode:</strong> Standalone · Lokal · Ohne VR — direkt, minimal, selbstständig.</p>

<h2>Live Load Status</h2>
<pre class="status">{live_status}</pre>

<h2>Spec</h2>
<pre>{content}</pre>

<p style="color:#94a3b8;font-size:12px">Served from heroic-highest-layer/ — pure logic only.</p>
</body></html>"""
    return HTMLResponse(html)


@app.get("/highest-layer-vr", response_class=HTMLResponse)
async def highest_layer_vr():
    """Serve Highest Layer mit VR (with visual protocol + assets)."""
    from pathlib import Path as _P
    import json
    spec_path = BASE.parent / "heroic-highest-layer" / "vr" / "VR_PROTOCOL.md"
    content = spec_path.read_text(encoding="utf-8") if spec_path.exists() else "VR Protocol not found."

    # Live load mit VR
    live_status = "VR layer not loadable."
    vr_visual = ""
    try:
        import sys as _sys
        hl_path = str((BASE.parent / "heroic-highest-layer").resolve())
        if hl_path not in _sys.path:
            _sys.path.insert(0, hl_path)
        from highest_layer import load_vr as _load_vr
        hl = _load_vr()
        live_status = json.dumps(hl.get_vr_status(), indent=2, default=str)
        vr_visual = hl.render_roadmap_visual().replace("\n", "<br>")
    except Exception as e:
        live_status = f"Load error: {e}"

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Heroic Highest Layer — MIT VR</title>
<style>body{{background:#0a0a0f;color:#e2e8f0;font-family:ui-sans-serif,system-ui;margin:40px;line-height:1.6}}pre{{background:#11121a;padding:20px;border-radius:12px;overflow:auto;border:1px solid #1e1e2e}}a{{color:#40e0d0}}.vr-panel{{background:#0f111a;padding:16px;border:1px solid #40e0d0;border-radius:12px}}</style>
</head><body>
<h1>Heroic Highest Layer — Layer 4 (MIT VR)</h1>
<p><a href="/">← Dashboard</a> · <a href="/highest-layer">Ohne VR</a> · <a href="/heroic">Heroic</a></p>
<p><strong>Mode:</strong> Mit VR — visual assets, VR protocol, immersive generational rendering.</p>

<h2>Live VR Status</h2>
<pre class="status">{live_status}</pre>

<div class="vr-panel">
<h3>VR Roadmap Visual</h3>
<p>{vr_visual}</p>
</div>

<h2>VR Protocol</h2>
<pre>{content}</pre>

<p style="color:#94a3b8;font-size:12px">Assets referenced from 03_VR_Assets/. Visual layer active.</p>
</body></html>"""
    return HTMLResponse(html)

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
