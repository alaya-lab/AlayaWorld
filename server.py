"""
Demo 后端服务器 - 用于 DSW (Data Science Workshop) 部署

功能：
  1. 提供静态网页服务（index.html）
  2. WebSocket 服务接收用户的动作/prompt，推送生成的视频帧

部署方式（在阿里云 DSW 的终端中运行）：
  pip install fastapi uvicorn websockets aiofiles
  python server.py

  然后在 DSW 的"自定义服务"或端口转发中暴露 8080 端口
"""

import asyncio
import csv
import io
import json
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse

# ============================================================
# 配置
# ============================================================
HOST = "0.0.0.0"
HTTP_PORT = 8080       # 网页服务端口
WS_PORT = 8765         # WebSocket 端口（前端连接用）

STATS_TOKEN = "change-me"   # ← 看板访问口令，改成你自己的
TRAFFIC_LOG = Path(__file__).parent / "traffic.csv"

# ============================================================
# FastAPI App
# ============================================================
app = FastAPI()

# 静态文件服务
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ============================================================
# 流量统计（方案 B：服务端计数）
# ============================================================
_traffic_lock = threading.Lock()


@app.middleware("http")
async def count_traffic(request: Request, call_next):
    """每次有人打开主页，追加一行到 traffic.csv"""
    if request.url.path == "/":
        client = request.client.host if request.client else "-"
        # 反代/网关后面取真实 IP
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            client = fwd.split(",")[0].strip()
        row = [
            datetime.now(timezone.utc).isoformat(),
            client,
            request.headers.get("referer", "-"),
            request.headers.get("user-agent", "-").replace("\n", " ")[:300],
        ]
        try:
            with _traffic_lock:
                new_file = not TRAFFIC_LOG.exists()
                with open(TRAFFIC_LOG, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if new_file:
                        w.writerow(["ts", "ip", "referer", "ua"])
                    w.writerow(row)
        except Exception as e:
            print(f"[Traffic] log error: {e}")
    return await call_next(request)


def _aggregate_daily():
    """按天聚合 PV / UV，返回按日期排序的 OrderedDict"""
    days = OrderedDict()
    if not TRAFFIC_LOG.exists():
        return days
    with open(TRAFFIC_LOG, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            day = r["ts"][:10]              # YYYY-MM-DD
            d = days.setdefault(day, {"pv": 0, "ips": set()})
            d["pv"] += 1
            d["ips"].add(r["ip"])
    return OrderedDict(
        (day, {"pv": v["pv"], "uv": len(v["ips"])})
        for day, v in sorted(days.items())
    )


@app.get("/stats")
async def stats(token: str = ""):
    """流量趋势看板：/stats?token=你的口令"""
    if token != STATS_TOKEN:
        return PlainTextResponse("forbidden", status_code=403)

    daily = _aggregate_daily()
    if not daily:
        return HTMLResponse("<h2>暂无数据</h2><p>还没有人访问主页。</p>")

    total_pv = sum(d["pv"] for d in daily.values())
    total_uv_days = daily  # 每天 UV
    max_pv = max(d["pv"] for d in daily.values()) or 1

    # 生成简单的 inline SVG 柱状图（无外部依赖，不怕被墙）
    W, H, pad = 900, 320, 40
    n = len(daily)
    bw = (W - 2 * pad) / max(n, 1)
    bars, labels = [], []
    for i, (day, d) in enumerate(daily.items()):
        x = pad + i * bw
        bh = (H - 2 * pad) * d["pv"] / max_pv
        y = H - pad - bh
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw*0.7:.1f}" height="{bh:.1f}" '
            f'fill="#6c8cff"><title>{day}: PV={d["pv"]} UV={d["uv"]}</title></rect>'
        )
        if n <= 20 or i % max(1, n // 15) == 0:
            labels.append(
                f'<text x="{x+bw*0.35:.1f}" y="{H-pad+15:.1f}" font-size="10" '
                f'fill="#888" text-anchor="middle" transform="rotate(45 {x+bw*0.35:.1f} {H-pad+15:.1f})">{day[5:]}</text>'
            )
    svg = (
        f'<svg width="{W}" height="{H}" xmlns="http://www.w3.org/2000/svg">'
        f'{"".join(bars)}{"".join(labels)}'
        f'<line x1="{pad}" y1="{H-pad}" x2="{W-pad}" y2="{H-pad}" stroke="#444"/>'
        f'</svg>'
    )

    rows = "".join(
        f"<tr><td>{day}</td><td>{d['pv']}</td><td>{d['uv']}</td></tr>"
        for day, d in reversed(list(daily.items()))
    )
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <title>AlayaWorld 流量趋势</title>
    <style>
      body{{font-family:sans-serif;background:#0a0a0f;color:#e0e0e0;padding:24px;}}
      h1{{font-size:20px;}} .kpi{{margin:12px 0;color:#6c8cff;font-size:16px;}}
      table{{border-collapse:collapse;margin-top:16px;}}
      td,th{{border:1px solid #333;padding:6px 14px;text-align:right;}}
      th{{background:#1a1a24;}} td:first-child,th:first-child{{text-align:left;}}
    </style></head><body>
    <h1>AlayaWorld 主页流量趋势</h1>
    <div class="kpi">累计 PV：{total_pv} ｜ 统计天数：{len(daily)} 天 ｜ 每根柱子悬停看当天 PV/UV</div>
    {svg}
    <table><tr><th>日期</th><th>PV</th><th>UV</th></tr>{rows}</table>
    </body></html>"""
    return HTMLResponse(html)


@app.get("/")
async def index():
    """返回主页"""
    return FileResponse(Path(__file__).parent / "index.html")


# ============================================================
# WebSocket 服务（视频流式生成）
# ============================================================

# ---- 在这里替换为你的模型 ----
# from your_model import StreamingPipeline
# pipeline = StreamingPipeline()

class DummyPipeline:
    """
    模拟的 Pipeline，用于测试前端交互。
    替换为你的真实模型后删除这个类。
    """
    def __init__(self):
        self.current_prompt = ""
        self.frame_count = 0

    def set_prompt(self, prompt: str):
        self.current_prompt = prompt
        print(f"[Pipeline] Prompt switched to: {prompt[:50]}...")

    def generate_next_frame(self, keyboard, mouse):
        """
        生成下一帧（返回 JPEG bytes）

        在真实实现中：
          1. 把 keyboard/mouse 编码为动作条件
          2. DiT 少步去噪生成 latent
          3. VAE 解码为像素
          4. 编码为 JPEG 返回
        """
        import numpy as np
        self.frame_count += 1

        # 生成一个假的彩色帧用于测试
        h, w = 360, 640
        frame = np.zeros((h, w, 3), dtype=np.uint8)

        # 根据按键改变颜色
        r = 50 + keyboard[0] * 100  # W pressed → more red
        g = 50 + keyboard[2] * 100  # A pressed → more green
        b = 50 + keyboard[1] * 100  # S pressed → more blue
        frame[:, :] = [r, g, b]

        # 加个移动的方块表示动态
        x = (self.frame_count * 5) % w
        y = h // 2
        frame[y-20:y+20, x:min(x+40, w)] = [255, 255, 255]

        # 编码为 JPEG
        from PIL import Image
        img = Image.fromarray(frame)
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=80)
        return buf.getvalue()


# 初始化 Pipeline
pipeline = DummyPipeline()
# pipeline = StreamingPipeline()  # 替换为你的真实模型


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """处理 WebSocket 连接"""
    await websocket.accept()
    print("[WS] Client connected")

    try:
        while True:
            # 接收客户端消息
            data = await websocket.receive_text()
            msg = json.loads(data)

            if msg["type"] == "init":
                # 初始化生成
                pipeline.set_prompt(msg["prompt"])
                print(f"[WS] Generation started: mode={msg.get('mode')}, "
                      f"resolution={msg.get('resolution')}")

            elif msg["type"] == "action":
                # 接收动作，生成下一帧
                keyboard = msg.get("keyboard", [0, 0, 0, 0])
                mouse = msg.get("mouse", [0, 0])

                # 生成帧
                frame_bytes = pipeline.generate_next_frame(keyboard, mouse)

                # 发送帧给前端
                await websocket.send_bytes(frame_bytes)

            elif msg["type"] == "prompt_switch":
                # 切换 Prompt
                pipeline.set_prompt(msg["prompt"])
                print(f"[WS] Prompt switched")

    except WebSocketDisconnect:
        print("[WS] Client disconnected")
    except Exception as e:
        print(f"[WS] Error: {e}")


# ============================================================
# 启动服务
# ============================================================
if __name__ == "__main__":
    print(f"""
    ==========================================
    Demo Server Starting
    ==========================================
    Web UI:    http://localhost:{HTTP_PORT}
    WebSocket: ws://localhost:{HTTP_PORT}/ws

    In DSW: Use the port forwarding feature
    to access from your browser.
    ==========================================
    """)

    uvicorn.run(app, host=HOST, port=HTTP_PORT)
