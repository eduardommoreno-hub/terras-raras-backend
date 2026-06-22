from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict
import asyncpg
import os
import jwt
import bcrypt
import json
import random
import string
from datetime import datetime, timedelta
import httpx

app = FastAPI(title="Terras Raras API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()
SECRET = os.getenv("JWT_SECRET", "terras-raras-secret-2026")
DATABASE_URL = os.getenv("DATABASE_URL", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, ws: WebSocket, room_id: str):
        await ws.accept()
        if room_id not in self.rooms:
            self.rooms[room_id] = []
        self.rooms[room_id].append(ws)

    def disconnect(self, ws: WebSocket, room_id: str):
        if room_id in self.rooms:
            self.rooms[room_id] = [w for w in self.rooms[room_id] if w != ws]

    async def broadcast(self, room_id: str, message: dict, exclude: WebSocket = None):
        if room_id in self.rooms:
            dead = []
            for ws in self.rooms[room_id]:
                if ws == exclude:
                    continue
                try:
                    await ws.send_text(json.dumps(message))
                except:
                    dead.append(ws)
            for ws in dead:
                self.rooms[room_id].remove(ws)

manager = ConnectionManager()

async def get_db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS rooms (
            id VARCHAR(20) PRIMARY KEY,
            code VARCHAR(10) UNIQUE NOT NULL,
            name TEXT NOT NULL,
            owner VARCHAR(50) NOT NULL,
            zone TEXT NOT NULL,
            rivals TEXT[] DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS room_players (
            room_id VARCHAR(20) REFERENCES rooms(id) ON DELETE CASCADE,
            username VARCHAR(50) NOT NULL,
            hp INT DEFAULT 100,
            energy INT DEFAULT 80,
            strength INT DEFAULT 12,
            inventory TEXT[] DEFAULT '{"Adaga enferrujada","Cantil vazio"}',
            joined_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (room_id, username)
        );
        CREATE TABLE IF NOT EXISTS room_log (
            id SERIAL PRIMARY KEY,
            room_id VARCHAR(20) REFERENCES rooms(id) ON DELETE CASCADE,
            type VARCHAR(20) NOT NULL,
            player VARCHAR(50),
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    await conn.close()

@app.on_event("startup")
async def startup():
    if DATABASE_URL:
        await init_db()

def make_token(username: str) -> str:
    payload = {"sub": username, "exp": datetime.utcnow() + timedelta(days=30)}
    return jwt.encode(payload, SECRET, algorithm="HS256")

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)) -> str:
    try:
        data = jwt.decode(creds.credentials, SECRET, algorithms=["HS256"])
        return data["sub"]
    except:
        raise HTTPException(status_code=401, detail="Token inválido")

def gen_room_id():
    return "rm_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

def gen_room_code():
    return "SALA-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))

ZONES = [
    "Floresta Negra", "Fábrica Carmesim", "Picos Arcaicos",
    "Gelo Eterno", "Alexandria", "Tempestade dos Deuses",
    "Enclave Sombrio", "O Vazio"
]
RIVALS = ["Dim", "Katrina", "Sarah", "Mira", "Vex", "Thorne"]

class RegisterRequest(BaseModel):
    username: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class CreateRoomRequest(BaseModel):
    name: str

class JoinRoomRequest(BaseModel):
    code: str

class ActionRequest(BaseModel):
    room_id: str
    action: str
    context: Optional[str] = ""

@app.get("/health")
async def health():
    return {"status": "ok", "service": "Terras Raras"}

@app.post("/auth/register")
async def register(req: RegisterRequest, db=Depends(get_db)):
    if len(req.username) < 2 or len(req.password) < 4:
        raise HTTPException(400, "Usuário mínimo 2 chars, senha mínimo 4")
    hashed = bcrypt.hashpw(req.password.encode(), bcrypt.gensalt()).decode()
    try:
        await db.execute(
            "INSERT INTO users (username, password_hash) VALUES ($1, $2)",
            req.username, hashed
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(400, "Nome já existe")
    return {"token": make_token(req.username), "username": req.username}

@app.post("/auth/login")
async def login(req: LoginRequest, db=Depends(get_db)):
    row = await db.fetchrow("SELECT * FROM users WHERE username=$1", req.username)
    if not row or not bcrypt.checkpw(req.password.encode(), row["password_hash"].encode()):
        raise HTTPException(401, "Credenciais inválidas")
    return {"token": make_token(req.username), "username": req.username}

@app.get("/rooms/mine")
async def my_rooms(username: str = Depends(verify_token), db=Depends(get_db)):
    rows = await db.fetch("""
        SELECT r.id, r.code, r.name, r.zone, r.created_at, r.updated_at,
               (SELECT text FROM room_log WHERE room_id=r.id ORDER BY id DESC LIMIT 1) as last_entry
        FROM rooms r
        JOIN room_players rp ON r.id = rp.room_id
        WHERE rp.username = $1
        ORDER BY r.updated_at DESC
    """, username)
    return [dict(r) for r in rows]

@app.post("/rooms/create")
async def create_room(req: CreateRoomRequest, username: str = Depends(verify_token), db=Depends(get_db)):
    rid = gen_room_id()
    code = gen_room_code()
    zone = random.choice(ZONES)
    rivals = random.sample(RIVALS, 4)
    await db.execute(
        "INSERT INTO rooms (id, code, name, owner, zone, rivals) VALUES ($1,$2,$3,$4,$5,$6)",
        rid, code, req.name, username, zone, rivals
    )
    await db.execute(
        "INSERT INTO room_players (room_id, username) VALUES ($1,$2)",
        rid, username
    )
    return {"id": rid, "code": code, "name": req.name, "zone": zone, "rivals": rivals}

@app.post("/rooms/join")
async def join_room(req: JoinRoomRequest, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE code=$1", req.code.upper())
    if not room:
        raise HTTPException(404, "Sala não encontrada")
    existing = await db.fetchrow(
        "SELECT 1 FROM room_players WHERE room_id=$1 AND username=$2", room["id"], username
    )
    if not existing:
        await db.execute(
            "INSERT INTO room_players (room_id, username) VALUES ($1,$2)",
            room["id"], username
        )
    return {"id": room["id"], "code": room["code"], "name": room["name"], "zone": room["zone"], "rivals": list(room["rivals"])}

@app.get("/rooms/{room_id}")
async def get_room(room_id: str, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", room_id)
    if not room:
        raise HTTPException(404, "Sala não encontrada")
    players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", room_id)
    log = await db.fetch("SELECT * FROM room_log WHERE room_id=$1 ORDER BY id ASC", room_id)
    me = next((p for p in players if p["username"] == username), None)
    return {
        "id": room["id"],
        "code": room["code"],
        "name": room["name"],
        "zone": room["zone"],
        "rivals": list(room["rivals"]),
        "players": [dict(p) for p in players],
        "log": [dict(l) for l in log],
        "my_stats": dict(me) if me else None
    }

@app.post("/rooms/{room_id}/update-stats")
async def update_stats(room_id: str, stats: dict, username: str = Depends(verify_token), db=Depends(get_db)):
    hp = min(100, max(0, stats.get("hp", 100)))
    energy = min(100, max(0, stats.get("energy", 80)))
    inventory = stats.get("inventory", [])
    await db.execute(
        "UPDATE room_players SET hp=$1, energy=$2, inventory=$3 WHERE room_id=$4 AND username=$5",
        hp, energy, inventory, room_id, username
    )
    await db.execute("UPDATE rooms SET updated_at=NOW() WHERE id=$1", room_id)
    return {"ok": True}

@app.post("/narrate")
async def narrate(req: ActionRequest, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", req.room_id)
    if not room:
        raise HTTPException(404, "Sala não encontrada")
    me = await db.fetchrow(
        "SELECT * FROM room_players WHERE room_id=$1 AND username=$2", req.room_id, username
    )
    players = await db.fetch("SELECT username FROM room_players WHERE room_id=$1", req.room_id)
    recent_log = await db.fetch(
        "SELECT type, player, text FROM room_log WHERE room_id=$1 ORDER BY id DESC LIMIT 8",
        req.room_id
    )
    recent_log = list(reversed(recent_log))
    log_text = "\n".join([
        f"NARRADORA: {l['text']}" if l['type'] == 'narrator'
        else f"{l['player']}: {l['text']}" if l['type'] == 'player'
        else l['text']
        for l in recent_log
    ])
    player_names = [p["username"] for p in players]
    inventory = list(me["inventory"]) if me else ["Adaga enferrujada"]
    hp = me["hp"] if me else 100

    system = """Você é a narradora épica e sombria do universo TERRAS RARAS — um mundo distópico pós-colapso dividido em 8 Zonas perigosas, onde sobreviventes competem em uma arena mortal criada por Maria Julia. Escreva APENAS em português brasileiro, com prosa literária, sombria e imersiva. Nunca quebre a imersão. Nunca mencione que é uma IA."""

    prompt = f"""ZONA ATUAL: {room['zone']}
JOGADORAS: {', '.join(player_names)}
RIVAIS NA ARENA: {', '.join(room['rivals'])}
INVENTÁRIO DE {username}: {', '.join(inventory)}
HP DE {username}: {hp}/100

HISTÓRICO RECENTE:
{log_text or '(início da aventura)'}

AÇÃO ATUAL DE {username}: {req.action}

Narre as consequências com atmosfera épica e sombria. Crie tensão, surpresas, e mantenha a imersão do universo Terras Raras. Máximo 4 parágrafos."""

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 800,
                "system": system,
                "messages": [{"role": "user", "content": prompt}]
            }
        )
    data = resp.json()
    narration = data["content"][0]["text"] if data.get("content") else "A narradora está em silêncio..."

    if req.action and req.action != "__start__":
        await db.execute(
            "INSERT INTO room_log (room_id, type, player, text) VALUES ($1,'player',$2,$3)",
            req.room_id, username, req.action
        )

    await db.execute(
        "INSERT INTO room_log (room_id, type, player, text) VALUES ($1,'narrator',NULL,$2)",
        req.room_id, narration
    )
    await db.execute("UPDATE rooms SET updated_at=NOW() WHERE id=$1", req.room_id)

    await manager.broadcast(req.room_id, {
        "type": "new_entries",
        "entries": [
            {"type": "player", "player": username, "text": req.action} if req.action != "__start__" else None,
            {"type": "narrator", "text": narration}
        ]
    })

    return {"narration": narration}

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(ws: WebSocket, room_id: str):
    await manager.connect(ws, room_id)
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            await manager.broadcast(room_id, msg, exclude=ws)
    except WebSocketDisconnect:
        manager.disconnect(ws, room_id)
