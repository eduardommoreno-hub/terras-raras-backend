from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncpg, os, jwt, bcrypt, json, random, string
from datetime import datetime, timedelta
import httpx

app = FastAPI(title="Terras Raras")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
                if ws == exclude: continue
                try:
                    await ws.send_text(json.dumps(message, ensure_ascii=False))
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
            zone TEXT NOT NULL DEFAULT 'Floresta Negra',
            rivals TEXT[] DEFAULT '{}',
            phase TEXT DEFAULT 'lobby',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS room_players (
            room_id VARCHAR(20) REFERENCES rooms(id) ON DELETE CASCADE,
            username VARCHAR(50) NOT NULL,
            role VARCHAR(20) DEFAULT 'participante',
            character_id VARCHAR(30) DEFAULT '',
            character_name VARCHAR(50) DEFAULT '',
            current_zone TEXT DEFAULT 'Centro',
            hp INT DEFAULT 100,
            energy INT DEFAULT 80,
            strength INT DEFAULT 12,
            agility INT DEFAULT 10,
            intelligence INT DEFAULT 10,
            inventory TEXT[] DEFAULT '{"Adaga enferrujada","Cantil vazio"}',
            status TEXT DEFAULT 'vivo',
            joined_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (room_id, username)
        );
        CREATE TABLE IF NOT EXISTS room_log (
            id SERIAL PRIMARY KEY,
            room_id VARCHAR(20) REFERENCES rooms(id) ON DELETE CASCADE,
            type VARCHAR(20) NOT NULL,
            player VARCHAR(50),
            text TEXT NOT NULL,
            metadata JSONB DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS room_events (
            id SERIAL PRIMARY KEY,
            room_id VARCHAR(20) REFERENCES rooms(id) ON DELETE CASCADE,
            event_type VARCHAR(30) NOT NULL,
            data JSONB DEFAULT '{}',
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

def gen_id(): return "rm_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
def gen_code(): return "SALA-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))

ZONES = ["Floresta Negra","Fábrica Carmesim","Picos Arcaicos","Gelo Eterno","Alexandria","Tempestade dos Deuses","Enclave Sombrio","O Vazio"]
RIVALS = ["Dim","Katrina","Sarah","Mira","Vex","Thorne"]

class RegisterReq(BaseModel):
    username: str
    password: str

class LoginReq(BaseModel):
    username: str
    password: str

class CreateRoomReq(BaseModel):
    name: str

class JoinRoomReq(BaseModel):
    code: str

class SetRoleReq(BaseModel):
    room_id: str
    role: str
    character_id: str = ""
    character_name: str = ""

class ActionReq(BaseModel):
    room_id: str
    action: str

class MoveReq(BaseModel):
    room_id: str
    username: str
    zone: str

class UpdateStatsReq(BaseModel):
    room_id: str
    hp: Optional[int] = None
    energy: Optional[int] = None
    inventory: Optional[List[str]] = None
    status: Optional[str] = None

class SendImageReq(BaseModel):
    room_id: str
    image_id: str
    image_name: str

class MasterEventReq(BaseModel):
    room_id: str
    event_type: str
    data: dict = {}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "Terras Raras v2"}

@app.post("/auth/register")
async def register(req: RegisterReq, db=Depends(get_db)):
    if len(req.username) < 2 or len(req.password) < 4:
        raise HTTPException(400, "Usuário mínimo 2 chars, senha mínimo 4")
    hashed = bcrypt.hashpw(req.password.encode(), bcrypt.gensalt()).decode()
    try:
        await db.execute("INSERT INTO users (username, password_hash) VALUES ($1, $2)", req.username, hashed)
    except asyncpg.UniqueViolationError:
        raise HTTPException(400, "Nome já existe")
    return {"token": make_token(req.username), "username": req.username}

@app.post("/auth/login")
async def login(req: LoginReq, db=Depends(get_db)):
    row = await db.fetchrow("SELECT * FROM users WHERE username=$1", req.username)
    if not row or not bcrypt.checkpw(req.password.encode(), row["password_hash"].encode()):
        raise HTTPException(401, "Credenciais inválidas")
    return {"token": make_token(req.username), "username": req.username}

@app.get("/rooms/mine")
async def my_rooms(username: str = Depends(verify_token), db=Depends(get_db)):
    rows = await db.fetch("""
        SELECT r.id, r.code, r.name, r.zone, r.phase, r.updated_at,
               rp.role, rp.character_name,
               (SELECT text FROM room_log WHERE room_id=r.id ORDER BY id DESC LIMIT 1) as last_entry
        FROM rooms r JOIN room_players rp ON r.id=rp.room_id
        WHERE rp.username=$1 ORDER BY r.updated_at DESC
    """, username)
    return [dict(r) for r in rows]

@app.post("/rooms/create")
async def create_room(req: CreateRoomReq, username: str = Depends(verify_token), db=Depends(get_db)):
    rid = gen_id(); code = gen_code()
    zone = random.choice(ZONES)
    rivals = random.sample(RIVALS, 4)
    await db.execute("INSERT INTO rooms (id,code,name,owner,zone,rivals) VALUES ($1,$2,$3,$4,$5,$6)", rid, code, req.name, username, zone, rivals)
    await db.execute("INSERT INTO room_players (room_id,username,role) VALUES ($1,$2,'mestre')", rid, username)
    return {"id": rid, "code": code, "name": req.name, "zone": zone, "rivals": rivals, "role": "mestre"}

@app.post("/rooms/join")
async def join_room(req: JoinRoomReq, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE code=$1", req.code.upper())
    if not room: raise HTTPException(404, "Sala não encontrada")
    existing = await db.fetchrow("SELECT 1 FROM room_players WHERE room_id=$1 AND username=$2", room["id"], username)
    if not existing:
        await db.execute("INSERT INTO room_players (room_id,username) VALUES ($1,$2)", room["id"], username)
    player = await db.fetchrow("SELECT * FROM room_players WHERE room_id=$1 AND username=$2", room["id"], username)
    return {"id": room["id"], "code": room["code"], "name": room["name"], "zone": room["zone"], "rivals": list(room["rivals"]), "role": player["role"]}

@app.post("/rooms/set-role")
async def set_role(req: SetRoleReq, username: str = Depends(verify_token), db=Depends(get_db)):
    await db.execute("""
        UPDATE room_players SET role=$1, character_id=$2, character_name=$3
        WHERE room_id=$4 AND username=$5
    """, req.role, req.character_id, req.character_name, req.room_id, username)
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", req.room_id)
    players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", req.room_id)
    await manager.broadcast(req.room_id, {
        "type": "player_update",
        "players": [dict(p) for p in players]
    })
    return {"ok": True}

@app.get("/rooms/{room_id}")
async def get_room(room_id: str, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", room_id)
    if not room: raise HTTPException(404, "Sala não encontrada")
    players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", room_id)
    log = await db.fetch("SELECT * FROM room_log WHERE room_id=$1 ORDER BY id ASC LIMIT 100", room_id)
    me = next((p for p in players if p["username"] == username), None)
    return {
        "id": room["id"], "code": room["code"], "name": room["name"],
        "zone": room["zone"], "rivals": list(room["rivals"]), "phase": room["phase"],
        "owner": room["owner"],
        "players": [dict(p) for p in players],
        "log": [dict(l) for l in log],
        "my_stats": dict(me) if me else None
    }

@app.post("/rooms/move-player")
async def move_player(req: MoveReq, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", req.room_id)
    if not room: raise HTTPException(404, "Sala não encontrada")
    me = await db.fetchrow("SELECT role FROM room_players WHERE room_id=$1 AND username=$2", req.room_id, username)
    if not me: raise HTTPException(403, "Não autorizado")
    if me["role"] not in ("mestre", "ajudante") and username != req.username:
        raise HTTPException(403, "Apenas a Mestre pode mover outros jogadores")
    await db.execute("UPDATE room_players SET current_zone=$1 WHERE room_id=$2 AND username=$3", req.zone, req.room_id, req.username)
    players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", req.room_id)
    await manager.broadcast(req.room_id, {"type": "map_update", "players": [dict(p) for p in players]})
    return {"ok": True}

@app.post("/rooms/update-stats")
async def update_stats(req: UpdateStatsReq, username: str = Depends(verify_token), db=Depends(get_db)):
    sets = []
    vals = []
    i = 1
    if req.hp is not None: sets.append(f"hp=${i}"); vals.append(min(100, max(0, req.hp))); i+=1
    if req.energy is not None: sets.append(f"energy=${i}"); vals.append(min(100, max(0, req.energy))); i+=1
    if req.inventory is not None: sets.append(f"inventory=${i}"); vals.append(req.inventory); i+=1
    if req.status is not None: sets.append(f"status=${i}"); vals.append(req.status); i+=1
    if sets:
        vals.extend([req.room_id, username])
        await db.execute(f"UPDATE room_players SET {','.join(sets)} WHERE room_id=${i} AND username=${i+1}", *vals)
    await db.execute("UPDATE rooms SET updated_at=NOW() WHERE id=$1", req.room_id)
    return {"ok": True}

@app.post("/master/event")
async def master_event(req: MasterEventReq, username: str = Depends(verify_token), db=Depends(get_db)):
    me = await db.fetchrow("SELECT role FROM room_players WHERE room_id=$1 AND username=$2", req.room_id, username)
    if not me or me["role"] not in ("mestre", "ajudante"):
        raise HTTPException(403, "Apenas Mestre/Ajudante pode fazer isso")
    await db.execute("INSERT INTO room_events (room_id,event_type,data) VALUES ($1,$2,$3)", req.room_id, req.event_type, json.dumps(req.data))
    if req.event_type == "show_image":
        await manager.broadcast(req.room_id, {"type": "show_image", "image_id": req.data.get("image_id"), "image_name": req.data.get("image_name","")})
    elif req.event_type == "zone_change":
        new_zone = req.data.get("zone","")
        await db.execute("UPDATE rooms SET zone=$1, updated_at=NOW() WHERE id=$2", new_zone, req.room_id)
        await db.execute("INSERT INTO room_log (room_id,type,player,text) VALUES ($1,'system',NULL,$2)", req.room_id, f"— A Mestre moveu a aventura para: {new_zone} —")
        players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", req.room_id)
        await manager.broadcast(req.room_id, {"type": "zone_change", "zone": new_zone, "players": [dict(p) for p in players]})
    elif req.event_type == "start_game":
        await db.execute("UPDATE rooms SET phase='playing', updated_at=NOW() WHERE id=$1", req.room_id)
        await manager.broadcast(req.room_id, {"type": "game_started"})
    elif req.event_type == "atmosphere":
        await manager.broadcast(req.room_id, {"type": "atmosphere", "mood": req.data.get("mood","dark")})
    return {"ok": True}

@app.post("/narrate")
async def narrate(req: ActionReq, username: str = Depends(verify_token), db=Depends(get_db)):
    room = await db.fetchrow("SELECT * FROM rooms WHERE id=$1", req.room_id)
    if not room: raise HTTPException(404, "Sala não encontrada")
    me = await db.fetchrow("SELECT * FROM room_players WHERE room_id=$1 AND username=$2", req.room_id, username)
    players = await db.fetch("SELECT * FROM room_players WHERE room_id=$1", req.room_id)
    recent_log = await db.fetch("SELECT type,player,text FROM room_log WHERE room_id=$1 ORDER BY id DESC LIMIT 8", req.room_id)
    recent_log = list(reversed(recent_log))
    log_text = "\n".join([
        f"NARRADORA: {l['text']}" if l['type']=='narrator'
        else f"{l['player']}: {l['text']}" if l['type']=='player'
        else l['text']
        for l in recent_log
    ])
    player_names = [f"{p['username']} ({p['character_name'] or p['username']}, zona: {p['current_zone']})" for p in players]
    inventory = list(me["inventory"]) if me else ["Adaga enferrujada"]
    hp = me["hp"] if me else 100
    char_name = me["character_name"] if me and me["character_name"] else username

    system = """Você é a narradora épica e sombria do universo TERRAS RARAS — um mundo distópico pós-colapso dividido em 8 Zonas perigosas, onde sobreviventes competem em uma arena mortal. Escreva APENAS em português brasileiro, com prosa literária, sombria e imersiva. Nunca quebre a imersão. Nunca mencione IA."""

    prompt = f"""ZONA ATUAL: {room['zone']}
PERSONAGEM: {char_name} (jogadora: {username})
JOGADORAS NA ARENA: {', '.join(player_names)}
RIVAIS: {', '.join(room['rivals'])}
INVENTÁRIO: {', '.join(inventory)}
HP: {hp}/100

HISTÓRICO RECENTE:
{log_text or '(início da aventura)'}

AÇÃO: {req.action}

Narre com atmosfera épica e sombria. Máximo 3 parágrafos. Crie tensão e imersão total."""

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6", "max_tokens": 600, "system": system, "messages": [{"role": "user", "content": prompt}]}
        )
    data = resp.json()
    narration = data["content"][0]["text"] if data.get("content") else "A narradora está em silêncio..."

    if req.action and req.action != "__start__":
        await db.execute("INSERT INTO room_log (room_id,type,player,text) VALUES ($1,'player',$2,$3)", req.room_id, username, req.action)

    await db.execute("INSERT INTO room_log (room_id,type,player,text) VALUES ($1,'narrator',NULL,$2)", req.room_id, narration)
    await db.execute("UPDATE rooms SET updated_at=NOW() WHERE id=$1", req.room_id)

    await manager.broadcast(req.room_id, {
        "type": "new_entries",
        "entries": [
            {"type": "player", "player": username, "character": char_name, "text": req.action} if req.action != "__start__" else None,
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
