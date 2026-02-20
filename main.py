import os, json, datetime as dt, zipfile, re
from dataclasses import dataclass
import xml.etree.ElementTree as ET

import discord
from discord import app_commands
import asyncpg

APP_VERSION = "EconBot_v52"

# --- Timezone handling (Railway-safe) ---
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    CHICAGO_TZ = ZoneInfo("America/Chicago")
except Exception:
    CHICAGO_TZ = dt.timezone(dt.timedelta(hours=-6))  # fixed UTC-6 fallback (no DST auto-adjust)

ASSET_XLSX_FILENAME = "NEW Asset Table.xlsx"
SEP = "|||"

# If the XLSX file is not present in the deploy container, the bot can fall back to
# an embedded snapshot derived from NEW Asset Table.xlsx (SHA256=c027a200d5de80dcc405bfa0703a87a8426464f72d18123368cb719b678a591f).
# This preserves "spreadsheet is the authority" behavior for this deployment.
EMBEDDED_ASSET_XLSX_SHA256 = "c027a200d5de80dcc405bfa0703a87a8426464f72d18123368cb719b678a591f"
EMBEDDED_ASSET_ROWS_JSON = r"""[{"asset_type": "Guild Trade Workshop", "tier": "(1) Guild Apprentice", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Guild Trade Workshop", "tier": "(2) Guild Journeyman", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Guild Trade Workshop", "tier": "(3) Leased Workshop", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Guild Trade Workshop", "tier": "(4) Small Workshop", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Guild Trade Workshop", "tier": "(5) Large Workshop", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Market Stall", "tier": "(1) Consignment Arrangement", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Market Stall", "tier": "(2) Small Alley Stand", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Market Stall", "tier": "(3) Market Stall", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Market Stall", "tier": "(4) Small Shop", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Market Stall", "tier": "(5) Large Shop", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Farm/Ranch", "tier": "(1) Subsistence Surplus", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Farm/Ranch", "tier": "(2) Leased Fields", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Farm/Ranch", "tier": "(3) Owned Acre", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Farm/Ranch", "tier": "(4) Small Fields and Barn", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Farm/Ranch", "tier": "(5) Large Fields and Barn", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Tavern/Inn", "tier": "(1) One-Room Flophouse", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Tavern/Inn", "tier": "(2) Leased Establishment", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Tavern/Inn", "tier": "(3) Small Tavern", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Tavern/Inn", "tier": "(4) Large Tavern", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Tavern/Inn", "tier": "(5) Large Tavern and Inn", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Warehouse/Trade House", "tier": "(1) Small Storage Shed", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Warehouse/Trade House", "tier": "(2) Large Storage Shed", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Warehouse/Trade House", "tier": "(3) Small Trading Post", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Warehouse/Trade House", "tier": "(4) Large Trading Post", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Warehouse/Trade House", "tier": "(5) Large Warehouse and Trading Post", "cost_val": 0, "add_income_val": 0}, {"asset_type": "House", "tier": "(1) Shack", "cost_val": 0, "add_income_val": 0}, {"asset_type": "House", "tier": "(2) Hut", "cost_val": 0, "add_income_val": 0}, {"asset_type": "House", "tier": "(3) House", "cost_val": 0, "add_income_val": 0}, {"asset_type": "House", "tier": "(4) Lodge", "cost_val": 0, "add_income_val": 0}, {"asset_type": "House", "tier": "(5) Mansion", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Village", "tier": "(1) Chartered Assembly", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Village", "tier": "(2) Hamlet", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Village", "tier": "(3) Village", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Village", "tier": "(4) Town", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Village", "tier": "(5) Small City", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(1) Hit +1 / Dmg +1d4", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(2) Hit +1 / Dmg +1d6", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(3) Hit +2 / Dmg +1d8", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(4) Hit +2 / Dmg +1d10", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(5) Hit +2 / Dmg +1d12", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(1) AC +1", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(2) AC +2", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(3) AC +2 / Adv Magic Atk", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(4) AC +2 / Adv Magic and Melee Atk", "cost_val": 0, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(5) AC +3 / Adv Magic and Melee Atk", "cost_val": 0, "add_income_val": 0}]"""

def _get(name, default=None):
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default

def _req(name):
    v = _get(name)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _int(name, default=None):
    v = _get(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _int_list(name):
    v = _get(name, "") or ""
    out = []
    for p in v.split(","):
        p = p.strip()
        if p.isdigit():
            out.append(int(p))
    return out

DISCORD_TOKEN = _req("DISCORD_TOKEN")
DATABASE_URL = _req("DATABASE_URL")

# Required to avoid global drift / duplicates
GUILD_ID = _int("GUILD_ID")
if not GUILD_ID:
    raise RuntimeError("Missing required env var: GUILD_ID")

LEGACY_SOURCE_GUILD_ID = _int("LEGACY_SOURCE_GUILD_ID") or 0

BANK_CHANNEL_ID = _int("BANK_CHANNEL_ID") or 0
ECON_LOG_CHANNEL_ID = _int("ECON_LOG_CHANNEL_ID") or 0
BANK_MESSAGE_IDS = _int_list("BANK_MESSAGE_IDS")

STAFF_ROLE_IDS = set(_int_list("STAFF_ROLE_IDS"))
BANK_REFRESH_ROLE_IDS = set(_int_list("BANK_REFRESH_ROLE_IDS"))

DENOMS = [("NOVIR", 10000), ("ORIN", 1000), ("ELSH", 100), ("ARCE", 10), ("CINTH", 1)]
BASE_DAILY_INCOME_VAL = 10

def format_val(total_val):
    total_val = int(total_val)
    if total_val < 0:
        sign = "-"
        total_val = abs(total_val)
    else:
        sign = ""
    parts = []
    rem = total_val
    for code, mult in DENOMS:
        if rem <= 0:
            continue
        c = rem // mult
        if c:
            parts.append(f"{c} {code.title()}")
            rem -= c * mult
    return sign + (", ".join(parts) if parts else "0 Cinth")

@dataclass(frozen=True)
class AssetRow:
    asset_type: str
    tier: str
    cost_val: int
    add_income_val: int

def _col_letters_to_index(col_letters: str) -> int:
    col_letters = (col_letters or "").strip().upper()
    n = 0
    for ch in col_letters:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(0, n - 1)

def _cell_ref_to_col_index(cell_ref: str) -> int:
    m = re.match(r"^([A-Za-z]+)", cell_ref or "")
    if not m:
        return 0
    return _col_letters_to_index(m.group(1))

class SimpleXlsx:
    """Minimal XLSX reader (stdlib-only). Reads the FIRST worksheet."""
    def __init__(self, path: str):
        self.path = path

    def _read_shared_strings(self, zf: zipfile.ZipFile):
        try:
            data = zf.read("xl/sharedStrings.xml")
        except Exception:
            return []
        root = ET.fromstring(data)
        ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        out = []
        for si in root.findall("s:si", ns):
            texts = []
            t = si.find("s:t", ns)
            if t is not None and t.text is not None:
                texts.append(t.text)
            else:
                for r in si.findall("s:r", ns):
                    tt = r.find("s:t", ns)
                    if tt is not None and tt.text is not None:
                        texts.append(tt.text)
            out.append("".join(texts))
        return out

    def _find_first_sheet_path(self, zf: zipfile.ZipFile):
        # prefer sheet1.xml
        try:
            zf.getinfo("xl/worksheets/sheet1.xml")
            return "xl/worksheets/sheet1.xml"
        except Exception:
            pass
        # fallback via workbook relationships
        try:
            wb = ET.fromstring(zf.read("xl/workbook.xml"))
            ns = {
                "w": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            }
            sheets = wb.find("w:sheets", ns)
            if sheets is None:
                return None
            first = sheets.find("w:sheet", ns)
            if first is None:
                return None
            rid = first.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if not rid:
                return None
            rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
            rns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
            for rel in rels.findall("r:Relationship", rns):
                if rel.attrib.get("Id") == rid:
                    target = (rel.attrib.get("Target", "") or "").lstrip("/")
                    if not target.startswith("xl/"):
                        target = "xl/" + target
                    return target
        except Exception:
            return None
        return None

    def read_rows(self):
        rows = []
        with zipfile.ZipFile(self.path, "r") as zf:
            shared = self._read_shared_strings(zf)
            sheet_path = self._find_first_sheet_path(zf)
            if not sheet_path:
                return rows
            xml = ET.fromstring(zf.read(sheet_path))
            ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            sheet_data = xml.find("s:sheetData", ns)
            if sheet_data is None:
                return rows

            for row in sheet_data.findall("s:row", ns):
                cells = {}
                for c in row.findall("s:c", ns):
                    ref = c.attrib.get("r", "")
                    col_i = _cell_ref_to_col_index(ref)
                    t = c.attrib.get("t", "")
                    v = c.find("s:v", ns)
                    if t == "s":
                        try:
                            idx = int(v.text) if v is not None and v.text is not None else None
                            cells[col_i] = shared[idx] if idx is not None and 0 <= idx < len(shared) else ""
                        except Exception:
                            cells[col_i] = ""
                    elif t == "inlineStr":
                        is_el = c.find("s:is", ns)
                        tt = is_el.find("s:t", ns) if is_el is not None else None
                        cells[col_i] = tt.text if tt is not None and tt.text is not None else ""
                    else:
                        cells[col_i] = (v.text if v is not None and v.text is not None else "")

                if not cells:
                    continue
                max_col = max(cells.keys())
                out = [cells.get(i, "") for i in range(max_col + 1)]
                rows.append(out)
        return rows

class AssetCatalog:
    """
    Single source of truth: NEW Asset Table.xlsx
    Required columns (case-insensitive exact):
      - Asset Type
      - Tier
      - Cost to Acquire
      - Add to Income
    """
    def __init__(self):
        self.path = None
        self.rows = []
        self.by_type = {}
        self.by_type_tier = {}
        self.asset_types = []

    def _debug_listdir(self, p: str):
        try:
            items = os.listdir(p)
            preview = ", ".join(items[:50])
            print(f"[debug] listdir({p}): {preview}{' …' if len(items)>50 else ''}")
        except Exception as e:
            print(f"[debug] listdir({p}) failed: {e}")

    def _find_xlsx(self):
        # 1) exact expected location
        p = os.path.join("/app", ASSET_XLSX_FILENAME)
        if os.path.exists(p):
            return p

        # helpful diagnostics
        self._debug_listdir("/app")
        self._debug_listdir(os.getcwd())

        # 2) case-insensitive match in /app
        try:
            for fn in os.listdir("/app"):
                if fn.lower() == ASSET_XLSX_FILENAME.lower():
                    cand = os.path.join("/app", fn)
                    if os.path.exists(cand):
                        return cand
        except Exception:
            pass

        # 3) any .xlsx in /app that contains "asset" and "table" (fallback)
        try:
            for fn in os.listdir("/app"):
                low = fn.lower()
                if low.endswith(".xlsx") and ("asset" in low) and ("table" in low):
                    cand = os.path.join("/app", fn)
                    if os.path.exists(cand):
                        print(f"[warn] Using fallback asset spreadsheet name: {fn}")
                        return cand
        except Exception:
            pass

        # 4) working directory
        p2 = os.path.join(os.getcwd(), ASSET_XLSX_FILENAME)
        if os.path.exists(p2):
            return p2

        return None

    def load(self):
        # Startup diagnostics so Railway logs clearly show where the bot is looking.
        try:
            here = os.path.dirname(os.path.abspath(__file__))
        except Exception:
            here = '(unknown)'
        print(f"[debug] cwd: {os.getcwd()}")
        print(f"[debug] script dir: {here}")
        self._debug_listdir('/app')
        self._debug_listdir(os.getcwd())
        self.rows = []
        self.by_type = {}
        self.by_type_tier = {}
        self.asset_types = []
        self.path = self._find_xlsx()

        if not self.path:
            # Container does not have the XLSX. Fall back to embedded snapshot.
            print(f"[warn] Asset spreadsheet not found at /app/{ASSET_XLSX_FILENAME}. Using EMBEDDED asset catalog snapshot (SHA256={EMBEDDED_ASSET_XLSX_SHA256}).")
            try:
                raw_rows = json.loads(EMBEDDED_ASSET_ROWS_JSON)
                self._load_from_embedded(raw_rows)
                return
            except Exception as e:
                print(f"[warn] Embedded asset catalog failed to load: {e}")
                return

        try:
            raw = SimpleXlsx(self.path).read_rows()
        except Exception as e:
            print(f"[warn] Failed to read asset spreadsheet ({self.path}): {e}")
            return

        if not raw:
            print(f"[warn] Asset spreadsheet loaded but contains no rows: {self.path}")
            return

        header = [str(x).strip() if x is not None else "" for x in raw[0]]

        def idx_of(name):
            name = name.strip().lower()
            for i, h in enumerate(header):
                if h.strip().lower() == name:
                    return i
            return None

        i_type = idx_of("asset type")
        i_tier = idx_of("tier")
        i_cost = idx_of("cost to acquire")
        i_add = idx_of("add to income")

        if None in (i_type, i_tier, i_cost, i_add):
            print("[warn] Asset spreadsheet headers not recognized. Expected: Asset Type, Tier, Cost to Acquire, Add to Income")
            print(f"[warn] Found headers: {header}")
            return

        def to_int(x):
            if x is None:
                return 0
            s = str(x).strip()
            if s == "":
                return 0
            try:
                return int(s)
            except Exception:
                try:
                    return int(float(s))
                except Exception:
                    return 0

        seen_types = set()
        for r in raw[1:]:
            asset_type = str(r[i_type] if i_type < len(r) else "" or "").strip()
            tier = str(r[i_tier] if i_tier < len(r) else "" or "").strip()
            if not asset_type or not tier:
                continue

            cost_val = to_int(r[i_cost] if i_cost < len(r) else 0)
            add_val = to_int(r[i_add] if i_add < len(r) else 0)

            ar = AssetRow(asset_type=asset_type, tier=tier, cost_val=cost_val, add_income_val=add_val)
            self.rows.append(ar)
            self.by_type.setdefault(asset_type, []).append(ar)
            self.by_type_tier[(asset_type, tier)] = ar
            if asset_type not in seen_types:
                seen_types.add(asset_type)
                self.asset_types.append(asset_type)

        print(f"[test] Asset catalog loaded: {len(self.rows)} row(s) from {self.path}")

    def is_loaded(self):
        return bool(self.rows)

    def get_chain_cost(self, asset_type, tier):
        tiers = self.by_type.get(asset_type) or []
        if not tiers:
            return None
        idx = None
        for i, ar in enumerate(tiers):
            if ar.tier == tier:
                idx = i
                break
        if idx is None:
            return None
        return sum(int(ar.cost_val) for ar in tiers[:idx + 1])

    def get_add_income(self, asset_type, tier):
        ar = self.by_type_tier.get((asset_type, tier))
        return int(ar.add_income_val) if ar else 0

    def autocomplete_asset_choices(self, current, limit=25):
        if not self.rows:
            return []
        q = (current or "").strip().lower()
        out = []
        for ar in self.rows:
            label = f"{ar.asset_type} — {ar.tier}"
            if q and q not in label.lower():
                continue
            out.append(app_commands.Choice(name=label[:100], value=f"{ar.asset_type}{SEP}{ar.tier}"[:100]))
            if len(out) >= limit:
                break
        return out

assets = AssetCatalog()

class DB:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        if self.pool:
            return
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5, command_timeout=60)

    async def init(self):
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as con:
            await con.execute("""
            CREATE TABLE IF NOT EXISTS econ_balances (
              guild_id BIGINT NOT NULL,
              character_name TEXT NOT NULL,
              balance_val BIGINT NOT NULL DEFAULT 0,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (guild_id, character_name)
            );""")
            await con.execute("""
            CREATE TABLE IF NOT EXISTS econ_income_claims (
              guild_id BIGINT NOT NULL,
              character_name TEXT NOT NULL,
              last_claim_date DATE NOT NULL,
              PRIMARY KEY (guild_id, character_name)
            );""")
            await con.execute("""
            CREATE TABLE IF NOT EXISTS econ_assets (
              guild_id BIGINT NOT NULL,
              character_name TEXT NOT NULL,
              asset_type TEXT NOT NULL,
              tier TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );""")

            cols = await con.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name='econ_assets';
            """)
            colset = {r["column_name"] for r in cols}
            if "asset_name" not in colset:
                await con.execute("ALTER TABLE econ_assets ADD COLUMN asset_name TEXT;")
            if "user_id" not in colset:
                await con.execute("ALTER TABLE econ_assets ADD COLUMN user_id BIGINT;")
            await con.execute("UPDATE econ_assets SET asset_name = COALESCE(asset_name,'') WHERE asset_name IS NULL;")
            await con.execute("UPDATE econ_assets SET user_id = COALESCE(user_id,0) WHERE user_id IS NULL;")

            exists = await con.fetchval("SELECT 1 FROM pg_constraint WHERE conname='econ_assets_unique_assetname' LIMIT 1;")
            if not exists:
                try:
                    await con.execute("""
                        ALTER TABLE econ_assets
                        ADD CONSTRAINT econ_assets_unique_assetname
                        UNIQUE (guild_id, user_id, character_name, asset_name);
                    """)
                except Exception:
                    pass

            await con.execute("""
            CREATE TABLE IF NOT EXISTS econ_audit_log (
              id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              guild_id BIGINT NOT NULL,
              actor_user_id BIGINT NOT NULL,
              action TEXT NOT NULL,
              details JSONB NOT NULL DEFAULT '{}'::jsonb
            );""")

    async def log(self, guild_id, actor, action, details):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO econ_audit_log (guild_id, actor_user_id, action, details) VALUES ($1,$2,$3,$4::jsonb);",
                int(guild_id), int(actor), str(action), json.dumps(details or {})
            )

    async def search_characters(self, legacy_guild_id, query, limit=25):
        assert self.pool is not None
        like = f"%{(query or '').strip().lower()}%"
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT name, user_id FROM characters
                WHERE guild_id=$1 AND archived=FALSE AND LOWER(name) LIKE $2
                ORDER BY name ASC LIMIT $3;
            """, int(legacy_guild_id), like, int(limit))
        return [(r["name"], int(r["user_id"])) for r in rows]

    async def get_character_owner(self, legacy_guild_id, character_name):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT user_id FROM characters
                WHERE guild_id=$1 AND name=$2 AND archived=FALSE LIMIT 1;
            """, int(legacy_guild_id), str(character_name))
        return int(row["user_id"]) if row else None

    async def get_balance(self, guild_id, character_name):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            v = await con.fetchval(
                "SELECT balance_val FROM econ_balances WHERE guild_id=$1 AND character_name=$2;",
                int(guild_id), str(character_name)
            )
        return int(v) if v is not None else 0

    async def set_balance(self, guild_id, character_name, val):
        assert self.pool is not None
        val = max(0, int(val))
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO econ_balances (guild_id, character_name, balance_val, updated_at)
                VALUES ($1,$2,$3,NOW())
                ON CONFLICT (guild_id, character_name)
                DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW();
            """, int(guild_id), str(character_name), int(val))

    async def get_assets(self, guild_id, character_name):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT asset_name, asset_type, tier, user_id
                FROM econ_assets
                WHERE guild_id=$1 AND character_name=$2
                ORDER BY asset_type, tier, asset_name;
            """, int(guild_id), str(character_name))
        return [dict(r) for r in rows]

    async def add_asset(self, guild_id, character_name, owner_user_id, asset_type, tier, asset_name):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO econ_assets (guild_id, character_name, user_id, asset_name, asset_type, tier, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW());
            """, int(guild_id), str(character_name), int(owner_user_id), str(asset_name), str(asset_type), str(tier))

db = DB(DATABASE_URL)

def is_staff(member):
    if member is None:
        return False
    try:
        if member.guild_permissions.administrator:
            return True
    except Exception:
        pass
    return bool(STAFF_ROLE_IDS) and any(r.id in STAFF_ROLE_IDS for r in getattr(member, "roles", []))

def can_refresh_bank(member):
    if member is None:
        return False
    try:
        if member.guild_permissions.administrator:
            return True
    except Exception:
        pass
    if BANK_REFRESH_ROLE_IDS and any(r.id in BANK_REFRESH_ROLE_IDS for r in getattr(member, "roles", [])):
        return True
    return is_staff(member)

intents = discord.Intents.none()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

GUILD_OBJ = discord.Object(id=int(GUILD_ID))

async def character_autocomplete(interaction, current):
    try:
        res = await db.search_characters(LEGACY_SOURCE_GUILD_ID, current or "", 25)
        return [app_commands.Choice(name=n, value=n) for n, _uid in res]
    except Exception:
        return []

async def asset_autocomplete(interaction, current):
    try:
        if not assets.is_loaded():
            return [app_commands.Choice(name="(Asset sheet missing: NEW Asset Table.xlsx)", value="__MISSING__")]
        return assets.autocomplete_asset_choices(current, 25)
    except Exception:
        return []

async def build_balance_embed(guild, character):
    bal = await db.get_balance(guild.id, character)
    owned = await db.get_assets(guild.id, character)
    lines = []
    for a in owned:
        nm = (a.get("asset_name") or "").strip() or "Unnamed"
        tier = (a.get("tier") or "").strip()
        lines.append(f"{tier} — {nm}")
    e = discord.Embed(
        title=str(character),
        description=f"**Balance:** {format_val(bal)} *(= {bal} Val)*",
        color=discord.Color.teal()
    )
    e.set_footer(text="Bank of Vilyra")
    e.add_field(name="__*Assets*__", value=("\n".join(lines)[:1024] if lines else "None"), inline=False)
    return e

async def get_display_name_no_ping(guild, uid):
    m = guild.get_member(uid)
    if m:
        return m.display_name
    try:
        u = await client.fetch_user(uid)
        return u.name
    except Exception:
        return f"User {uid}"

async def refresh_bank_dashboard(guild):
    if BANK_CHANNEL_ID == 0:
        return
    ch = guild.get_channel(BANK_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return
    assert db.pool is not None
    async with db.pool.acquire() as con:
        rows = await con.fetch("""
            SELECT user_id, name FROM characters
            WHERE guild_id=$1 AND archived=FALSE
            ORDER BY user_id, name;
        """, int(LEGACY_SOURCE_GUILD_ID))
    by_user = {}
    for r in rows:
        by_user.setdefault(int(r["user_id"]), []).append(r["name"])
    users = sorted(by_user.keys())

    msg_ids = list(BANK_MESSAGE_IDS)
    msgs = []
    for mid in msg_ids:
        try:
            msgs.append(await ch.fetch_message(mid))
        except Exception:
            msgs.append(None)
    while len(msgs) < len(users):
        m = await ch.send("Initializing Bank of Vilyra…", allowed_mentions=discord.AllowedMentions.none())
        msgs.append(m)
        msg_ids.append(m.id)

    for i, uid in enumerate(users):
        msg = msgs[i] or await ch.fetch_message(msg_ids[i])
        display = await get_display_name_no_ping(guild, uid)
        lines = []
        for cname in by_user[uid]:
            bal = await db.get_balance(guild.id, cname)
            lines.append(f"**{cname}** — {format_val(bal)}")
        embed = discord.Embed(title=display, description="\n".join(lines)[:4096] if lines else "No characters.", color=discord.Color.blurple())
        embed.set_footer(text="Bank of Vilyra • Public ledger")
        await msg.edit(content="", embed=embed, allowed_mentions=discord.AllowedMentions.none())

def chicago_today():
    return dt.datetime.now(tz=CHICAGO_TZ).date()

async def calc_asset_income(guild_id, character):
    owned = await db.get_assets(guild_id, character)
    total = 0
    for a in owned:
        total += assets.get_add_income(str(a.get("asset_type") or ""), str(a.get("tier") or ""))
    return int(total)

@tree.command(name="balance", description="Show a character’s current money and owned assets.", guild=GUILD_OBJ)
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_balance(interaction, character: str):
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    embed = await build_balance_embed(interaction.guild, character)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="income", description="Claim daily income for one of YOUR characters (once per day, Chicago time).", guild=GUILD_OBJ)
@app_commands.describe(character="Pick one of your characters")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_income(interaction, character: str):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)

    owner = await db.get_character_owner(LEGACY_SOURCE_GUILD_ID, character)
    if owner is None:
        return await interaction.followup.send("Character not found.", ephemeral=True)
    if owner != interaction.user.id:
        return await interaction.followup.send("You can only claim income for characters you own.", ephemeral=True)

    today = chicago_today()
    assert db.pool is not None
    async with db.pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT last_claim_date FROM econ_income_claims WHERE guild_id=$1 AND character_name=$2;",
            int(g.id), str(character)
        )
        if row and row["last_claim_date"] == today:
            return await interaction.followup.send("You already claimed income for this character today (Chicago time).", ephemeral=True)

        asset_income = await calc_asset_income(g.id, character)
        delta = int(BASE_DAILY_INCOME_VAL) + int(asset_income)

        bal = await db.get_balance(g.id, character)
        new_bal = bal + delta

        await con.execute("""
            INSERT INTO econ_balances (guild_id, character_name, balance_val, updated_at)
            VALUES ($1,$2,$3,NOW())
            ON CONFLICT (guild_id, character_name)
            DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW();
        """, int(g.id), str(character), int(new_bal))

        await con.execute("""
            INSERT INTO econ_income_claims (guild_id, character_name, last_claim_date)
            VALUES ($1,$2,$3)
            ON CONFLICT (guild_id, character_name)
            DO UPDATE SET last_claim_date=EXCLUDED.last_claim_date;
        """, int(g.id), str(character), today)

    await db.log(g.id, interaction.user.id, "income", {
        "character": character,
        "delta_val": delta,
        "base_val": BASE_DAILY_INCOME_VAL,
        "asset_income_val": asset_income
    })
    await interaction.followup.send(
        f"Income claimed for **{character}**: +{format_val(delta)}. New balance: {format_val(new_bal)}.\n"
        f"Asset income included today: {format_val(asset_income)}.",
        ephemeral=True
    )
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_adjust", description="Staff-only. Add or subtract money from a character (non-negative enforced).", guild=GUILD_OBJ)
@app_commands.describe(character="Pick a character", delta_val="Positive or negative Val", reason="Optional reason")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_adjust(interaction, character: str, delta_val: int, reason: str=None):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)

    bal = await db.get_balance(g.id, character)
    new_bal = bal + int(delta_val)
    if new_bal < 0:
        return await interaction.followup.send(f"Insufficient funds. Current: {format_val(bal)} (= {bal} Val).", ephemeral=True)

    await db.set_balance(g.id, character, new_bal)
    await db.log(g.id, interaction.user.id, "econ_adjust", {"character": character, "delta_val": int(delta_val), "reason": reason or ""})
    await interaction.followup.send(f"Adjusted **{character}** by {int(delta_val)} Val. New balance: {format_val(new_bal)}.", ephemeral=True)
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_set_balance", description="Staff-only. Set a character’s balance to an exact amount (non-negative).", guild=GUILD_OBJ)
@app_commands.describe(character="Pick a character", new_balance_val="New balance in Val (>=0)", reason="Optional reason")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_set_balance(interaction, character: str, new_balance_val: int, reason: str=None):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)
    if int(new_balance_val) < 0:
        return await interaction.followup.send("Balance cannot be negative.", ephemeral=True)

    await db.set_balance(g.id, character, int(new_balance_val))
    await db.log(g.id, interaction.user.id, "econ_set_balance", {"character": character, "new_balance_val": int(new_balance_val), "reason": reason or ""})
    await interaction.followup.send(f"Set **{character}** balance to {format_val(int(new_balance_val))}.", ephemeral=True)
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="purchase_new", description="Staff-only. Purchase a new asset for a character.", guild=GUILD_OBJ)
@app_commands.describe(character="Pick a character", asset="Pick an Asset Type — Tier (from spreadsheet)", asset_name="Unique asset name (executor entered)")
@app_commands.autocomplete(character=character_autocomplete, asset=asset_autocomplete)
async def cmd_purchase_new(interaction, character: str, asset: str, asset_name: str):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)

    if asset == "__MISSING__" or not assets.is_loaded():
        return await interaction.followup.send(
            f"Asset sheet missing. Ensure **{ASSET_XLSX_FILENAME}** is included in the Railway deploy at **/app/{ASSET_XLSX_FILENAME}**.",
            ephemeral=True
        )

    asset_name = (asset_name or "").strip()
    if not asset_name:
        return await interaction.followup.send("Asset name is required.", ephemeral=True)
    if SEP not in (asset or ""):
        return await interaction.followup.send("Invalid asset selection.", ephemeral=True)

    asset_type, tier = [x.strip() for x in asset.split(SEP, 1)]

    owner_user_id = await db.get_character_owner(LEGACY_SOURCE_GUILD_ID, character)
    if owner_user_id is None:
        return await interaction.followup.send("Character not found in characters table.", ephemeral=True)

    existing = await db.get_assets(g.id, character)
    for a in existing:
        if int(a.get("user_id") or 0) == int(owner_user_id) and (a.get("asset_name") or "").strip().lower() == asset_name.lower():
            return await interaction.followup.send("That asset name already exists for this character. Use a unique name.", ephemeral=True)

    total_cost = assets.get_chain_cost(asset_type, tier)
    if total_cost is None:
        return await interaction.followup.send("That Asset Type/Tier is not recognized in the spreadsheet.", ephemeral=True)

    bal = await db.get_balance(g.id, character)
    if bal < total_cost:
        short = total_cost - bal
        return await interaction.followup.send(
            f"**Insufficient funds** for **{character}**.\n"
            f"Available: {format_val(bal)} (= {bal} Val)\n"
            f"Total cost: {format_val(total_cost)} (= {total_cost} Val)\n"
            f"Short by: {format_val(short)} (= {short} Val)",
            ephemeral=True
        )

    await db.set_balance(g.id, character, bal - total_cost)
    await db.add_asset(g.id, character, owner_user_id, asset_type, tier, asset_name)
    await db.log(g.id, interaction.user.id, "purchase_new", {
        "character": character,
        "owner_user_id": owner_user_id,
        "asset_type": asset_type,
        "tier": tier,
        "asset_name": asset_name,
        "total_cost_val": total_cost
    })

    add_income = assets.get_add_income(asset_type, tier)
    await interaction.followup.send(
        f"Purchased **{asset_type} — {tier}** for **{character}** as **{asset_name}**.\n"
        f"Cost: {format_val(total_cost)}. New balance: {format_val(bal-total_cost)}.\n"
        f"Adds to daily income: {format_val(add_income)} (from spreadsheet).",
        ephemeral=True
    )
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_refresh_bank", description="Staff: manually refresh the Bank of Vilyra dashboard.", guild=GUILD_OBJ)
async def cmd_refresh_bank(interaction):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not can_refresh_bank(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)
    await refresh_bank_dashboard(g)
    await interaction.followup.send("Bank dashboard refreshed.", ephemeral=True)

@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.", guild=GUILD_OBJ)
async def cmd_econ_commands(interaction):
    await interaction.response.defer(ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)
    text = (
        "**Player Commands**\n"
        "/balance — Show a character’s current money and owned assets.\n"
        "/income — Claim daily income for one of YOUR characters (once per day, Chicago time). Daily income = base + sum(Add to Income) for owned assets.\n\n"
        "**Staff Commands**\n"
        "/econ_adjust — Add or subtract money from a character (never allows negative balances).\n"
        "/econ_set_balance — Set a character’s balance to an exact Val amount (non-negative).\n"
        "/purchase_new — Purchase a new asset for a character. Asset choices come from NEW Asset Table.xlsx. Enter a unique asset name.\n"
        "/econ_refresh_bank — Force-refresh the Bank of Vilyra dashboard messages.\n"
        "/econ_commands — Show this command list.\n"
    )
    await interaction.followup.send(text, ephemeral=True)

@client.event
async def on_ready():
    print(f"[test] Starting {APP_VERSION}…")
    await db.init()

    assets.load()

    guild_obj = discord.Object(id=int(GUILD_ID))

    # --- HARD cleanup of duplicates (requested) ---
    # Delete ALL 'purchase_new' commands registered globally and in this guild, then re-sync guild-only.
    try:
        # Global commands
        try:
            global_cmds = await tree.fetch_commands()  # global
            for c in global_cmds:
                if getattr(c, "name", "") == "purchase_new":
                    try:
                        await c.delete()
                        print("[test] Deleted GLOBAL /purchase_new (cleanup).")
                    except Exception as e:
                        print(f"[warn] Failed to delete GLOBAL /purchase_new via cmd.delete(): {e}")
        except Exception as e:
            print(f"[warn] Global command fetch skipped: {e}")

        # Guild commands
        try:
            guild_cmds = await tree.fetch_commands(guild=guild_obj)
            for c in guild_cmds:
                if getattr(c, "name", "") == "purchase_new":
                    try:
                        await c.delete()
                        print("[test] Deleted GUILD /purchase_new (cleanup).")
                    except Exception as e:
                        print(f"[warn] Failed to delete GUILD /purchase_new via cmd.delete(): {e}")
        except Exception as e:
            print(f"[warn] Guild command fetch skipped: {e}")

    except Exception as e:
        print(f"[warn] Duplicate cleanup step failed: {e}")

    # Sync guild-only command set
    try:
        synced = await tree.sync(guild=guild_obj)
        print(f"[test] Synced {len(synced)} guild command(s).")
    except Exception as e:
        print(f"[warn] Guild sync failed: {e}")

    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID}; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")

    try:
        g = client.get_guild(int(GUILD_ID))
        if g:
            await refresh_bank_dashboard(g)
    except Exception as e:
        print(f"[warn] Initial bank refresh failed: {e}")

def main():
    client.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
