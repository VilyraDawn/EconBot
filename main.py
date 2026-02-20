import os, json, datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple

import discord
from discord import app_commands
import asyncpg
import re

APP_VERSION = "EconBot_v61"

# --- Timezone handling (Railway-safe) ---
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    CHICAGO_TZ = ZoneInfo("America/Chicago")
except Exception:
    CHICAGO_TZ = dt.timezone(dt.timedelta(hours=-6))  # fixed UTC-6 fallback (no DST auto-adjust)

def _get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default

def _req(name: str) -> str:
    v = _get(name)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _int(name: str, default: Optional[int] = None) -> Optional[int]:
    v = _get(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _int_list(name: str) -> List[int]:
    """Parse comma-separated integer IDs from env var (syntax-safe)."""
    raw = _get(name, "") or ""
    raw = str(raw).strip()
    if not raw:
        return []
    parts = raw.split(",")
    out: List[int] = []
    seen: set[int] = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # Strip non-digits (allows role mention formats like <@&123>)
        digits = re.sub(r"[^0-9]", "", p)
        if not digits:
            continue
        try:
            val = int(digits)
        except Exception:
            continue
        if val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out

DISCORD_TOKEN = _req("DISCORD_TOKEN")
DATABASE_URL = _req("DATABASE_URL")

# Required: prevents accidental global registration/drift
GUILD_ID = _int("GUILD_ID")
if not GUILD_ID:
    raise RuntimeError("Missing required env var: GUILD_ID")

# Characters source (already in your DB)
LEGACY_SOURCE_GUILD_ID = _int("LEGACY_SOURCE_GUILD_ID") or 0

BANK_CHANNEL_ID = _int("BANK_CHANNEL_ID") or 0
ECON_LOG_CHANNEL_ID = _int("ECON_LOG_CHANNEL_ID") or 0
BANK_MESSAGE_IDS = _int_list("BANK_MESSAGE_IDS")

STAFF_ROLE_IDS = set(_int_list("STAFF_ROLE_IDS"))
# Fallback default STAFF role IDs (used only if STAFF_ROLE_IDS env is missing/empty).
# Provided by you (role IDs):
STAFF_ROLE_IDS_DEFAULT = {1473523681132019824, 1473523738891784232}
if not STAFF_ROLE_IDS:
    STAFF_ROLE_IDS = set(STAFF_ROLE_IDS_DEFAULT)

BANK_REFRESH_ROLE_IDS = set(_int_list("BANK_REFRESH_ROLE_IDS"))

# Currency
DENOMS = [("NOVIR", 10000), ("ORIN", 1000), ("ELSH", 100), ("ARCE", 10), ("CINTH", 1)]
BASE_DAILY_INCOME_VAL = 10

def format_val(total_val: int) -> str:
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

# ---------------------------------------------------------------------------
# Asset Catalog: Postgres-backed (requested)
#
# The spreadsheet does NOT exist in Railway, so we seed asset_catalog from an
# embedded snapshot derived from NEW Asset Table.xlsx (SHA256 below).
#
# Updating the spreadsheet later can be done by shipping a new code snapshot
# (still spreadsheet-authoritative) OR by adding a staff import command
# (not included here unless you ask).
# ---------------------------------------------------------------------------

EMBEDDED_ASSET_XLSX_SHA256 = "9f61a9afbb9f777dfed30bc8ee206279be9cdaf50b01b458458de363a2343299"
EMBEDDED_ASSET_ROWS_JSON = r"""[{"asset_type": "Guild Trade Workshop", "tier": "(1) Guild Apprentice", "cost_val": 300, "add_income_val": 50}, {"asset_type": "Guild Trade Workshop", "tier": "(2) Guild Journeyman", "cost_val": 600, "add_income_val": 100}, {"asset_type": "Guild Trade Workshop", "tier": "(3) Leased Workshop", "cost_val": 1200, "add_income_val": 150}, {"asset_type": "Guild Trade Workshop", "tier": "(4) Small Workshop", "cost_val": 2000, "add_income_val": 200}, {"asset_type": "Guild Trade Workshop", "tier": "(5) Large Workshop", "cost_val": 3000, "add_income_val": 250}, {"asset_type": "Market Stall", "tier": "(1) Consignment Arrangement", "cost_val": 300, "add_income_val": 50}, {"asset_type": "Market Stall", "tier": "(2) Small Alley Stand", "cost_val": 600, "add_income_val": 100}, {"asset_type": "Market Stall", "tier": "(3) Market Stall", "cost_val": 1200, "add_income_val": 150}, {"asset_type": "Market Stall", "tier": "(4) Small Shop", "cost_val": 2000, "add_income_val": 200}, {"asset_type": "Market Stall", "tier": "(5) Large Shop", "cost_val": 3000, "add_income_val": 250}, {"asset_type": "Farm/Ranch", "tier": "(1) Subsistence Surplus", "cost_val": 300, "add_income_val": 50}, {"asset_type": "Farm/Ranch", "tier": "(2) Leased Fields", "cost_val": 600, "add_income_val": 100}, {"asset_type": "Farm/Ranch", "tier": "(3) Owned Acre", "cost_val": 1200, "add_income_val": 150}, {"asset_type": "Farm/Ranch", "tier": "(4) Small Fields and Barn", "cost_val": 2000, "add_income_val": 200}, {"asset_type": "Farm/Ranch", "tier": "(5) Large Fields and Barn", "cost_val": 3000, "add_income_val": 250}, {"asset_type": "Tavern/Inn", "tier": "(1) One-Room Flophouse", "cost_val": 300, "add_income_val": 50}, {"asset_type": "Tavern/Inn", "tier": "(2) Leased Establishment", "cost_val": 600, "add_income_val": 100}, {"asset_type": "Tavern/Inn", "tier": "(3) Small Tavern", "cost_val": 1200, "add_income_val": 150}, {"asset_type": "Tavern/Inn", "tier": "(4) Large Tavern", "cost_val": 2000, "add_income_val": 200}, {"asset_type": "Tavern/Inn", "tier": "(5) Large Tavern and Inn", "cost_val": 3000, "add_income_val": 250}, {"asset_type": "Warehouse/Trade House", "tier": "(1) Small Storage Shed", "cost_val": 300, "add_income_val": 50}, {"asset_type": "Warehouse/Trade House", "tier": "(2) Large Storage Shed", "cost_val": 600, "add_income_val": 100}, {"asset_type": "Warehouse/Trade House", "tier": "(3) Small Trading Post", "cost_val": 1200, "add_income_val": 150}, {"asset_type": "Warehouse/Trade House", "tier": "(4) Large Trading Post", "cost_val": 2000, "add_income_val": 200}, {"asset_type": "Warehouse/Trade House", "tier": "(5) Large Warehouse and Trading Post", "cost_val": 3000, "add_income_val": 250}, {"asset_type": "House", "tier": "(1) Shack", "cost_val": 600, "add_income_val": 0}, {"asset_type": "House", "tier": "(2) Hut", "cost_val": 1200, "add_income_val": 0}, {"asset_type": "House", "tier": "(3) House", "cost_val": 2000, "add_income_val": 0}, {"asset_type": "House", "tier": "(4) Lodge", "cost_val": 3000, "add_income_val": 0}, {"asset_type": "House", "tier": "(5) Mansion", "cost_val": 5000, "add_income_val": 0}, {"asset_type": "Village", "tier": "(1) Chartered Assembly", "cost_val": 1200, "add_income_val": 100}, {"asset_type": "Village", "tier": "(2) Hamlet", "cost_val": 2400, "add_income_val": 200}, {"asset_type": "Village", "tier": "(3) Village", "cost_val": 4800, "add_income_val": 300}, {"asset_type": "Village", "tier": "(4) Town", "cost_val": 9600, "add_income_val": 400}, {"asset_type": "Village", "tier": "(5) Small City", "cost_val": 15000, "add_income_val": 500}, {"asset_type": "Weapons", "tier": "(1) Hit +1 / Dmg +1d4", "cost_val": 300, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(2) Hit +1 / Dmg +1d6", "cost_val": 600, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(3) Hit +2 / Dmg +1d8", "cost_val": 1200, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(4) Hit +2 / Dmg +1d10", "cost_val": 2400, "add_income_val": 0}, {"asset_type": "Weapons", "tier": "(5) Hit +2 / Dmg +1d12", "cost_val": 4800, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(1) AC +1", "cost_val": 300, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(2) AC +2", "cost_val": 600, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(3) AC +2 / Adv Magic Atk", "cost_val": 1200, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(4) AC +2 / Adv Magic and Melee Atk", "cost_val": 2400, "add_income_val": 0}, {"asset_type": "Armor", "tier": "(5) AC +3 / Adv Magic and Melee Atk", "cost_val": 4800, "add_income_val": 0}]"""

@dataclass(frozen=True)
class AssetRow:
    asset_type: str
    tier: str
    tier_order: int
    cost_val: int
    add_income_val: int

class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self.pool:
            return
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5, command_timeout=60)

    async def init(self):
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as con:
            # Existing economy tables
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
            # Schema drift guards (existing)
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

            # NEW: Asset catalog table (requested)
            await con.execute("""
            CREATE TABLE IF NOT EXISTS asset_catalog (
              asset_type TEXT NOT NULL,
              tier TEXT NOT NULL,
              tier_order INT NOT NULL,
              cost_val INT NOT NULL DEFAULT 0,
              add_income_val INT NOT NULL DEFAULT 0,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              PRIMARY KEY (asset_type, tier)
            );""")
            await con.execute("CREATE INDEX IF NOT EXISTS asset_catalog_type_order_idx ON asset_catalog(asset_type, tier_order);")

            # Seed from embedded snapshot if empty
            count = await con.fetchval("SELECT COUNT(*) FROM asset_catalog;")
            if int(count or 0) == 0:
                try:
                    raw = json.loads(EMBEDDED_ASSET_ROWS_JSON)
                except Exception as e:
                    raise RuntimeError(f"Embedded asset catalog JSON is invalid: {e}")

                # preserve per-type order as in the spreadsheet snapshot
                order_map = {}
                seed_rows = []
                for d in raw:
                    at = str(d.get("asset_type","") or "").strip()
                    tr = str(d.get("tier","") or "").strip()
                    if not at or not tr:
                        continue
                    order_map.setdefault(at, 0)
                    order_map[at] += 1
                    seed_rows.append((
                        at,
                        tr,
                        int(order_map[at]),
                        int(d.get("cost_val", 0) or 0),
                        int(d.get("add_income_val", 0) or 0),
                    ))
                if seed_rows:
                    await con.executemany("""
                        INSERT INTO asset_catalog(asset_type, tier, tier_order, cost_val, add_income_val, updated_at)
                        VALUES ($1,$2,$3,$4,$5,NOW())
                        ON CONFLICT (asset_type, tier) DO UPDATE
                        SET tier_order=EXCLUDED.tier_order,
                            cost_val=EXCLUDED.cost_val,
                            add_income_val=EXCLUDED.add_income_val,
                            updated_at=NOW();
                    """, seed_rows)
                print(f"[test] Seeded asset_catalog with {len(seed_rows)} row(s) from embedded spreadsheet snapshot (SHA256={EMBEDDED_ASSET_XLSX_SHA256}).")
            else:
                print(f"[test] asset_catalog already populated: {count} row(s).")

    async def log(self, guild_id: int, actor: int, action: str, details: dict):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO econ_audit_log (guild_id, actor_user_id, action, details) VALUES ($1,$2,$3,$4::jsonb);",
                int(guild_id), int(actor), str(action), json.dumps(details or {})
            )

    # --- Characters (existing table, character-based economy) ---
    async def search_characters(self, legacy_guild_id: int, query: str, limit: int = 25) -> List[Tuple[str, int]]:
        assert self.pool is not None
        like = f"%{(query or '').strip().lower()}%"
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT name, user_id FROM characters
                WHERE guild_id=$1 AND archived=FALSE AND LOWER(name) LIKE $2
                ORDER BY name ASC LIMIT $3;
            """, int(legacy_guild_id), like, int(limit))
        return [(r["name"], int(r["user_id"])) for r in rows]

    async def get_character_owner(self, legacy_guild_id: int, character_name: str) -> Optional[int]:
        assert self.pool is not None
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT user_id FROM characters
                WHERE guild_id=$1 AND name=$2 AND archived=FALSE LIMIT 1;
            """, int(legacy_guild_id), str(character_name))
        return int(row["user_id"]) if row else None

    # --- Balances ---
    async def get_balance(self, guild_id: int, character_name: str) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as con:
            v = await con.fetchval(
                "SELECT balance_val FROM econ_balances WHERE guild_id=$1 AND character_name=$2;",
                int(guild_id), str(character_name)
            )
        return int(v) if v is not None else 0

    async def set_balance(self, guild_id: int, character_name: str, val: int):
        assert self.pool is not None
        val = max(0, int(val))
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO econ_balances (guild_id, character_name, balance_val, updated_at)
                VALUES ($1,$2,$3,NOW())
                ON CONFLICT (guild_id, character_name)
                DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW();
            """, int(guild_id), str(character_name), int(val))

    # --- Assets owned by characters ---
    async def get_assets(self, guild_id: int, character_name: str) -> List[dict]:
        assert self.pool is not None
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT asset_name, asset_type, tier, user_id
                FROM econ_assets
                WHERE guild_id=$1 AND character_name=$2
                ORDER BY asset_type, tier, asset_name;
            """, int(guild_id), str(character_name))
        return [dict(r) for r in rows]

    async def add_asset(self, guild_id: int, character_name: str, owner_user_id: int, asset_type: str, tier: str, asset_name: str):
        assert self.pool is not None
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO econ_assets (guild_id, character_name, user_id, asset_name, asset_type, tier, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW());
            """, int(guild_id), str(character_name), int(owner_user_id), str(asset_name), str(asset_type), str(tier))

    # --- Asset catalog (NEW table) ---
    async def asset_types(self, query: str = "", limit: int = 25) -> List[str]:
        assert self.pool is not None
        q = (query or "").strip()
        async with self.pool.acquire() as con:
            if q:
                rows = await con.fetch("""
                    SELECT DISTINCT asset_type
                    FROM asset_catalog
                    WHERE asset_type ILIKE $1
                    ORDER BY asset_type ASC
                    LIMIT $2;
                """, f"%{q}%", int(limit))
            else:
                rows = await con.fetch("""
                    SELECT DISTINCT asset_type
                    FROM asset_catalog
                    ORDER BY asset_type ASC
                    LIMIT $1;
                """, int(limit))
        return [r["asset_type"] for r in rows]

    async def tiers_for_type(self, asset_type: str, query: str = "", limit: int = 25) -> List[str]:
        assert self.pool is not None
        at = (asset_type or "").strip()
        q = (query or "").strip()
        if not at:
            return []
        async with self.pool.acquire() as con:
            if q:
                rows = await con.fetch("""
                    SELECT tier
                    FROM asset_catalog
                    WHERE asset_type=$1 AND tier ILIKE $2
                    ORDER BY tier_order ASC
                    LIMIT $3;
                """, at, f"%{q}%", int(limit))
            else:
                rows = await con.fetch("""
                    SELECT tier
                    FROM asset_catalog
                    WHERE asset_type=$1
                    ORDER BY tier_order ASC
                    LIMIT $2;
                """, at, int(limit))
        return [r["tier"] for r in rows]

    async def chain_cost_for(self, asset_type: str, tier: str) -> Optional[int]:
        assert self.pool is not None
        at = (asset_type or "").strip()
        tr = (tier or "").strip()
        if not at or not tr:
            return None
        async with self.pool.acquire() as con:
            target_order = await con.fetchval(
                "SELECT tier_order FROM asset_catalog WHERE asset_type=$1 AND tier=$2;",
                at, tr
            )
            if target_order is None:
                return None
            total = await con.fetchval(
                "SELECT COALESCE(SUM(cost_val),0) FROM asset_catalog WHERE asset_type=$1 AND tier_order <= $2;",
                at, int(target_order)
            )
        return int(total or 0)

    async def add_income_for(self, asset_type: str, tier: str) -> int:
        assert self.pool is not None
        at = (asset_type or "").strip()
        tr = (tier or "").strip()
        async with self.pool.acquire() as con:
            v = await con.fetchval(
                "SELECT COALESCE(add_income_val,0) FROM asset_catalog WHERE asset_type=$1 AND tier=$2;",
                at, tr
            )
        return int(v or 0)

    async def calc_asset_income(self, guild_id: int, character_name: str) -> int:
        """Sum(Add to Income) across currently owned assets, by joining econ_assets to asset_catalog."""
        assert self.pool is not None
        async with self.pool.acquire() as con:
            v = await con.fetchval("""
                SELECT COALESCE(SUM(ac.add_income_val),0)
                FROM econ_assets ea
                JOIN asset_catalog ac
                  ON ac.asset_type = ea.asset_type AND ac.tier = ea.tier
                WHERE ea.guild_id=$1 AND ea.character_name=$2;
            """, int(guild_id), str(character_name))
        return int(v or 0)

db = DB(DATABASE_URL)

async def _ensure_member(interaction: discord.Interaction) -> discord.Member | None:
    """Ensure we have a discord.Member (with roles/permissions) for a guild interaction.

    With Intents.none(), interaction.user is usually a Member, but can sometimes be a User-like object
    without roles. We fetch the Member from the API as a fallback (does not require members intent).
    """
    g = interaction.guild
    if not g:
        return None
    u = interaction.user
    if isinstance(u, discord.Member):
        return u
    # Try cache
    try:
        m = g.get_member(u.id)
        if m:
            return m
    except Exception:
        pass
    # Fetch from API
    try:
        return await g.fetch_member(u.id)
    except Exception:
        return None

def _member_role_ids(member: discord.Member | None) -> list[int]:
    try:
        return [int(r.id) for r in (member.roles or [])]
    except Exception:
        return []

def is_staff_member(member: discord.Member | None) -> bool:
    """Pure staff predicate once we have a Member."""
    if member is None:
        return False

    try:
        gp = getattr(member, "guild_permissions", None)
        if gp and gp.administrator:
            return True
    except Exception:
        pass
    # Role-based allow if configured
    try:
        if STAFF_ROLE_IDS:
            return any(getattr(r, "id", 0) in STAFF_ROLE_IDS for r in (member.roles or []))
    except Exception:
        pass
    # Fallback if STAFF_ROLE_IDS not configured
    try:
        gp = getattr(member, "guild_permissions", None)
        if not STAFF_ROLE_IDS and gp:
            return bool(gp.manage_guild or gp.manage_messages)
    except Exception:
        pass
    return False

async def staff_check(interaction: discord.Interaction) -> tuple[bool, str]:
    """Returns (allowed, debug_string)."""
    member = await _ensure_member(interaction)
    allowed = is_staff_member(member)
    role_ids = _member_role_ids(member)
    gp = getattr(member, "guild_permissions", None) if member else None
    dbg = (
        f"Your user_id: {interaction.user.id}\n"
        f"Detected role IDs: {role_ids}\n"
        f"Configured STAFF_ROLE_IDS (effective): {sorted(list(STAFF_ROLE_IDS))}\n"
        f"Configured STAFF_ROLE_IDS_DEFAULT: {sorted(list(STAFF_ROLE_IDS_DEFAULT))}\n"
        f"Admin: {bool(getattr(gp,'administrator', False))}\n"
        f"Manage Guild: {bool(getattr(gp,'manage_guild', False))}\n"
        f"Manage Messages: {bool(getattr(gp,'manage_messages', False))}"
    )
    return allowed, dbg


def can_refresh_bank(member: Optional[discord.Member]) -> bool:
    if member is None:
        return False

    # User-ID allowlist fallback
    try:
        if int(getattr(member, "id", 0)) in STAFF_USER_IDS_ALLOWLIST:
            return True
    except Exception:
        pass
    try:
        if member.guild_permissions.administrator:
            return True
    except Exception:
        pass
    if BANK_REFRESH_ROLE_IDS and any(r.id in BANK_REFRESH_ROLE_IDS for r in getattr(member, "roles", [])):
        return True
    return is_staff_member(member)

intents = discord.Intents.none()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
GUILD_OBJ = discord.Object(id=int(GUILD_ID))

# ---------------------- Autocomplete callbacks ----------------------

async def character_autocomplete(interaction: discord.Interaction, current: str):
    try:
        res = await db.search_characters(LEGACY_SOURCE_GUILD_ID, current or "", 25)
        return [app_commands.Choice(name=n, value=n) for n, _uid in res]
    except Exception:
        return []

async def asset_type_autocomplete(interaction: discord.Interaction, current: str):
    try:
        types = await db.asset_types(current or "", 25)
        return [app_commands.Choice(name=t[:100], value=t[:100]) for t in types]
    except Exception:
        return []

async def tier_autocomplete(interaction: discord.Interaction, current: str):
    try:
        ns = getattr(interaction, "namespace", None)
        at = ""
        if ns is not None:
            at = getattr(ns, "asset_type", "") or ""
        at = (at or "").strip()
        if not at:
            return [app_commands.Choice(name="(select asset_type first)", value="")]
        tiers = await db.tiers_for_type(at, current or "", 25)
        return [app_commands.Choice(name=t[:100], value=t[:100]) for t in tiers]
    except Exception:
        return []

# ---------------------- UI helpers ----------------------

async def build_balance_embed(guild: discord.Guild, character: str) -> discord.Embed:
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

async def get_display_name_no_ping(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    if m:
        return m.display_name
    try:
        u = await client.fetch_user(uid)
        return u.name
    except Exception:
        return f"User {uid}"

async def refresh_bank_dashboard(guild: discord.Guild):
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

def chicago_today() -> dt.date:
    return dt.datetime.now(tz=CHICAGO_TZ).date()

# ---------------------- Commands ----------------------

@tree.command(name="balance", description="Show a character’s current money and owned assets.", guild=GUILD_OBJ)
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_balance(interaction: discord.Interaction, character: str):
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    embed = await build_balance_embed(interaction.guild, character)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="income", description="Claim daily income for one of YOUR characters (once per day, Chicago time).", guild=GUILD_OBJ)
@app_commands.describe(character="Pick one of your characters")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_income(interaction: discord.Interaction, character: str):
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

        asset_income = await db.calc_asset_income(g.id, character)
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
async def cmd_econ_adjust(interaction: discord.Interaction, character: str, delta_val: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = await _ensure_member(interaction)
    allowed, dbg = await staff_check(interaction)
    if not allowed:
        return await interaction.followup.send("You do not have permission to run this staff command.\n\nIf you expect access, verify STAFF_ROLE_IDS matches your role IDs, or grant Admin/Manage Server/Manage Messages.\n\n--- Debug ---\n" + dbg, ephemeral=True)

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
async def cmd_econ_set_balance(interaction: discord.Interaction, character: str, new_balance_val: int, reason: Optional[str] = None):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = await _ensure_member(interaction)
    allowed, dbg = await staff_check(interaction)
    if not allowed:
        return await interaction.followup.send("You do not have permission to run this staff command.\n\nIf you expect access, verify STAFF_ROLE_IDS matches your role IDs, or grant Admin/Manage Server/Manage Messages.\n\n--- Debug ---\n" + dbg, ephemeral=True)
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
@app_commands.describe(
    character="Pick a character",
    asset_type="Pick an Asset Type (from asset_catalog)",
    tier="Pick a Tier for the chosen Asset Type",
    asset_name="Unique asset name (executor entered)"
)
@app_commands.autocomplete(character=character_autocomplete, asset_type=asset_type_autocomplete, tier=tier_autocomplete)
async def cmd_purchase_new(interaction: discord.Interaction, character: str, asset_type: str, tier: str, asset_name: str):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = await _ensure_member(interaction)
    allowed, dbg = await staff_check(interaction)
    if not allowed:
        return await interaction.followup.send("You do not have permission to run this staff command.\n\nIf you expect access, verify STAFF_ROLE_IDS matches your role IDs, or grant Admin/Manage Server/Manage Messages.\n\n--- Debug ---\n" + dbg, ephemeral=True)

    asset_type = (asset_type or "").strip()
    tier = (tier or "").strip()
    asset_name = (asset_name or "").strip()

    if not asset_type:
        return await interaction.followup.send("asset_type is required.", ephemeral=True)
    if not tier:
        return await interaction.followup.send("tier is required.", ephemeral=True)
    if not asset_name:
        return await interaction.followup.send("asset_name is required.", ephemeral=True)

    owner_user_id = await db.get_character_owner(LEGACY_SOURCE_GUILD_ID, character)
    if owner_user_id is None:
        return await interaction.followup.send("Character not found in characters table.", ephemeral=True)

    existing = await db.get_assets(g.id, character)
    for a in existing:
        if int(a.get("user_id") or 0) == int(owner_user_id) and (a.get("asset_name") or "").strip().lower() == asset_name.lower():
            return await interaction.followup.send("That asset name already exists for this character. Use a unique name.", ephemeral=True)

    total_cost = await db.chain_cost_for(asset_type, tier)
    if total_cost is None:
        return await interaction.followup.send("That Asset Type/Tier is not recognized in asset_catalog.", ephemeral=True)

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

    add_income = await db.add_income_for(asset_type, tier)

    await db.log(g.id, interaction.user.id, "purchase_new", {
        "character": character,
        "owner_user_id": owner_user_id,
        "asset_type": asset_type,
        "tier": tier,
        "asset_name": asset_name,
        "total_cost_val": total_cost,
        "add_income_val": add_income
    })

    await interaction.followup.send(
        f"Purchased **{asset_type} — {tier}** for **{character}** as **{asset_name}**.\n"
        f"Cost: {format_val(total_cost)}. New balance: {format_val(bal-total_cost)}.\n"
        f"Adds to daily income: {format_val(add_income)} (from asset_catalog).",
        ephemeral=True
    )
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_refresh_bank", description="Staff: manually refresh the Bank of Vilyra dashboard.", guild=GUILD_OBJ)
async def cmd_refresh_bank(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = await _ensure_member(interaction)
    if not can_refresh_bank(mem):
        return await interaction.followup.send("You do not have permission to run this staff command.", ephemeral=True)
    await refresh_bank_dashboard(g)
    await interaction.followup.send("Bank dashboard refreshed.", ephemeral=True)

@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.", guild=GUILD_OBJ)
async def cmd_econ_commands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    mem = await _ensure_member(interaction)
    allowed, dbg = await staff_check(interaction)
    if not allowed:
        return await interaction.followup.send("You do not have permission to run this staff command.\n\nIf you expect access, verify STAFF_ROLE_IDS matches your role IDs, or grant Admin/Manage Server/Manage Messages.\n\n--- Debug ---\n" + dbg, ephemeral=True)
    text = (
        "**Player Commands**\n"
        "/balance — Show a character’s current money and owned assets.\n"
        "/income — Claim daily income for one of YOUR characters (once per day, Chicago time). Daily income = base + sum(Add to Income) for owned assets.\n\n"
        "**Staff Commands**\n"
        "/econ_adjust — Add or subtract money from a character (never allows negative balances).\n"
        "/econ_set_balance — Set a character’s balance to an exact Val amount (non-negative).\n"
        "/purchase_new — Purchase a new asset for a character. Asset Type/Tier dropdowns come from Postgres asset_catalog. Enter a unique asset name.\n"
        "/econ_refresh_bank — Force-refresh the Bank of Vilyra dashboard messages.\n"
        "/econ_commands — Show this command list.\n"
    )
    await interaction.followup.send(text, ephemeral=True)

@client.event
async def on_ready():
    print(f"[test] Starting {APP_VERSION}…")
    print(f"[debug] raw STAFF_ROLE_IDS env: {os.getenv('STAFF_ROLE_IDS', '')!r}")
    print(f"[debug] STAFF_ROLE_IDS (effective): {sorted(list(STAFF_ROLE_IDS))}")
    print(f"[debug] STAFF_ROLE_IDS_DEFAULT: {sorted(list(STAFF_ROLE_IDS_DEFAULT))}")
    await db.init()

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
                        print(f"[warn] Failed to delete GLOBAL /purchase_new: {e}")
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
                        print(f"[warn] Failed to delete GUILD /purchase_new: {e}")
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
