import os, json, datetime as dt
from dataclasses import dataclass

import discord
from discord import app_commands
import asyncpg

APP_VERSION = "EconBot_v46"

# --- Timezone handling (Railway-safe) ---
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    CHICAGO_TZ = ZoneInfo("America/Chicago")
except Exception:
    CHICAGO_TZ = dt.timezone(dt.timedelta(hours=-6))  # fixed UTC-6 fallback (no DST auto-adjust)

# --- Spreadsheet loader (authoritative asset source) ---
# Per Continuity: the ONLY source of truth for assets is "NEW Asset Table.xlsx"
# We do NOT introduce env vars. We look for the file in common deploy paths.
try:
    import openpyxl  # available in this environment
except Exception:
    openpyxl = None

ASSET_XLSX_FILENAME = "NEW Asset Table.xlsx"
SEP = "|||"

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

GUILD_ID = _int("GUILD_ID")
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

class AssetCatalog:
    """
    Loads asset definitions from the authoritative spreadsheet:
        NEW Asset Table.xlsx
    """
    def __init__(self):
        self.path = None
        self.rows = []               # list[AssetRow] in sheet order
        self.by_type = {}            # asset_type -> list[AssetRow] in sheet order
        self.by_type_tier = {}       # (asset_type, tier) -> AssetRow

    def _find_xlsx(self):
        # common paths: current working dir, alongside this file, /app
        candidates = []
        try:
            candidates.append(os.path.join(os.getcwd(), ASSET_XLSX_FILENAME))
        except Exception:
            pass
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            candidates.append(os.path.join(here, ASSET_XLSX_FILENAME))
        except Exception:
            pass
        candidates.append(os.path.join("/app", ASSET_XLSX_FILENAME))

        for p in candidates:
            if p and os.path.exists(p):
                return p
        return None

    def load(self):
        self.rows = []
        self.by_type = {}
        self.by_type_tier = {}
        self.path = self._find_xlsx()

        if openpyxl is None:
            print("[warn] openpyxl not available; asset dropdown will be empty.")
            return

        if not self.path:
            print(f"[warn] Asset spreadsheet not found: {ASSET_XLSX_FILENAME} (asset dropdown will be empty)")
            return

        try:
            wb = openpyxl.load_workbook(self.path, data_only=True)
            ws = wb.active
        except Exception as e:
            print(f"[warn] Failed to load asset spreadsheet ({self.path}): {e}")
            return

        # Expect header row with these columns:
        # Asset Type | Tier | Cost to Acquire | Add to Income
        # We match headers case-insensitively.
        header = []
        for cell in ws[1]:
            header.append(str(cell.value).strip() if cell.value is not None else "")

        def idx_of(name):
            name = name.strip().lower()
            for i, h in enumerate(header):
                if h.strip().lower() == name:
                    return i
            return None

        i_type = idx_of("asset type")
        i_tier = idx_of("tier")
        i_cost = idx_of("cost to acquire")
        i_add  = idx_of("add to income")

        if None in (i_type, i_tier, i_cost, i_add):
            print("[warn] Asset spreadsheet headers not recognized. Expected: Asset Type, Tier, Cost to Acquire, Add to Income")
            print(f"[warn] Found headers: {header}")
            return

        for row in ws.iter_rows(min_row=2, values_only=True):
            asset_type = (row[i_type] if i_type is not None else "") or ""
            tier = (row[i_tier] if i_tier is not None else "") or ""
            cost = row[i_cost] if i_cost is not None else 0
            add  = row[i_add]  if i_add  is not None else 0

            asset_type = str(asset_type).strip()
            tier = str(tier).strip()

            if not asset_type or not tier:
                continue

            try:
                cost_val = int(cost)
            except Exception:
                try:
                    cost_val = int(float(cost))
                except Exception:
                    cost_val = 0

            try:
                add_income_val = int(add)
            except Exception:
                try:
                    add_income_val = int(float(add))
                except Exception:
                    add_income_val = 0

            ar = AssetRow(asset_type=asset_type, tier=tier, cost_val=cost_val, add_income_val=add_income_val)
            self.rows.append(ar)
            self.by_type.setdefault(asset_type, []).append(ar)
            self.by_type_tier[(asset_type, tier)] = ar

        print(f"[test] Asset catalog loaded: {len(self.rows)} row(s) from {self.path}")

    def is_loaded(self):
        return bool(self.rows)

    def list_asset_types(self):
        # Unique asset types in sheet order of first appearance.
        seen = set()
        out = []
        for r in self.rows:
            if r.asset_type in seen:
                continue
            seen.add(r.asset_type)
            out.append(r.asset_type)
        return out

    def list_tiers_for_type(self, asset_type):
        tiers = self.by_type.get(asset_type) or []
        return [r.tier for r in tiers]

    def autocomplete_choices(self, current, limit=25):
        q = (current or "").strip().lower()
        out = []
        if not self.rows:
            return []
        # Provide "Asset Type — Tier" choices in sheet order.
        for ar in self.rows:
            label = f"{ar.asset_type} — {ar.tier}"
            if q and q not in label.lower():
                continue
            value = f"{ar.asset_type}{SEP}{ar.tier}"
            out.append(app_commands.Choice(name=label[:100], value=value[:100]))
            if len(out) >= limit:
                break
        return out

    def get_chain_cost(self, asset_type, tier):
        # Purchase rule: selecting Tier N costs sum of all tiers up to that tier, in sheet order for that asset_type.
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
        return sum(int(ar.cost_val) for ar in tiers[:idx+1])

    def get_add_income(self, asset_type, tier):
        ar = self.by_type_tier.get((asset_type, tier))
        return int(ar.add_income_val) if ar else 0

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

            # Runtime migration checks (existing v38 behavior)
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
        # Character dropdown must pull from Postgres table: characters
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

async def character_autocomplete(interaction, current):
    try:
        res = await db.search_characters(LEGACY_SOURCE_GUILD_ID, current or "", 25)
        return [app_commands.Choice(name=n, value=n) for n,_uid in res]
    except Exception:
        return []

async def asset_type_autocomplete(interaction, current):
    try:
        if not assets.is_loaded():
            return [app_commands.Choice(name="(Asset sheet missing: NEW Asset Table.xlsx)", value="__MISSING__")]
        q = (current or "").strip().lower()
        out = []
        for at in assets.list_asset_types():
            if q and q not in at.lower():
                continue
            out.append(app_commands.Choice(name=at[:100], value=at[:100]))
            if len(out) >= 25:
                break
        return out
    except Exception:
        return []

async def tier_autocomplete(interaction, current):
    try:
        if not assets.is_loaded():
            return [app_commands.Choice(name="(Asset sheet missing: NEW Asset Table.xlsx)", value="__MISSING__")]

        # Read the selected asset_type from the interaction namespace (Discord supplies filled options here).
        selected_type = None
        try:
            ns = getattr(interaction, "namespace", None)
            selected_type = getattr(ns, "asset_type", None) if ns else None
        except Exception:
            selected_type = None

        if not selected_type or str(selected_type).strip() in ("__MISSING__", ""):
            return [app_commands.Choice(name="(Select an Asset Type first)", value="__SELECT_TYPE__")]

        selected_type = str(selected_type).strip()
        q = (current or "").strip().lower()
        out = []
        for tr in assets.list_tiers_for_type(selected_type):
            if q and q not in tr.lower():
                continue
            out.append(app_commands.Choice(name=tr[:100], value=tr[:100]))
            if len(out) >= 25:
                break
        if not out:
            return [app_commands.Choice(name="(No tiers found for that type)", value="__NO_TIERS__")]
        return out
    except Exception:
        return []

async def build_balance_embed(guild, character):
    bal = await db.get_balance(guild.id, character)
    assets_owned = await db.get_assets(guild.id, character)
    lines = []
    for a in assets_owned:
        nm = (a.get("asset_name") or "").strip() or "Unnamed"
        tier = (a.get("tier") or "").strip()
        at = (a.get("asset_type") or "").strip()
        lines.append(f"{tier} — {nm}")
    e = discord.Embed(title=str(character), description=f"**Balance:** {format_val(bal)} *(= {bal} Val)*", color=discord.Color.teal())
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
    # Income adds are pulled from the asset spreadsheet (authoritative).
    for a in owned:
        total += assets.get_add_income(str(a.get("asset_type") or ""), str(a.get("tier") or ""))
    return int(total)

# --- Commands ---

@tree.command(name="balance", description="Show a character’s current money and owned assets.")
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_balance(interaction, character: str):
    await interaction.response.defer(ephemeral=True)
    if not interaction.guild:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    embed = await build_balance_embed(interaction.guild, character)
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="income", description="Claim daily income for one of YOUR characters (once per day, Chicago time).")
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
        row = await con.fetchrow("SELECT last_claim_date FROM econ_income_claims WHERE guild_id=$1 AND character_name=$2;", int(g.id), str(character))
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

    await db.log(g.id, interaction.user.id, "income", {"character": character, "delta_val": delta, "base_val": BASE_DAILY_INCOME_VAL, "asset_income_val": asset_income})
    await interaction.followup.send(f"Income claimed for **{character}**: +{format_val(delta)}. New balance: {format_val(new_bal)}.", ephemeral=True)
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_adjust", description="Staff-only. Add or subtract money from a character (non-negative enforced).")
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

@tree.command(name="econ_set_balance", description="Staff-only. Set a character’s balance to an exact amount (non-negative).")
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

@tree.command(name="purchase_new", description="Staff-only. Purchase a new asset for a character (tiered cost sum).")
@app_commands.describe(character="Pick a character", asset_type="Pick an Asset Type", tier="Pick a Tier (depends on Asset Type)", asset_name="Name this asset (unique per character)")
@app_commands.autocomplete(character=character_autocomplete, asset_type=asset_type_autocomplete, tier=tier_autocomplete)
async def cmd_purchase_new(interaction, character: str, asset_type: str, tier: str, asset_name: str):
    await interaction.response.defer(ephemeral=True)
    g = interaction.guild
    if not g:
        return await interaction.followup.send("Use this in a server.", ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)

    if asset_type in ("__MISSING__", "") or tier in ("__MISSING__", ""):
        return await interaction.followup.send(
            f"Asset sheet not found. Place **{ASSET_XLSX_FILENAME}** in the bot directory (Railway /app) and redeploy.",
            ephemeral=True
        )

    if asset_type in ("__SELECT_TYPE__", "__NO_TIERS__") or tier in ("__SELECT_TYPE__", "__NO_TIERS__", "__SELECT_TYPE__"):
        return await interaction.followup.send("Select a valid Asset Type and Tier.", ephemeral=True)

    asset_name = (asset_name or "").strip()
    if not asset_name:
        return await interaction.followup.send("Asset name is required.", ephemeral=True)
    owner_user_id = await db.get_character_owner(LEGACY_SOURCE_GUILD_ID, character)
    if owner_user_id is None:
        return await interaction.followup.send("Character not found in characters table.", ephemeral=True)

    existing = await db.get_assets(g.id, character)
    for a in existing:
        if int(a.get("user_id") or 0) == int(owner_user_id) and (a.get("asset_name") or "").strip().lower() == asset_name.lower():
            return await interaction.followup.send("That asset name already exists for this character. Use a unique name.", ephemeral=True)

    total_cost = assets.get_chain_cost(asset_type, tier)
    if total_cost is None:
        return await interaction.followup.send(
            "That Asset Type/Tier is not recognized from the asset spreadsheet. (Did the sheet change?)",
            ephemeral=True
        )

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
    await db.log(g.id, interaction.user.id, "purchase_new", {"character": character, "owner_user_id": owner_user_id, "asset_type": asset_type, "tier": tier, "asset_name": asset_name, "total_cost_val": total_cost})
    await interaction.followup.send(
        f"Purchased **{asset_type} — {tier}** for **{character}** as **{asset_name}**.\n"
        f"Cost: {format_val(total_cost)}. New balance: {format_val(bal-total_cost)}.",
        ephemeral=True
    )
    try:
        await refresh_bank_dashboard(g)
    except Exception:
        pass

@tree.command(name="econ_refresh_bank", description="Staff: manually refresh the Bank of Vilyra dashboard.")
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

@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.")
async def cmd_econ_commands(interaction):
    await interaction.response.defer(ephemeral=True)
    mem = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(mem):
        return await interaction.followup.send("You do not have permission.", ephemeral=True)
    text = (
        "**Player Commands**\n"
        "/balance — Show a character’s current money and owned assets.\n"
        "/income — Claim daily income for one of YOUR characters (once per day, Chicago time). Adds base income plus income from owned assets.\n\n"
        "**Staff Commands**\n"
        "/econ_adjust — Add or subtract money from a character (never allows negative balances).\n"
        "/econ_set_balance — Set a character’s balance to an exact Val amount (non-negative).\n"
        "/purchase_new — Purchase a new asset for a character. Select an Asset Type — Tier from the spreadsheet, then provide a unique asset name.\n"
        "/econ_refresh_bank — Force-refresh the Bank of Vilyra dashboard messages.\n"
        "/econ_commands — Show this command list.\n"
    )
    await interaction.followup.send(text, ephemeral=True)

@client.event
async def on_ready():
    print(f"[test] Starting {APP_VERSION}…")
    await db.init()

    # Load asset spreadsheet once at startup (authoritative).
    assets.load()

    if not GUILD_ID:
        print("[warn] GUILD_ID env var is missing; slash commands may register globally and appear slowly.")
        try:
            await tree.sync()
        except Exception as e:
            print(f"[warn] Global sync failed: {e}")
        print(f"[test] Logged in as {client.user} (commands guild: GLOBAL; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")
        return

    # IMPORTANT: Our commands are defined as GLOBAL in-code, but we ONLY sync them to the guild.
    # To do that reliably, we must copy the global command definitions into the guild command list first.
    guild_obj = discord.Object(id=int(GUILD_ID))
    try:
        tree.copy_global_to(guild=guild_obj)
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
