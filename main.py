import datetime as dt
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import asyncpg
import discord
from discord import app_commands
from zoneinfo import ZoneInfo

CHICAGO_TZ = ZoneInfo("America/Chicago")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _parse_int_list(csv: Optional[str]) -> List[int]:
    if not csv:
        return []
    out: List[int] = []
    for part in csv.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


DISCORD_TOKEN = _env("DISCORD_TOKEN") or _env("BOT_TOKEN")  # allow either name
DATABASE_URL = _env("DATABASE_URL")

# Commands run in this guild (recommended). If omitted, commands are global and may take longer to appear.
COMMANDS_GUILD_ID = int(_env("COMMANDS_GUILD_ID", "") or 0) or None

# Where to read characters from (Legacy bot database). This is the guild_id stored in the legacy `characters` table.
LEGACY_SOURCE_GUILD_ID = int(_env("LEGACY_SOURCE_GUILD_ID", "") or 0) or None

BANK_CHANNEL_ID = int(_env("BANK_CHANNEL_ID", "") or 0) or None
ECON_LOG_CHANNEL_ID = int(_env("ECON_LOG_CHANNEL_ID", "") or 0) or None

# Staff role IDs are a comma-separated env var.
STAFF_ROLE_IDS = set(_parse_int_list(_env("STAFF_ROLE_IDS", "")))

# Base daily income (10 Val == 10 Cinth == 1 Arce)
BASE_DAILY_INCOME_VAL = 10

if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN (or BOT_TOKEN) env var")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var")
if not LEGACY_SOURCE_GUILD_ID:
    raise RuntimeError("Missing LEGACY_SOURCE_GUILD_ID env var")
if not BANK_CHANNEL_ID:
    raise RuntimeError("Missing BANK_CHANNEL_ID env var")
if not ECON_LOG_CHANNEL_ID:
    raise RuntimeError("Missing ECON_LOG_CHANNEL_ID env var")

GUILD_OBJ = discord.Object(id=COMMANDS_GUILD_ID) if COMMANDS_GUILD_ID else None


# -------------------------
# Currency helpers
# -------------------------

DENOMS: List[Tuple[str, int]] = [
    ("Novir", 10_000),
    ("Orin", 1_000),
    ("Elsh", 100),
    ("Arce", 10),
    ("Cinth", 1),
]


def format_val(val: int) -> str:
    val = int(val)
    if val < 0:
        return f"-{format_val(abs(val))}"
    if val == 0:
        return "0 Cinth"
    parts: List[str] = []
    remaining = val
    for name, size in DENOMS:
        if remaining >= size:
            qty, remaining = divmod(remaining, size)
            parts.append(f"{qty} {name}")
    if remaining > 0:
        parts.append(f"{remaining} Cinth")
    return ", ".join(parts)


# -------------------------
# Asset definitions (from NEW Asset Table.xlsx)
# Only these rows exist. Do not add others without updating the sheet.
# -------------------------

ASSET_DEFS: List[Dict[str, object]] = [{"asset_type": "Guild Trade Workshop", "tier": "(1) Guild Apprentice", "cost_val": 300, "income_val": 50}, {"asset_type": "Guild Trade Workshop", "tier": "(2) Guild Journeyman", "cost_val": 600, "income_val": 100}, {"asset_type": "Guild Trade Workshop", "tier": "(3) Leased Workshop", "cost_val": 1200, "income_val": 150}, {"asset_type": "Guild Trade Workshop", "tier": "(4) Small Workshop", "cost_val": 2000, "income_val": 200}, {"asset_type": "Guild Trade Workshop", "tier": "(5) Large Workshop", "cost_val": 3000, "income_val": 250}, {"asset_type": "Market Stall", "tier": "(1) Consignment Arrangement", "cost_val": 300, "income_val": 50}, {"asset_type": "Market Stall", "tier": "(2) Small Alley Stand", "cost_val": 600, "income_val": 100}, {"asset_type": "Market Stall", "tier": "(3) Market Stall", "cost_val": 1200, "income_val": 150}, {"asset_type": "Market Stall", "tier": "(4) Small Shop", "cost_val": 2000, "income_val": 200}, {"asset_type": "Market Stall", "tier": "(5) Large Shop", "cost_val": 3000, "income_val": 250}, {"asset_type": "Farm/Ranch", "tier": "(1) Subsistence Surplus", "cost_val": 300, "income_val": 50}, {"asset_type": "Farm/Ranch", "tier": "(2) Leased Fields", "cost_val": 600, "income_val": 100}, {"asset_type": "Farm/Ranch", "tier": "(3) Owned Acre", "cost_val": 1200, "income_val": 150}, {"asset_type": "Farm/Ranch", "tier": "(4) Small Fields and Barn", "cost_val": 2000, "income_val": 200}, {"asset_type": "Farm/Ranch", "tier": "(5) Large Fields and Barn", "cost_val": 3000, "income_val": 250}, {"asset_type": "Tavern/Inn", "tier": "(1) One-Room Flophouse", "cost_val": 300, "income_val": 50}, {"asset_type": "Tavern/Inn", "tier": "(2) Leased Establishment", "cost_val": 600, "income_val": 100}, {"asset_type": "Tavern/Inn", "tier": "(3) Small Tavern", "cost_val": 1200, "income_val": 150}, {"asset_type": "Tavern/Inn", "tier": "(4) Large Tavern", "cost_val": 2000, "income_val": 200}, {"asset_type": "Tavern/Inn", "tier": "(5) Large Tavern and Inn", "cost_val": 3000, "income_val": 250}, {"asset_type": "Warehouse/Trade House", "tier": "(1) Small Storage Shed", "cost_val": 300, "income_val": 50}, {"asset_type": "Warehouse/Trade House", "tier": "(2) Large Storage Shed", "cost_val": 600, "income_val": 100}, {"asset_type": "Warehouse/Trade House", "tier": "(3) Small Trading Post", "cost_val": 1200, "income_val": 150}, {"asset_type": "Warehouse/Trade House", "tier": "(4) Large Trading Post", "cost_val": 2000, "income_val": 200}, {"asset_type": "Warehouse/Trade House", "tier": "(5) Large Warehouse and Trading Post", "cost_val": 3000, "income_val": 250}, {"asset_type": "House", "tier": "(1) Shack", "cost_val": 600, "income_val": 0}, {"asset_type": "House", "tier": "(2) Hut", "cost_val": 1200, "income_val": 0}, {"asset_type": "House", "tier": "(3) House", "cost_val": 2000, "income_val": 0}, {"asset_type": "House", "tier": "(4) Lodge", "cost_val": 3000, "income_val": 0}, {"asset_type": "House", "tier": "(5) Mansion", "cost_val": 5000, "income_val": 0}, {"asset_type": "Village", "tier": "(1) Chartered Assembly", "cost_val": 1200, "income_val": 100}, {"asset_type": "Village", "tier": "(2) Hamlet", "cost_val": 2400, "income_val": 200}, {"asset_type": "Village", "tier": "(3) Village", "cost_val": 4800, "income_val": 300}, {"asset_type": "Village", "tier": "(4) Town", "cost_val": 9600, "income_val": 400}, {"asset_type": "Village", "tier": "(5) Small City", "cost_val": 15000, "income_val": 500}, {"asset_type": "Weapons", "tier": "(1) Hit +1 / Dmg +1d4", "cost_val": 300, "income_val": 0}, {"asset_type": "Weapons", "tier": "(2) Hit +1 / Dmg +1d6", "cost_val": 600, "income_val": 0}, {"asset_type": "Weapons", "tier": "(3) Hit +2 / Dmg +1d8", "cost_val": 1200, "income_val": 0}, {"asset_type": "Weapons", "tier": "(4) Hit +2 / Dmg +1d10", "cost_val": 2400, "income_val": 0}, {"asset_type": "Weapons", "tier": "(5) Hit +2 / Dmg +1d12", "cost_val": 4800, "income_val": 0}, {"asset_type": "Armor", "tier": "(1) AC +1", "cost_val": 300, "income_val": 0}, {"asset_type": "Armor", "tier": "(2) AC +2", "cost_val": 600, "income_val": 0}, {"asset_type": "Armor", "tier": "(3) AC +2 / Adv Magic Atk", "cost_val": 1200, "income_val": 0}, {"asset_type": "Armor", "tier": "(4) AC +2 / Adv Magic and Melee Atk", "cost_val": 2400, "income_val": 0}, {"asset_type": "Armor", "tier": "(5) AC +3 / Adv Magic and Melee Atk", "cost_val": 4800, "income_val": 0}]

_TIER_NUM_RE = re.compile(r"^\((\d+)\)\s*")


def tier_number(tier_name: str) -> int:
    m = _TIER_NUM_RE.match(tier_name.strip())
    if not m:
        return 10_000
    return int(m.group(1))


_ASSET_TYPE_TO_TIERS: Dict[str, List[Dict[str, object]]] = {}
_ASSET_KEY_TO_DEF: Dict[Tuple[str, str], Dict[str, object]] = {}

for row in ASSET_DEFS:
    at = str(row["asset_type"]).strip()
    tn = str(row["tier"]).strip()
    _ASSET_KEY_TO_DEF[(at, tn)] = row
    _ASSET_TYPE_TO_TIERS.setdefault(at, []).append(row)

for at, rows in _ASSET_TYPE_TO_TIERS.items():
    rows.sort(key=lambda r: tier_number(str(r["tier"])))


def asset_display(asset_type: str, tier: str) -> str:
    return f"{asset_type} — {tier}"


def all_asset_display_strings() -> List[str]:
    out: List[str] = []
    for at, rows in sorted(_ASSET_TYPE_TO_TIERS.items(), key=lambda x: x[0].lower()):
        for r in rows:
            out.append(asset_display(at, str(r["tier"])))
    return out


def parse_asset_display(s: str) -> Optional[Tuple[str, str]]:
    if "—" in s:
        parts = [p.strip() for p in s.split("—", 1)]
    elif "-" in s:
        parts = [p.strip() for p in s.split("-", 1)]
    else:
        return None
    if len(parts) != 2:
        return None
    asset_type, tier = parts[0], parts[1]
    if (asset_type, tier) in _ASSET_KEY_TO_DEF:
        return (asset_type, tier)
    return None


def total_cost_for_new_purchase(asset_type: str, target_tier: str) -> int:
    rows = _ASSET_TYPE_TO_TIERS.get(asset_type, [])
    tgt_n = tier_number(target_tier)
    total = 0
    for r in rows:
        n = tier_number(str(r["tier"]))
        if n <= tgt_n:
            total += int(r["cost_val"])
    return total


# -------------------------
# Database
# -------------------------

@dataclass
class Character:
    guild_id: int
    user_id: int
    name: str
    archived: bool


class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5, command_timeout=30)
        async with self.pool.acquire() as con:
            await con.execute("CREATE SCHEMA IF NOT EXISTS economy")
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.balances (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    balance_val BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, character_name)
                );
                """
            )
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.income_claims (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    last_claim_date DATE NOT NULL,
                    PRIMARY KEY (guild_id, character_name)
                );
                """
            )
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.assets (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    owner_user_id BIGINT NOT NULL,
                    asset_name TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, character_name, asset_name)
                );
                """
            )
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.asset_defs (
                    asset_type TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    cost_val BIGINT NOT NULL,
                    income_val BIGINT NOT NULL,
                    PRIMARY KEY (asset_type, tier)
                );
                """
            )
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.bank_messages (
                    guild_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    PRIMARY KEY (guild_id, user_id)
                );
                """
            )

            # Seed defs (idempotent)
            await con.executemany(
                """
                INSERT INTO economy.asset_defs (asset_type, tier, cost_val, income_val)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (asset_type, tier) DO UPDATE
                SET cost_val=EXCLUDED.cost_val, income_val=EXCLUDED.income_val
                """,
                [
                    (a["asset_type"], a["tier"], int(a["cost_val"]), int(a["income_val"]))
                    for a in ASSET_DEFS
                ],
            )

    async def fetch_character(self, name: str) -> Optional[Character]:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                """
                SELECT guild_id, user_id, name, archived
                FROM public.characters
                WHERE guild_id=$1 AND name=$2
                LIMIT 1
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                name,
            )
            if not row:
                return None
            return Character(int(row["guild_id"]), int(row["user_id"]), str(row["name"]), bool(row["archived"]))

    async def search_characters(self, query: str, limit: int = 25) -> List[str]:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT name
                FROM public.characters
                WHERE guild_id=$1 AND archived=false AND name ILIKE $2
                ORDER BY name ASC
                LIMIT $3
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                f"%{query}%",
                limit,
            )
            return [str(r["name"]) for r in rows]

    async def get_balance(self, character_name: str) -> int:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT balance_val FROM economy.balances WHERE guild_id=$1 AND character_name=$2",
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
            )
            return int(row["balance_val"]) if row else 0

    async def set_balance(self, character_name: str, new_balance: int) -> None:
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO economy.balances (guild_id, character_name, balance_val)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, character_name) DO UPDATE
                SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
                int(new_balance),
            )

    async def add_balance(self, character_name: str, delta: int) -> int:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                """
                INSERT INTO economy.balances (guild_id, character_name, balance_val)
                VALUES ($1, $2, GREATEST(0, $3))
                ON CONFLICT (guild_id, character_name) DO UPDATE
                SET balance_val = GREATEST(0, economy.balances.balance_val + $3),
                    updated_at = NOW()
                RETURNING balance_val
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
                int(delta),
            )
            return int(row["balance_val"])

    async def get_assets(self, character_name: str) -> List[Dict[str, str]]:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT asset_name, asset_type, tier
                FROM economy.assets
                WHERE guild_id=$1 AND character_name=$2
                ORDER BY asset_type ASC, tier ASC, asset_name ASC
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
            )
            return [
                {"asset_name": str(r["asset_name"]), "asset_type": str(r["asset_type"]), "tier": str(r["tier"])}
                for r in rows
            ]

    async def insert_asset(self, character_name: str, owner_user_id: int, asset_name: str, asset_type: str, tier: str) -> None:
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO economy.assets (guild_id, character_name, owner_user_id, asset_name, asset_type, tier)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (guild_id, character_name, asset_name) DO UPDATE
                SET asset_type=EXCLUDED.asset_type,
                    tier=EXCLUDED.tier,
                    owner_user_id=EXCLUDED.owner_user_id,
                    updated_at=NOW()
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
                int(owner_user_id),
                asset_name,
                asset_type,
                tier,
            )

    async def sum_asset_income(self, character_name: str) -> int:
        assert self.pool
        assets = await self.get_assets(character_name)
        total = 0
        for a in assets:
            d = _ASSET_KEY_TO_DEF.get((a["asset_type"], a["tier"]))
            if d:
                total += int(d["income_val"])
        return total

    async def last_income_date(self, character_name: str) -> Optional[dt.date]:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT last_claim_date FROM economy.income_claims WHERE guild_id=$1 AND character_name=$2",
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
            )
            return row["last_claim_date"] if row else None

    async def set_income_date(self, character_name: str, d: dt.date) -> None:
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO economy.income_claims (guild_id, character_name, last_claim_date)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, character_name) DO UPDATE
                SET last_claim_date=EXCLUDED.last_claim_date
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                character_name,
                d,
            )

    async def list_users_with_characters(self) -> List[int]:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT DISTINCT user_id
                FROM public.characters
                WHERE guild_id=$1 AND archived=false
                ORDER BY user_id ASC
                """,
                int(LEGACY_SOURCE_GUILD_ID),
            )
            return [int(r["user_id"]) for r in rows]

    async def list_characters_for_user(self, user_id: int) -> List[str]:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT name
                FROM public.characters
                WHERE guild_id=$1 AND user_id=$2 AND archived=false
                ORDER BY name ASC
                """,
                int(LEGACY_SOURCE_GUILD_ID),
                int(user_id),
            )
            return [str(r["name"]) for r in rows]

    async def get_bank_message_id(self, guild_id: int, user_id: int) -> Optional[int]:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT message_id FROM economy.bank_messages WHERE guild_id=$1 AND user_id=$2",
                int(guild_id),
                int(user_id),
            )
            return int(row["message_id"]) if row else None

    async def set_bank_message_id(self, guild_id: int, user_id: int, message_id: int) -> None:
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO economy.bank_messages (guild_id, user_id, message_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (guild_id, user_id) DO UPDATE
                SET message_id=EXCLUDED.message_id
                """,
                int(guild_id),
                int(user_id),
                int(message_id),
            )


# -------------------------
# Discord
# -------------------------

intents = discord.Intents.none()
intents.guilds = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
db = DB(DATABASE_URL)


def is_staff(member: Optional[discord.Member]) -> bool:
    if not member:
        return False
    if member.guild_permissions.administrator:
        return True
    if not STAFF_ROLE_IDS:
        return False
    return any(r.id in STAFF_ROLE_IDS for r in member.roles)


async def log_econ(guild: discord.Guild, text: str) -> None:
    ch = guild.get_channel(ECON_LOG_CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        await ch.send(text, allowed_mentions=discord.AllowedMentions.none())


async def make_balance_embed(character_name: str) -> discord.Embed:
    bal = await db.get_balance(character_name)
    assets = await db.get_assets(character_name)
    e = discord.Embed(title=f"{character_name} — Balance", description=f"**Money:** {format_val(bal)}")
    if assets:
        lines = [f"{a['asset_name']} — {a['asset_type']} / {a['tier']}" for a in assets]
        e.add_field(name="_*Assets*_", value="\n".join(lines), inline=False)
    else:
        e.add_field(name="_*Assets*_", value="(none)", inline=False)
    return e


async def refresh_bank_for_user(guild: discord.Guild, user_id: int) -> None:
    ch = guild.get_channel(BANK_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return

    display_name = f"User {user_id}"
    try:
        member = guild.get_member(user_id) or await guild.fetch_member(user_id)
        if member:
            display_name = member.display_name
    except Exception:
        pass

    char_names = await db.list_characters_for_user(user_id)
    if not char_names:
        return

    e = discord.Embed(title=f"Bank of Vilyra — {display_name}")
    blocks: List[str] = []
    for cn in char_names:
        bal = await db.get_balance(cn)
        assets = await db.get_assets(cn)
        assets_lines = [f"{a['asset_name']} — {a['asset_type']} / {a['tier']}" for a in assets]
        assets_text = "(none)" if not assets_lines else "\n".join(assets_lines)
        blocks.append(f"**{cn}**\nMoney: {format_val(bal)}\n_*Assets*_\n{assets_text}")
    e.description = "\n\n".join(blocks)

    msg_id = await db.get_bank_message_id(guild.id, user_id)
    if msg_id:
        try:
            msg = await ch.fetch_message(msg_id)
            await msg.edit(embed=e, allowed_mentions=discord.AllowedMentions.none())
            return
        except Exception:
            pass

    msg = await ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())
    await db.set_bank_message_id(guild.id, user_id, msg.id)


async def refresh_bank_for_character(guild: discord.Guild, character_name: str) -> None:
    c = await db.fetch_character(character_name)
    if not c:
        return
    await refresh_bank_for_user(guild, c.user_id)


async def refresh_bank_all(guild: discord.Guild) -> None:
    for uid in await db.list_users_with_characters():
        await refresh_bank_for_user(guild, uid)


# -------------------------
# Autocomplete helpers
# -------------------------

async def character_autocomplete(_: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    try:
        names = await db.search_characters(current or "")
        return [app_commands.Choice(name=n, value=n) for n in names[:25]]
    except Exception:
        return []


async def asset_autocomplete(_: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    current_l = (current or "").lower()
    all_disp = all_asset_display_strings()
    filtered = [s for s in all_disp if current_l in s.lower()] if current_l else all_disp
    return [app_commands.Choice(name=s, value=s) for s in filtered[:25]]


# -------------------------
# Commands
# -------------------------

@tree.command(name="balance", description="Show a character’s current money and owned assets.", guild=GUILD_OBJ)
@app_commands.describe(character_name="Select a character")
@app_commands.autocomplete(character_name=character_autocomplete)
async def cmd_balance(interaction: discord.Interaction, character_name: str):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    c = await db.fetch_character(character_name)
    if not c or c.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return
    e = await make_balance_embed(character_name)
    await interaction.followup.send(embed=e, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="income", description="Claim daily income for one of YOUR characters (once per day, Chicago time).", guild=GUILD_OBJ)
@app_commands.describe(character_name="Select one of your characters")
@app_commands.autocomplete(character_name=character_autocomplete)
async def cmd_income(interaction: discord.Interaction, character_name: str):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return

    c = await db.fetch_character(character_name)
    if not c or c.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return
    if interaction.user.id != c.user_id:
        await interaction.followup.send("You can only claim income for characters you own.", ephemeral=True)
        return

    today = dt.datetime.now(CHICAGO_TZ).date()
    last = await db.last_income_date(character_name)
    if last == today:
        await interaction.followup.send("Income already claimed for this character today (Chicago time).", ephemeral=True)
        return

    asset_income = await db.sum_asset_income(character_name)
    total = BASE_DAILY_INCOME_VAL + asset_income
    new_bal = await db.add_balance(character_name, total)
    await db.set_income_date(character_name, today)

    await refresh_bank_for_character(guild, character_name)

    await interaction.followup.send(
        f"Income claimed for **{character_name}**. +{format_val(total)} (base {format_val(BASE_DAILY_INCOME_VAL)} + assets {format_val(asset_income)}). New balance: {format_val(new_bal)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    await log_econ(guild, f"[income] {interaction.user.id} claimed income for {character_name}: +{total} Val")


@tree.command(name="econ_adjust", description="Staff: add or subtract money from a character (no negative balances).", guild=GUILD_OBJ)
@app_commands.describe(character_name="Select a character", delta_val="Positive to add, negative to subtract (in Val)")
@app_commands.autocomplete(character_name=character_autocomplete)
async def cmd_econ_adjust(interaction: discord.Interaction, character_name: str, delta_val: int):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(member):
        await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
        return

    c = await db.fetch_character(character_name)
    if not c or c.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    before = await db.get_balance(character_name)
    after = before + int(delta_val)
    if after < 0:
        await interaction.followup.send(
            f"Insufficient funds. Available: {format_val(before)}. Requested change: {format_val(delta_val)}.",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return

    new_bal = await db.add_balance(character_name, int(delta_val))
    await refresh_bank_for_character(guild, character_name)
    await interaction.followup.send(
        f"Adjusted **{character_name}** by {format_val(delta_val)}. New balance: {format_val(new_bal)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    await log_econ(guild, f"[econ_adjust] {interaction.user.id} adjusted {character_name} by {delta_val} Val")


@tree.command(name="econ_set_balance", description="Staff: set a character balance to an exact amount (non-negative).", guild=GUILD_OBJ)
@app_commands.describe(character_name="Select a character", new_balance_val="New balance in Val (>= 0)")
@app_commands.autocomplete(character_name=character_autocomplete)
async def cmd_econ_set_balance(interaction: discord.Interaction, character_name: str, new_balance_val: int):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(member):
        await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
        return
    if new_balance_val < 0:
        await interaction.followup.send("Balance cannot be negative.", ephemeral=True)
        return

    c = await db.fetch_character(character_name)
    if not c or c.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    await db.set_balance(character_name, int(new_balance_val))
    await refresh_bank_for_character(guild, character_name)
    await interaction.followup.send(
        f"Set **{character_name}** balance to {format_val(int(new_balance_val))}.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    await log_econ(guild, f"[econ_set_balance] {interaction.user.id} set {character_name} to {new_balance_val} Val")


@tree.command(name="econ_refresh_bank", description="Staff: manually refresh the Bank of Vilyra dashboard.", guild=GUILD_OBJ)
async def cmd_refresh_bank(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(member):
        await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
        return

    await refresh_bank_all(guild)
    await interaction.followup.send("Bank of Vilyra refreshed.", ephemeral=True)


@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.", guild=GUILD_OBJ)
async def cmd_econ_commands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(member):
        await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
        return

    lines = [
        "**Player Commands**",
        "/balance — Show a character’s current money and owned assets. Anyone can use this; it matches the public bank dashboard.",
        "/income — Claim daily income for one of YOUR characters (once per day, Chicago time). Adds base income plus income from owned assets.",
        "",
        "**Staff Commands**",
        "/econ_adjust — Add or subtract money from a character. The bot will not allow balances to go negative.",
        "/econ_set_balance — Set a character’s balance to an exact amount (non-negative). Useful for corrections.",
        "/purchase_new — NEW purchase: choose a character, choose an Asset Type + Tier from the official sheet, name the asset. Cost is Tier1..Target summed; must have sufficient funds.",
        "/econ_refresh_bank — Manually rebuild / resync the Bank of Vilyra dashboard.",
        "/econ_commands — Shows this command list (kept updated as we add features).",
    ]
    await interaction.followup.send("\n".join(lines), ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="purchase_new", description="Staff: purchase a new asset for a character (Asset Type + Tier).", guild=GUILD_OBJ)
@app_commands.describe(
    character_name="Select a character",
    asset="Select an Asset Type + Tier (from NEW Asset Table)",
    asset_name="Name this asset (unique per character)",
)
@app_commands.autocomplete(character_name=character_autocomplete, asset=asset_autocomplete)
async def cmd_purchase_new(interaction: discord.Interaction, character_name: str, asset: str, asset_name: str):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if not guild:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not is_staff(member):
        await interaction.followup.send("You do not have permission to use this command.", ephemeral=True)
        return

    c = await db.fetch_character(character_name)
    if not c or c.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    asset_name = (asset_name or "").strip()
    if not asset_name:
        await interaction.followup.send("Asset name cannot be empty.", ephemeral=True)
        return

    parsed = parse_asset_display(asset)
    if not parsed:
        await interaction.followup.send("Invalid asset selection. Please choose from the dropdown.", ephemeral=True)
        return
    asset_type, tier = parsed

    existing = await db.get_assets(character_name)
    if any(a["asset_name"].lower() == asset_name.lower() for a in existing):
        await interaction.followup.send("That asset name is already used for this character. Choose a unique name.", ephemeral=True)
        return

    total_cost = total_cost_for_new_purchase(asset_type, tier)
    available = await db.get_balance(character_name)

    if available < total_cost:
        delta = total_cost - available
        await interaction.followup.send(
            f"**Insufficient funds.**\nAvailable: {format_val(available)}\nTotal cost: {format_val(total_cost)}\nShort by: {format_val(delta)}",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return

    await db.add_balance(character_name, -total_cost)
    await db.insert_asset(character_name, c.user_id, asset_name, asset_type, tier)

    await refresh_bank_for_character(guild, character_name)

    new_bal = await db.get_balance(character_name)
    await interaction.followup.send(
        f"Purchased **{asset_name}** for **{character_name}**: {asset_display(asset_type, tier)}\nCost: {format_val(total_cost)}\nNew balance: {format_val(new_bal)}",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    await log_econ(guild, f"[purchase_new] {interaction.user.id} purchased {asset_type} / {tier} for {character_name} name={asset_name} cost={total_cost} Val")


# -------------------------
# Lifecycle
# -------------------------

@client.event
async def on_ready():
    await db.start()

    # Remove stale/ghost commands and sync fresh.
    try:
        if COMMANDS_GUILD_ID:
            # Clear globals to prevent "shows in list but can't run" confusion.
            tree.clear_commands(guild=None)
            await tree.sync()

            # Clear & resync guild commands.
            tree.clear_commands(guild=GUILD_OBJ)
            await tree.sync(guild=GUILD_OBJ)
        else:
            await tree.sync()
    except Exception as e:
        print(f"[boot] command sync issue: {e}")

    print("[test] Starting EconBot_v37…")
    print(f"[test] Logged in as {client.user} (commands guild: {COMMANDS_GUILD_ID or 'GLOBAL'}; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")


def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
