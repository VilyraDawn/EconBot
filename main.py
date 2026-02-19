import os
import json
import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import discord
from discord import app_commands

import asyncpg

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# -------------------------
# Version / Config
# -------------------------
VERSION = "EconBot_v28"

TZ_NAME = "America/Chicago"
TZ = ZoneInfo(TZ_NAME) if ZoneInfo else dt.timezone.utc

BASE_DAILY_INCOME_VAL = 10  # 10 Cinths == 10 Val == 1 Arce

# Denominations (in Val)
DENOMS: List[Tuple[str, int]] = [
    ("NOVIR", 10_000),
    ("ORIN", 1_000),
    ("ELSH", 100),
    ("ARCE", 10),
    ("CINTH", 1),
]


def _get(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def _req_token() -> str:
    v = _get("DISCORD_TOKEN")
    if not v:
        raise RuntimeError("Missing required env var: DISCORD_TOKEN")
    return v


def _parse_int(v: str | None) -> int | None:
    try:
        return int(v) if v is not None and str(v).strip() != "" else None
    except Exception:
        return None


def _parse_int_list(v: str | None) -> list[int]:
    if not v:
        return []
    out: list[int] = []
    for part in v.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


def _parse_int_set(v: str | None) -> set[int]:
    return set(_parse_int_list(v))


DISCORD_TOKEN = _req_token()
DATABASE_URL = _get("DATABASE_URL")

GUILD_ID = _parse_int(_get("GUILD_ID"))
LEGACY_SOURCE_GUILD_ID = _parse_int(_get("LEGACY_SOURCE_GUILD_ID")) or GUILD_ID

BANK_CHANNEL_ID = _parse_int(_get("BANK_CHANNEL_ID"))
ECON_LOG_CHANNEL_ID = _parse_int(_get("ECON_LOG_CHANNEL_ID"))

BANK_MESSAGE_IDS = _parse_int_list(_get("BANK_MESSAGE_IDS"))

STAFF_ROLE_IDS = _parse_int_set(
    _get("STAFF_ROLE_IDS")
    or _get("Staff_Role_IDs")
    or _get("STAFF_ROLE_IDs")
    or _get("STAFF_ROLE_IDS")
)


# -------------------------
# Asset Catalog (from uploaded Asset Table.xlsx)
# Keys: (asset_type, secondary_type, tier_name)
# Costs are absolute (exact to spreadsheet) in Val.
# -------------------------
ASSET_CATALOG: List[Dict[str, object]] = [
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 1, "tier_name": "Apprentice", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 2, "tier_name": "Journeyman", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 3, "tier_name": "Leased Workshop", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 4, "tier_name": "Small Workshop", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 5, "tier_name": "Large Workshop", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 1, "tier_name": "Street Vendor", "cost_val": 200, "income_val": 25},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 2, "tier_name": "Market Stall", "cost_val": 500, "income_val": 60},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 3, "tier_name": "Small Shop", "cost_val": 1200, "income_val": 120},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 4, "tier_name": "Established Shop", "cost_val": 2200, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 5, "tier_name": "Prosperous Shop", "cost_val": 3500, "income_val": 300},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 6, "tier_name": "Regional Brand", "cost_val": 6000, "income_val": 450},
    {"asset_type": "Business", "secondary_type": "Independent Trade", "tier": 7, "tier_name": "Major Brand", "cost_val": 10000, "income_val": 700},
    {"asset_type": "Business", "secondary_type": "Hospitality", "tier": 1, "tier_name": "Cart & Kettle", "cost_val": 300, "income_val": 40},
    {"asset_type": "Business", "secondary_type": "Hospitality", "tier": 2, "tier_name": "Rustic Eatery", "cost_val": 900, "income_val": 120},
    {"asset_type": "Business", "secondary_type": "Hospitality", "tier": 3, "tier_name": "Comfort Tavern", "cost_val": 1800, "income_val": 220},
    {"asset_type": "Business", "secondary_type": "Hospitality", "tier": 4, "tier_name": "Fine Dining", "cost_val": 4500, "income_val": 450},
    {"asset_type": "Business", "secondary_type": "Hospitality", "tier": 5, "tier_name": "Luxury Estate Inn", "cost_val": 9000, "income_val": 800},
    {"asset_type": "Business", "secondary_type": "Agriculture", "tier": 1, "tier_name": "Subsistence Surplus", "cost_val": 250, "income_val": 25},
    {"asset_type": "Business", "secondary_type": "Agriculture", "tier": 2, "tier_name": "Modest Farm", "cost_val": 800, "income_val": 80},
    {"asset_type": "Business", "secondary_type": "Agriculture", "tier": 3, "tier_name": "Productive Holdings", "cost_val": 1800, "income_val": 160},
    {"asset_type": "Business", "secondary_type": "Agriculture", "tier": 4, "tier_name": "Large Ranch", "cost_val": 3500, "income_val": 300},
    {"asset_type": "Business", "secondary_type": "Agriculture", "tier": 5, "tier_name": "Regional Supplier", "cost_val": 7000, "income_val": 550},
    {"asset_type": "Business", "secondary_type": "Shipping", "tier": 1, "tier_name": "Courier Contract", "cost_val": 400, "income_val": 60},
    {"asset_type": "Business", "secondary_type": "Shipping", "tier": 2, "tier_name": "Local Fleet", "cost_val": 1200, "income_val": 180},
    {"asset_type": "Business", "secondary_type": "Shipping", "tier": 3, "tier_name": "Trade Route Share", "cost_val": 3000, "income_val": 380},
    {"asset_type": "Business", "secondary_type": "Shipping", "tier": 4, "tier_name": "Regional Logistics", "cost_val": 6500, "income_val": 650},
    {"asset_type": "Business", "secondary_type": "Shipping", "tier": 5, "tier_name": "Dominant Consortium", "cost_val": 13000, "income_val": 1200},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 1, "tier_name": "Modest Room", "cost_val": 150, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 2, "tier_name": "Small Cottage", "cost_val": 600, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 3, "tier_name": "Townhome", "cost_val": 1500, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 4, "tier_name": "Manor Home", "cost_val": 4000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 5, "tier_name": "Estate", "cost_val": 9000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 6, "tier_name": "Noble Villa", "cost_val": 18000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Residence", "tier": 7, "tier_name": "Royal Seat", "cost_val": 35000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 1, "tier_name": "Small Plot", "cost_val": 200, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 2, "tier_name": "Small Acreage", "cost_val": 900, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 3, "tier_name": "Large Acreage", "cost_val": 2500, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 4, "tier_name": "Regional Holdings", "cost_val": 7000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 5, "tier_name": "Lordship Grounds", "cost_val": 16000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 6, "tier_name": "Baronial Tracts", "cost_val": 30000, "income_val": 0},
    {"asset_type": "Property", "secondary_type": "Land", "tier": 7, "tier_name": "Ducal Domain", "cost_val": 55000, "income_val": 0},
    {"asset_type": "Asset", "secondary_type": "Vehicle", "tier": 1, "tier_name": "Pack Animal", "cost_val": 120, "income_val": 0},
    {"asset_type": "Asset", "secondary_type": "Vehicle", "tier": 2, "tier_name": "Cart", "cost_val": 350, "income_val": 0},
    {"asset_type": "Asset", "secondary_type": "Vehicle", "tier": 3, "tier_name": "Carriage", "cost_val": 900, "income_val": 0},
    {"asset_type": "Asset", "secondary_type": "Vehicle", "tier": 4, "tier_name": "Coach", "cost_val": 2000, "income_val": 0},
    {"asset_type": "Asset", "secondary_type": "Vehicle", "tier": 5, "tier_name": "Luxury Coach", "cost_val": 4500, "income_val": 0},
]


def _catalog_key(a: Dict[str, object]) -> Tuple[str, str, str]:
    return (str(a["asset_type"]), str(a["secondary_type"]), str(a["tier_name"]))


CATALOG_BY_KEY: Dict[Tuple[str, str, str], Dict[str, object]] = { _catalog_key(a): a for a in ASSET_CATALOG }


# -------------------------
# DB
# -------------------------
_pool: asyncpg.Pool | None = None


async def db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set; database features are disabled.")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def init_db() -> None:
    if not DATABASE_URL:
        print(f"[{VERSION}] DATABASE_URL missing; running without DB.")
        return

    pool = await db_pool()
    async with pool.acquire() as con:
        await con.execute("CREATE SCHEMA IF NOT EXISTS economy")

        # Balances (stored in Val)
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS economy.balances (
                guild_id BIGINT NOT NULL,
                character_name TEXT NOT NULL,
                balance_val BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (guild_id, character_name)
            )
            """
        )

        # Income claims (daily, per character)
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS economy.income_claims (
                guild_id BIGINT NOT NULL,
                character_name TEXT NOT NULL,
                claim_date DATE NOT NULL,
                claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (guild_id, character_name, claim_date)
            )
            """
        )

        # Owned assets (named by player; upgrades replace tier fields but keep asset_name)
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS economy.assets_owned (
                guild_id BIGINT NOT NULL,
                character_name TEXT NOT NULL,
                character_user_id BIGINT NOT NULL,
                asset_name TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                secondary_type TEXT NOT NULL,
                tier_name TEXT NOT NULL,
                tier INTEGER NOT NULL,
                cost_val BIGINT NOT NULL,
                income_val BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (guild_id, character_name, character_user_id, asset_name)
            )
            """
        )

        # Action log
        await con.execute(
            """
            CREATE TABLE IF NOT EXISTS economy.ledger (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                guild_id BIGINT NOT NULL,
                character_name TEXT NOT NULL,
                character_user_id BIGINT NOT NULL,
                actor_user_id BIGINT NOT NULL,
                kind TEXT NOT NULL,
                delta_val BIGINT NOT NULL,
                reason TEXT NOT NULL,
                details_json TEXT NOT NULL
            )
            """
        )


# -------------------------
# Legacy characters table helpers
# -------------------------
@dataclass
class Character:
    guild_id: int
    user_id: int
    name: str
    archived: bool


async def fetch_character_by_name(legacy_guild_id: int, name: str) -> Character | None:
    """Legacy source: public.characters. Character ID is name (unique per guild)."""
    if not DATABASE_URL:
        return None
    pool = await db_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            SELECT guild_id, user_id, name, archived
            FROM characters
            WHERE guild_id=$1 AND name=$2
            LIMIT 1
            """,
            int(legacy_guild_id),
            str(name),
        )
        if not row:
            return None
        return Character(
            guild_id=int(row["guild_id"]),
            user_id=int(row["user_id"]),
            name=str(row["name"]),
            archived=bool(row["archived"]),
        )


async def search_characters(legacy_guild_id: int, query: str, limit: int = 25) -> List[str]:
    if not DATABASE_URL:
        return []
    q = (query or "").strip()
    if not q:
        q = ""
    pool = await db_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT name
            FROM characters
            WHERE guild_id=$1
              AND archived=FALSE
              AND name ILIKE $2
            ORDER BY name ASC
            LIMIT $3
            """,
            int(legacy_guild_id),
            f"%{q}%",
            int(limit),
        )
        return [str(r["name"]) for r in rows]


# -------------------------
# Economy helpers
# -------------------------
async def get_balance_val(guild_id: int, character_name: str) -> int:
    if not DATABASE_URL:
        return 0
    pool = await db_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT balance_val FROM economy.balances WHERE guild_id=$1 AND character_name=$2",
            int(guild_id),
            str(character_name),
        )
        return int(row["balance_val"]) if row else 0


async def set_balance_val(guild_id: int, character_name: str, new_val: int) -> None:
    if not DATABASE_URL:
        return
    pool = await db_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.balances (guild_id, character_name, balance_val, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (guild_id, character_name)
            DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
            """,
            int(guild_id),
            str(character_name),
            int(new_val),
        )


async def add_balance_val(guild_id: int, character_name: str, delta: int) -> int:
    """Adds delta, never below 0. Returns updated balance."""
    if not DATABASE_URL:
        return 0
    pool = await db_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            cur = await con.fetchrow(
                "SELECT balance_val FROM economy.balances WHERE guild_id=$1 AND character_name=$2 FOR UPDATE",
                int(guild_id),
                str(character_name),
            )
            cur_val = int(cur["balance_val"]) if cur else 0
            new_val = cur_val + int(delta)
            if new_val < 0:
                new_val = 0
            await con.execute(
                """
                INSERT INTO economy.balances (guild_id, character_name, balance_val, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (guild_id, character_name)
                DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
                """,
                int(guild_id),
                str(character_name),
                int(new_val),
            )
            return int(new_val)


async def sum_asset_income_val(guild_id: int, character_name: str) -> int:
    if not DATABASE_URL:
        return 0
    pool = await db_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            SELECT COALESCE(SUM(income_val), 0) AS s
            FROM economy.assets_owned
            WHERE guild_id=$1 AND character_name=$2
            """,
            int(guild_id),
            str(character_name),
        )
        return int(row["s"]) if row else 0


async def fetch_assets_owned(guild_id: int, character_name: str) -> List[Dict[str, object]]:
    if not DATABASE_URL:
        return []
    pool = await db_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT asset_name, asset_type, secondary_type, tier_name, tier, cost_val, income_val, character_user_id
            FROM economy.assets_owned
            WHERE guild_id=$1 AND character_name=$2
            ORDER BY asset_type, secondary_type, tier DESC, asset_name
            """,
            int(guild_id),
            str(character_name),
        )
        return [dict(r) for r in rows]


async def ledger_write(
    guild_id: int,
    character_name: str,
    character_user_id: int,
    actor_user_id: int,
    kind: str,
    delta_val: int,
    reason: str,
    details: Dict[str, object] | None = None,
) -> None:
    if not DATABASE_URL:
        return
    pool = await db_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.ledger (guild_id, character_name, character_user_id, actor_user_id, kind, delta_val, reason, details_json)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """,
            int(guild_id),
            str(character_name),
            int(character_user_id),
            int(actor_user_id),
            str(kind),
            int(delta_val),
            str(reason),
            json.dumps(details or {}, ensure_ascii=False),
        )


# -------------------------
# Formatting
# -------------------------
UNIT_LABELS = {
    "CINTH": "Cinth",
    "ARCE": "Arce",
    "ELSH": "Elsh",
    "ORIN": "Orin",
    "NOVIR": "Novir",
}


def breakdown_denoms(val: int) -> List[Tuple[str, int]]:
    remaining = int(max(0, val))
    parts: List[Tuple[str, int]] = []
    for unit, unit_val in DENOMS:
        if remaining <= 0:
            break
        count = remaining // unit_val
        if count:
            parts.append((unit, int(count)))
            remaining -= count * unit_val
    if not parts:
        parts = [("CINTH", 0)]
    return parts


def format_money(val: int) -> str:
    parts = breakdown_denoms(val)
    out = []
    for unit, count in parts:
        label = UNIT_LABELS.get(unit, unit.title())
        plural = "" if count == 1 else "s"
        out.append(f"{count} {label}{plural}")
    return ", ".join(out)


def format_assets_lines(assets: List[Dict[str, object]]) -> str:
    if not assets:
        return "*(None)*"
    lines = []
    for a in assets:
        asset_name = str(a.get("asset_name", "")).strip()
        tier_name = str(a.get("tier_name", "")).strip()
        lines.append(f"{asset_name} — {tier_name}")
    return "\n".join(lines)


# -------------------------
# Discord setup
# -------------------------
intents = discord.Intents.none()
intents.guilds = True

client = discord.Client(intents=intents, allowed_mentions=discord.AllowedMentions.none())
tree = app_commands.CommandTree(client)


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg: str
    if isinstance(error, app_commands.CheckFailure):
        msg = str(error) or "You don’t have permission to use that command."
    elif isinstance(error, app_commands.CommandInvokeError) and getattr(error, "original", None) is not None:
        orig = error.original
        msg = f"Command error: {type(orig).__name__}: {orig}"
    else:
        msg = f"Command error: {type(error).__name__}: {error}"

    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        else:
            await interaction.response.send_message(msg, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass


def require_admin():
    """Staff check using STAFF_ROLE_IDS; if not configured, fall back to admin perms."""

    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise app_commands.CheckFailure("This command can only be used in a server.")

        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None:
            member = interaction.guild.get_member(interaction.user.id)

        if member is None:
            raise app_commands.CheckFailure("I couldn't verify your roles/permissions. Try again in a moment.")

        if STAFF_ROLE_IDS:
            if any(r.id in STAFF_ROLE_IDS for r in getattr(member, "roles", [])):
                return True
            raise app_commands.CheckFailure("You don’t have permission to use that command.")

        perms = getattr(member, "guild_permissions", None)
        if perms and (perms.administrator or perms.manage_guild):
            return True

        raise app_commands.CheckFailure(
            "Staff roles are not configured (STAFF_ROLE_IDS). Ask an admin to set STAFF_ROLE_IDS in Railway Variables."
        )

    return app_commands.check(predicate)


# -------------------------
# Autocomplete
# -------------------------
async def character_name_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    if not LEGACY_SOURCE_GUILD_ID:
        return []
    names = await search_characters(int(LEGACY_SOURCE_GUILD_ID), current or "", limit=25)
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]


async def owned_asset_name_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    # expects "character" to be in namespace
    char = str(getattr(interaction.namespace, "character", "") or "").strip()
    if not char or not LEGACY_SOURCE_GUILD_ID:
        return []
    assets = await fetch_assets_owned(int(LEGACY_SOURCE_GUILD_ID), char)
    cur = (current or "").lower().strip()
    names = []
    for a in assets:
        nm = str(a.get("asset_name", "")).strip()
        if not nm:
            continue
        if not cur or cur in nm.lower():
            names.append(nm)
    names = sorted(set(names))[:25]
    return [app_commands.Choice(name=n, value=n) for n in names]


# -------------------------
# Commands
# -------------------------
COMMAND_HELP_LINES = [
    ("balance", "Show a character’s current money and owned assets. Anyone can use this; it matches the public bank dashboard."),
    ("income", "Claim daily income for one of YOUR characters (once per day, Chicago time). Adds base income plus income from owned assets."),
    ("econ_adjust", "Staff-only. Add or subtract money from a character. The bot will not allow balances to go negative."),
    ("econ_set_balance", "Staff-only. Set a character’s balance to an exact amount (non-negative). Useful for corrections."),
    ("econ_purchase", "Staff-only. NEW purchase or UPGRADE an existing asset. Charges the full cost listed in the asset spreadsheet."),
    ("econ_commands", "Staff-only. Shows this command list with short descriptions (kept updated as we add features)."),
]


@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.")
@require_admin()
async def econ_commands_cmd(interaction: discord.Interaction):
    await interaction.response.defer(thinking=False, ephemeral=True)

    em = discord.Embed(title="EconBot Commands", description="Quick reference (kept updated as we build).")
    public = []
    staff = []
    for name, desc in COMMAND_HELP_LINES:
        line = f"**/{name}** — {desc}"
        if name in {"balance", "income"}:
            public.append(line)
        else:
            staff.append(line)

    if public:
        em.add_field(name="Player Commands", value="\n".join(public), inline=False)
    if staff:
        em.add_field(name="Staff Commands", value="\n".join(staff), inline=False)

    em.set_footer(text=VERSION)
    await interaction.followup.send(embed=em, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="balance", description="Show a character’s money + assets.")
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_name_autocomplete)
async def balance_cmd(interaction: discord.Interaction, character: str):
    await interaction.response.defer(thinking=False, ephemeral=True)

    if not LEGACY_SOURCE_GUILD_ID:
        await interaction.followup.send("LEGACY_SOURCE_GUILD_ID is not set.", ephemeral=True)
        return

    ch = await fetch_character_by_name(int(LEGACY_SOURCE_GUILD_ID), character)
    if not ch or ch.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    bal = await get_balance_val(ch.guild_id, ch.name)
    assets = await fetch_assets_owned(ch.guild_id, ch.name)

    em = discord.Embed(title=f"{ch.name}", description=f"**Balance:** {format_money(bal)}")
    em.add_field(name="__*Assets*__", value=format_assets_lines(assets), inline=False)
    em.set_footer(text=VERSION)
    await interaction.followup.send(embed=em, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


@tree.command(name="income", description="Owner-only: claim daily income for your character (Chicago time).")
@app_commands.describe(character="Pick one of your characters")
@app_commands.autocomplete(character=character_name_autocomplete)
async def income_cmd(interaction: discord.Interaction, character: str):
    await interaction.response.defer(thinking=False, ephemeral=True)

    if not (DATABASE_URL and LEGACY_SOURCE_GUILD_ID):
        await interaction.followup.send("Database is not configured.", ephemeral=True)
        return

    ch = await fetch_character_by_name(int(LEGACY_SOURCE_GUILD_ID), character)
    if not ch or ch.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    if interaction.user.id != ch.user_id:
        await interaction.followup.send("You can only claim income for characters you own.", ephemeral=True)
        return

    today = dt.datetime.now(TZ).date()

    pool = await db_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            exists = await con.fetchrow(
                "SELECT 1 FROM economy.income_claims WHERE guild_id=$1 AND character_name=$2 AND claim_date=$3",
                int(ch.guild_id),
                str(ch.name),
                today,
            )
            if exists:
                await interaction.followup.send("Income already claimed for today.", ephemeral=True)
                return

            asset_income = await sum_asset_income_val(ch.guild_id, ch.name)
            total_income = int(BASE_DAILY_INCOME_VAL + asset_income)

            # Apply
            cur = await con.fetchrow(
                "SELECT balance_val FROM economy.balances WHERE guild_id=$1 AND character_name=$2 FOR UPDATE",
                int(ch.guild_id),
                str(ch.name),
            )
            cur_val = int(cur["balance_val"]) if cur else 0
            new_val = cur_val + total_income

            await con.execute(
                """
                INSERT INTO economy.balances (guild_id, character_name, balance_val, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (guild_id, character_name)
                DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
                """,
                int(ch.guild_id),
                str(ch.name),
                int(new_val),
            )

            await con.execute(
                "INSERT INTO economy.income_claims (guild_id, character_name, claim_date) VALUES ($1,$2,$3)",
                int(ch.guild_id),
                str(ch.name),
                today,
            )

            await con.execute(
                """
                INSERT INTO economy.ledger (guild_id, character_name, character_user_id, actor_user_id, kind, delta_val, reason, details_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                int(ch.guild_id),
                str(ch.name),
                int(ch.user_id),
                int(interaction.user.id),
                "INCOME",
                int(total_income),
                "Daily income",
                json.dumps({"base": BASE_DAILY_INCOME_VAL, "assets": asset_income}, ensure_ascii=False),
            )

    await interaction.followup.send(
        f"Income claimed: **{format_money(total_income)}**. New balance: **{format_money(new_val)}**.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    asyncio.create_task(rebuild_dashboard())


@tree.command(name="econ_adjust", description="Staff: add/subtract currency (never negative).")
@require_admin()
@app_commands.describe(character="Pick a character", delta_val="Delta in Val (negative subtracts)", reason="Why")
@app_commands.autocomplete(character=character_name_autocomplete)
async def econ_adjust_cmd(interaction: discord.Interaction, character: str, delta_val: int, reason: str):
    await interaction.response.defer(thinking=False, ephemeral=True)

    if not (DATABASE_URL and LEGACY_SOURCE_GUILD_ID):
        await interaction.followup.send("Database is not configured.", ephemeral=True)
        return

    ch = await fetch_character_by_name(int(LEGACY_SOURCE_GUILD_ID), character)
    if not ch or ch.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    cur = await get_balance_val(ch.guild_id, ch.name)
    if cur + int(delta_val) < 0:
        await interaction.followup.send("Insufficient funds (this action would make the balance negative).", ephemeral=True)
        return

    new_val = await add_balance_val(ch.guild_id, ch.name, int(delta_val))
    await ledger_write(ch.guild_id, ch.name, ch.user_id, interaction.user.id, "ADJUST", int(delta_val), reason, {})

    await interaction.followup.send(
        f"Updated **{ch.name}**: {format_money(cur)} → **{format_money(new_val)}**.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    asyncio.create_task(rebuild_dashboard())


@tree.command(name="econ_set_balance", description="Staff: set exact balance (non-negative).")
@require_admin()
@app_commands.describe(character="Pick a character", balance_val="Exact balance in Val", reason="Why")
@app_commands.autocomplete(character=character_name_autocomplete)
async def econ_set_balance_cmd(interaction: discord.Interaction, character: str, balance_val: int, reason: str):
    await interaction.response.defer(thinking=False, ephemeral=True)

    if not (DATABASE_URL and LEGACY_SOURCE_GUILD_ID):
        await interaction.followup.send("Database is not configured.", ephemeral=True)
        return

    if int(balance_val) < 0:
        await interaction.followup.send("Balance cannot be negative.", ephemeral=True)
        return

    ch = await fetch_character_by_name(int(LEGACY_SOURCE_GUILD_ID), character)
    if not ch or ch.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    cur = await get_balance_val(ch.guild_id, ch.name)
    await set_balance_val(ch.guild_id, ch.name, int(balance_val))
    await ledger_write(ch.guild_id, ch.name, ch.user_id, interaction.user.id, "SET_BALANCE", int(balance_val) - cur, reason, {"set_to": int(balance_val)})

    await interaction.followup.send(
        f"Set **{ch.name}** balance: {format_money(cur)} → **{format_money(int(balance_val))}**.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    asyncio.create_task(rebuild_dashboard())


PURCHASE_MODE_CHOICES = [
    app_commands.Choice(name="New Purchase", value="NEW"),
    app_commands.Choice(name="Upgrade Existing", value="UPGRADE"),
]


async def asset_type_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    cur = (current or "").lower().strip()
    types = sorted({str(a["asset_type"]) for a in ASSET_CATALOG})
    out = [t for t in types if not cur or cur in t.lower()]
    return [app_commands.Choice(name=t, value=t) for t in out[:25]]


async def tier_name_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    asset_type = str(getattr(interaction.namespace, "asset_type", "") or "").strip()
    cur = (current or "").lower().strip()
    names = []
    for a in ASSET_CATALOG:
        if asset_type and str(a["asset_type"]) != asset_type:
            continue
        tn = str(a["tier_name"]) 
        if not cur or cur in tn.lower():
            names.append(tn)
    names = sorted(set(names))[:25]
    return [app_commands.Choice(name=n, value=n) for n in names]


async def secondary_type_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    asset_type = str(getattr(interaction.namespace, "asset_type", "") or "").strip()
    tier_name = str(getattr(interaction.namespace, "tier_name", "") or "").strip()
    cur = (current or "").lower().strip()
    secs = []
    for a in ASSET_CATALOG:
        if asset_type and str(a["asset_type"]) != asset_type:
            continue
        if tier_name and str(a["tier_name"]) != tier_name:
            continue
        st = str(a["secondary_type"]) 
        if not cur or cur in st.lower():
            secs.append(st)
    secs = sorted(set(secs))[:25]
    return [app_commands.Choice(name=s, value=s) for s in secs]


@tree.command(name="econ_purchase", description="Staff: purchase NEW asset or UPGRADE existing asset.")
@require_admin()
@app_commands.describe(
    character="Pick a character",
    mode="New Purchase or Upgrade Existing",
    asset_type="Type (Business/Property/Asset)",
    tier_name="Tier name (exact to spreadsheet)",
    secondary_type="Sub-type (exact to spreadsheet)",
    asset_name="For NEW purchases: name the asset (e.g., 'Vaelith Ranch'). For upgrades: leave blank.",
    upgrade_asset_name="For UPGRADE: pick the existing owned asset name to upgrade.",
)
@app_commands.autocomplete(
    character=character_name_autocomplete,
    asset_type=asset_type_autocomplete,
    tier_name=tier_name_autocomplete,
    secondary_type=secondary_type_autocomplete,
    upgrade_asset_name=owned_asset_name_autocomplete,
)
@app_commands.choices(mode=PURCHASE_MODE_CHOICES)
async def econ_purchase_cmd(
    interaction: discord.Interaction,
    character: str,
    mode: app_commands.Choice[str],
    asset_type: str,
    tier_name: str,
    secondary_type: str,
    asset_name: Optional[str] = None,
    upgrade_asset_name: Optional[str] = None,
):
    await interaction.response.defer(thinking=False, ephemeral=True)

    if not (DATABASE_URL and LEGACY_SOURCE_GUILD_ID):
        await interaction.followup.send("Database is not configured.", ephemeral=True)
        return

    ch = await fetch_character_by_name(int(LEGACY_SOURCE_GUILD_ID), character)
    if not ch or ch.archived:
        await interaction.followup.send("Character not found.", ephemeral=True)
        return

    key = (asset_type.strip(), secondary_type.strip(), tier_name.strip())
    cat = CATALOG_BY_KEY.get(key)
    if not cat:
        await interaction.followup.send("That asset type / sub-type / tier name combination is not in the asset spreadsheet.", ephemeral=True)
        return

    cost_val = int(cat["cost_val"])  # absolute
    income_val = int(cat["income_val"]) 
    tier = int(cat["tier"]) 

    cur_bal = await get_balance_val(ch.guild_id, ch.name)
    if cur_bal - cost_val < 0:
        await interaction.followup.send("Insufficient funds for this purchase.", ephemeral=True)
        return

    pool = await db_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            # lock balance
            row = await con.fetchrow(
                "SELECT balance_val FROM economy.balances WHERE guild_id=$1 AND character_name=$2 FOR UPDATE",
                int(ch.guild_id),
                str(ch.name),
            )
            locked_bal = int(row["balance_val"]) if row else 0
            if locked_bal - cost_val < 0:
                await interaction.followup.send("Insufficient funds for this purchase.", ephemeral=True)
                return

            if mode.value == "NEW":
                nm = (asset_name or "").strip()
                if not nm:
                    await interaction.followup.send("For a NEW purchase, you must provide an asset_name.", ephemeral=True)
                    return

                # insert asset
                await con.execute(
                    """
                    INSERT INTO economy.assets_owned
                      (guild_id, character_name, character_user_id, asset_name, asset_type, secondary_type, tier_name, tier, cost_val, income_val, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                    ON CONFLICT (guild_id, character_name, character_user_id, asset_name)
                    DO UPDATE SET asset_type=EXCLUDED.asset_type,
                                  secondary_type=EXCLUDED.secondary_type,
                                  tier_name=EXCLUDED.tier_name,
                                  tier=EXCLUDED.tier,
                                  cost_val=EXCLUDED.cost_val,
                                  income_val=EXCLUDED.income_val,
                                  updated_at=NOW()
                    """,
                    int(ch.guild_id),
                    str(ch.name),
                    int(ch.user_id),
                    nm,
                    asset_type.strip(),
                    secondary_type.strip(),
                    tier_name.strip(),
                    int(tier),
                    int(cost_val),
                    int(income_val),
                )

                action_details = {
                    "mode": "NEW",
                    "asset_name": nm,
                    "asset_type": asset_type,
                    "secondary_type": secondary_type,
                    "tier_name": tier_name,
                    "cost_val": cost_val,
                    "income_val": income_val,
                }

            else:
                # upgrade
                target = (upgrade_asset_name or "").strip()
                if not target:
                    await interaction.followup.send("For an UPGRADE, you must select upgrade_asset_name.", ephemeral=True)
                    return

                owned = await con.fetchrow(
                    """
                    SELECT asset_name
                    FROM economy.assets_owned
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND asset_name=$4
                    """,
                    int(ch.guild_id),
                    str(ch.name),
                    int(ch.user_id),
                    target,
                )
                if not owned:
                    await interaction.followup.send("That owned asset was not found for this character.", ephemeral=True)
                    return

                # update tier fields ONLY (no renaming)
                await con.execute(
                    """
                    UPDATE economy.assets_owned
                    SET asset_type=$5,
                        secondary_type=$6,
                        tier_name=$7,
                        tier=$8,
                        cost_val=$9,
                        income_val=$10,
                        updated_at=NOW()
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND asset_name=$4
                    """,
                    int(ch.guild_id),
                    str(ch.name),
                    int(ch.user_id),
                    target,
                    asset_type.strip(),
                    secondary_type.strip(),
                    tier_name.strip(),
                    int(tier),
                    int(cost_val),
                    int(income_val),
                )

                action_details = {
                    "mode": "UPGRADE",
                    "asset_name": target,
                    "asset_type": asset_type,
                    "secondary_type": secondary_type,
                    "tier_name": tier_name,
                    "cost_val": cost_val,
                    "income_val": income_val,
                }

            # subtract funds (never negative due to checks)
            new_bal = locked_bal - cost_val
            await con.execute(
                """
                INSERT INTO economy.balances (guild_id, character_name, balance_val, updated_at)
                VALUES ($1,$2,$3,NOW())
                ON CONFLICT (guild_id, character_name)
                DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
                """,
                int(ch.guild_id),
                str(ch.name),
                int(new_bal),
            )

            await con.execute(
                """
                INSERT INTO economy.ledger (guild_id, character_name, character_user_id, actor_user_id, kind, delta_val, reason, details_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                int(ch.guild_id),
                str(ch.name),
                int(ch.user_id),
                int(interaction.user.id),
                "PURCHASE",
                int(-cost_val),
                "Asset purchase/upgrade",
                json.dumps(action_details, ensure_ascii=False),
            )

    await interaction.followup.send(
        f"Recorded {mode.name.lower()} for **{ch.name}**. Charged **{format_money(cost_val)}**. New balance: **{format_money(new_bal)}**.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    asyncio.create_task(rebuild_dashboard())


# -------------------------
# Dashboard
# -------------------------
async def rebuild_dashboard() -> None:
    if not (DATABASE_URL and BANK_CHANNEL_ID and BANK_MESSAGE_IDS and LEGACY_SOURCE_GUILD_ID):
        return

    # fetch all characters in legacy guild
    pool = await db_pool()
    async with pool.acquire() as con:
        chars = await con.fetch(
            """
            SELECT name
            FROM characters
            WHERE guild_id=$1 AND archived=FALSE
            ORDER BY name ASC
            """,
            int(LEGACY_SOURCE_GUILD_ID),
        )

    # build embed pages (1 embed per message id)
    lines: List[str] = []
    for r in chars:
        nm = str(r["name"]) 
        bal = await get_balance_val(int(LEGACY_SOURCE_GUILD_ID), nm)
        assets = await fetch_assets_owned(int(LEGACY_SOURCE_GUILD_ID), nm)
        asset_line = ""
        if assets:
            # show asset summary (names + tier)
            asset_line = " | " + ", ".join([f"{str(a['asset_name'])}—{str(a['tier_name'])}" for a in assets][:3])
            if len(assets) > 3:
                asset_line += "…"
        lines.append(f"**{nm}** — {format_money(bal)}{asset_line}")

    # chunk into message-sized pages
    pages: List[str] = []
    current = []
    char_count = 0
    for line in lines:
        if sum(len(x) + 1 for x in current) + len(line) + 1 > 3500:
            pages.append("\n".join(current))
            current = []
        current.append(line)
        char_count += 1
    if current:
        pages.append("\n".join(current))

    # pad/truncate to BANK_MESSAGE_IDS length
    while len(pages) < len(BANK_MESSAGE_IDS):
        pages.append("*(No additional characters)*")
    pages = pages[: len(BANK_MESSAGE_IDS)]

    channel = client.get_channel(int(BANK_CHANNEL_ID))
    if not isinstance(channel, discord.TextChannel):
        return

    for msg_id, body in zip(BANK_MESSAGE_IDS, pages):
        try:
            msg = await channel.fetch_message(int(msg_id))
        except Exception:
            continue

        em = discord.Embed(title="Bank of Vilyra", description=body)
        em.set_footer(text=f"{VERSION} • Updated {dt.datetime.now(TZ).strftime('%Y-%m-%d %H:%M %Z')}")
        try:
            await msg.edit(embed=em, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            pass


# -------------------------
# Events
# -------------------------
@client.event
async def on_ready():
    try:
        await init_db()
    except Exception as e:
        print(f"[{VERSION}] DB init failed: {e}")
        raise

    # Sync commands (guild-scoped for fast iteration)
    if GUILD_ID:
        guild_obj = discord.Object(id=int(GUILD_ID))
        try:
            tree.copy_global_to(guild=guild_obj)
            await tree.sync(guild=guild_obj)
        except Exception as e:
            print(f"[{VERSION}] Command sync failed: {e}")
    else:
        try:
            await tree.sync()
        except Exception as e:
            print(f"[{VERSION}] Global command sync failed: {e}")

    print(f"[test] Starting {VERSION}…")
    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID or 'GLOBAL'}; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")

    asyncio.create_task(rebuild_dashboard())


def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
