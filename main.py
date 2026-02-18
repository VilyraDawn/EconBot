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
VERSION = "EconBot_v26"

TZ_NAME = "America/Chicago"
TZ = ZoneInfo(TZ_NAME) if ZoneInfo else dt.timezone.utc

BASE_DAILY_INCOME_VAL = 10  # 10 Cinths == 10 Val == 1 Arce

# Denominations (in Val)
DENOMS = [
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

STAFF_ROLE_IDS = _parse_int_set(_get("STAFF_ROLE_IDS") or _get("Staff_Role_IDs") or _get("STAFF_ROLE_IDs") or _get("STAFF_ROLE_IDS"))
ECON_ADMIN_ROLE_IDS = _parse_int_set(_get("ECON_ADMIN_ROLE_IDS"))
# Back-compat: if ECON_ADMIN_ROLE_IDS is set but STAFF_ROLE_IDS is empty, use it.
if (not STAFF_ROLE_IDS) and ECON_ADMIN_ROLE_IDS:
    STAFF_ROLE_IDS = set(ECON_ADMIN_ROLE_IDS)


# -------------------------
# Asset catalog seed (EXACT list you approved)
# -------------------------
ASSET_CATALOG_SEED: List[Dict[str, object]] = [
    # Business - Guild Trade Workshop
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 1, "tier_name": "Apprentice", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 2, "tier_name": "Journeyman", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 3, "tier_name": "Leased Workshop", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 4, "tier_name": "Small Workshop", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 5, "tier_name": "Large Workshop", "cost_val": 3000, "income_val": 250},

    # Business - Market Stall
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 1, "tier_name": "Consignment Arrangement", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 2, "tier_name": "Small Alley Stand", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 3, "tier_name": "Market Stall", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 4, "tier_name": "Small Shop", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 5, "tier_name": "Large Shop", "cost_val": 3000, "income_val": 250},

    # Business - Farm/Ranch
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 1, "tier_name": "Subsistence Surplus", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 2, "tier_name": "Leased Fields", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 3, "tier_name": "Owned Acre", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 4, "tier_name": "Small Fields and Barn", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 5, "tier_name": "Large Fields and Barn", "cost_val": 3000, "income_val": 250},

    # Business - Tavern/Inn
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 1, "tier_name": "One-Room Flophouse", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 2, "tier_name": "Leased Establishment", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 3, "tier_name": "Small Tavern", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 4, "tier_name": "Large Tavern", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 5, "tier_name": "Large Tavern and Inn", "cost_val": 3000, "income_val": 250},

    # Business - Warehouse/Trade House
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 1, "tier_name": "Small Storage Shed", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 2, "tier_name": "Large Storage Shed", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 3, "tier_name": "Small Trading Post", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 4, "tier_name": "Large Trading Post", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 5, "tier_name": "Large Warehouse and Trading Post", "cost_val": 3000, "income_val": 250},

    # House (income 0)
    {"asset_type": "House", "secondary_type": "House", "tier": 1, "tier_name": "Shack", "cost_val": 600, "income_val": 0},
    {"asset_type": "House", "secondary_type": "House", "tier": 2, "tier_name": "Hut", "cost_val": 1200, "income_val": 0},
    {"asset_type": "House", "secondary_type": "House", "tier": 3, "tier_name": "House", "cost_val": 2000, "income_val": 0},
    {"asset_type": "House", "secondary_type": "House", "tier": 4, "tier_name": "Lodge", "cost_val": 3000, "income_val": 0},
    {"asset_type": "House", "secondary_type": "House", "tier": 5, "tier_name": "Mansion", "cost_val": 5000, "income_val": 0},

    # Village
    {"asset_type": "Village", "secondary_type": "Village", "tier": 1, "tier_name": "Chartered Assembly", "cost_val": 1200, "income_val": 100},
    {"asset_type": "Village", "secondary_type": "Village", "tier": 2, "tier_name": "Hamlet", "cost_val": 2400, "income_val": 200},
    {"asset_type": "Village", "secondary_type": "Village", "tier": 3, "tier_name": "Village", "cost_val": 4800, "income_val": 300},
    {"asset_type": "Village", "secondary_type": "Village", "tier": 4, "tier_name": "Town", "cost_val": 9600, "income_val": 400},
    {"asset_type": "Village", "secondary_type": "Village", "tier": 5, "tier_name": "Small City", "cost_val": 15000, "income_val": 500},

    # Weapons
    {"asset_type": "Weapons", "secondary_type": "Weapons", "tier": 1, "tier_name": "Hit +1 / Dmg +1d4", "cost_val": 300, "income_val": 0},
    {"asset_type": "Weapons", "secondary_type": "Weapons", "tier": 2, "tier_name": "Hit +1 / Dmg +1d6", "cost_val": 600, "income_val": 0},
    {"asset_type": "Weapons", "secondary_type": "Weapons", "tier": 3, "tier_name": "Hit +2 / Dmg +1d8", "cost_val": 1200, "income_val": 0},
    {"asset_type": "Weapons", "secondary_type": "Weapons", "tier": 4, "tier_name": "Hit +2 / Dmg +1d10", "cost_val": 2400, "income_val": 0},
    {"asset_type": "Weapons", "secondary_type": "Weapons", "tier": 5, "tier_name": "Hit +2 / Dmg +1d12", "cost_val": 4800, "income_val": 0},

    # Armor
    {"asset_type": "Armor", "secondary_type": "Armor", "tier": 1, "tier_name": "AC +1", "cost_val": 300, "income_val": 0},
    {"asset_type": "Armor", "secondary_type": "Armor", "tier": 2, "tier_name": "AC +2", "cost_val": 600, "income_val": 0},
    {"asset_type": "Armor", "secondary_type": "Armor", "tier": 3, "tier_name": "AC +2 / Adv Magic Atk", "cost_val": 1200, "income_val": 0},
    {"asset_type": "Armor", "secondary_type": "Armor", "tier": 4, "tier_name": "AC +2 / Adv Magic and Melee Atk", "cost_val": 2400, "income_val": 0},
    {"asset_type": "Armor", "secondary_type": "Armor", "tier": 5, "tier_name": "AC +3 / Adv Magic and Melee Atk", "cost_val": 4800, "income_val": 0},
]


# -------------------------
# DB / Models
# -------------------------
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
    return _pool


@dataclass(frozen=True)
class LegacyCharacter:
    guild_id: int
    user_id: int
    name: str
    archived: bool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("CREATE SCHEMA IF NOT EXISTS economy")

            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.balances (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    character_user_id BIGINT NOT NULL,
                    balance_val BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, character_name, character_user_id)
                )
                """
            )

            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.income_claims (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    character_user_id BIGINT NOT NULL,
                    last_claim_date DATE NOT NULL,
                    PRIMARY KEY (guild_id, character_name, character_user_id)
                )
                """
            )

            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.asset_catalog (
                    asset_type TEXT NOT NULL,
                    secondary_type TEXT NOT NULL,
                    tier INT NOT NULL,
                    tier_name TEXT NOT NULL,
                    cost_val BIGINT NOT NULL,
                    income_val BIGINT NOT NULL,
                    PRIMARY KEY (asset_type, secondary_type, tier)
                )
                """
            )

            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.character_assets (
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    character_user_id BIGINT NOT NULL,
                    asset_name TEXT NOT NULL,               -- custom name
                    asset_type TEXT NOT NULL,
                    secondary_type TEXT NOT NULL,
                    tier INT NOT NULL,
                    tier_name TEXT NOT NULL,
                    cost_val BIGINT NOT NULL,
                    income_val BIGINT NOT NULL,
                    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, character_name, character_user_id, asset_name)
                )
                """
            )

            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS economy.transactions (
                    id BIGSERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    character_name TEXT NOT NULL,
                    character_user_id BIGINT NOT NULL,
                    actor_user_id BIGINT NOT NULL,
                    kind TEXT NOT NULL,           -- INCOME, ADJUST, PURCHASE, UPGRADE, SET_BALANCE
                    delta_val BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

            # Seed asset catalog safely (no duplicates; updates if changed)
            for row in ASSET_CATALOG_SEED:
                await con.execute(
                    """
                    INSERT INTO economy.asset_catalog (asset_type, secondary_type, tier, tier_name, cost_val, income_val)
                    VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (asset_type, secondary_type, tier)
                    DO UPDATE SET tier_name=EXCLUDED.tier_name, cost_val=EXCLUDED.cost_val, income_val=EXCLUDED.income_val
                    """,
                    str(row["asset_type"]),
                    str(row["secondary_type"]),
                    int(row["tier"]),
                    str(row["tier_name"]),
                    int(row["cost_val"]),
                    int(row["income_val"]),
                )


# -------------------------
# Legacy character lookups
# -------------------------
async def fetch_legacy_character_by_name(con: asyncpg.Connection, name: str) -> Optional[LegacyCharacter]:
    # Legacy table name is "characters" in the connected database (default schema likely public).
    row = await con.fetchrow(
        """
        SELECT guild_id, user_id, name, archived
        FROM characters
        WHERE guild_id=$1 AND name=$2
        LIMIT 1
        """,
        LEGACY_SOURCE_GUILD_ID,
        name,
    )
    if not row:
        return None
    return LegacyCharacter(
        guild_id=int(row["guild_id"]),
        user_id=int(row["user_id"]),
        name=str(row["name"]),
        archived=bool(row["archived"]),
    )


async def search_legacy_characters(con: asyncpg.Connection, query: str, limit: int = 25) -> List[LegacyCharacter]:
    q = (query or "").strip()
    rows = await con.fetch(
        """
        SELECT guild_id, user_id, name, archived
        FROM characters
        WHERE guild_id=$1
          AND archived=false
          AND name ILIKE $2
        ORDER BY name ASC
        LIMIT $3
        """,
        LEGACY_SOURCE_GUILD_ID,
        f"%{q}%",
        limit,
    )
    return [
        LegacyCharacter(int(r["guild_id"]), int(r["user_id"]), str(r["name"]), bool(r["archived"]))
        for r in rows
    ]


# -------------------------
# Economy helpers
# -------------------------
def now_chicago_date() -> dt.date:
    return dt.datetime.now(TZ).date()


def format_denoms(val: int) -> str:
    if val <= 0:
        return "0 Val"
    remaining = val
    parts = []
    for name, unit in DENOMS:
        if remaining >= unit:
            qty = remaining // unit
            remaining -= qty * unit
            parts.append(f"{qty} {name.title()}")
    if remaining > 0:
        parts.append(f"{remaining} Val")
    return ", ".join(parts)


async def get_balance_val(con: asyncpg.Connection, guild_id: int, character_name: str, character_user_id: int) -> int:
    row = await con.fetchrow(
        """
        SELECT balance_val
        FROM economy.balances
        WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
        """,
        guild_id, character_name, character_user_id
    )
    return int(row["balance_val"]) if row else 0


async def set_balance_val(con: asyncpg.Connection, guild_id: int, character_name: str, character_user_id: int, new_val: int):
    await con.execute(
        """
        INSERT INTO economy.balances (guild_id, character_name, character_user_id, balance_val)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (guild_id, character_name, character_user_id)
        DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW()
        """,
        guild_id, character_name, character_user_id, int(new_val)
    )


async def adjust_balance_val_guarded(
    con: asyncpg.Connection,
    guild_id: int,
    character_name: str,
    character_user_id: int,
    delta_val: int,
) -> Tuple[bool, int]:
    """
    Adjust balance and prevent going negative.
    Returns (ok, new_balance).
    """
    current = await get_balance_val(con, guild_id, character_name, character_user_id)
    new_val = current + int(delta_val)
    if new_val < 0:
        return (False, current)
    await set_balance_val(con, guild_id, character_name, character_user_id, new_val)
    return (True, new_val)


async def fetch_assets_for_character(con: asyncpg.Connection, guild_id: int, character_name: str, character_user_id: int):
    return await con.fetch(
        """
        SELECT asset_name, secondary_type, tier_name, tier, income_val
        FROM economy.character_assets
        WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
        ORDER BY secondary_type ASC, tier ASC, asset_name ASC
        """,
        guild_id, character_name, character_user_id
    )


async def sum_asset_income(con: asyncpg.Connection, guild_id: int, character_name: str, character_user_id: int) -> int:
    v = await con.fetchval(
        """
        SELECT COALESCE(SUM(income_val), 0)
        FROM economy.character_assets
        WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
        """,
        guild_id, character_name, character_user_id
    )
    return int(v or 0)


async def log_tx(
    con: asyncpg.Connection,
    guild_id: int,
    character_name: str,
    character_user_id: int,
    actor_user_id: int,
    kind: str,
    delta_val: int,
    reason: str,
    details: Dict[str, object] | None = None,
):
    await con.execute(
        """
        INSERT INTO economy.transactions (guild_id, character_name, character_user_id, actor_user_id, kind, delta_val, reason, details_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
        guild_id,
        character_name,
        character_user_id,
        actor_user_id,
        kind,
        int(delta_val),
        str(reason),
        json.dumps(details or {}, ensure_ascii=False),
    )


# -------------------------
# Discord bot setup
# -------------------------
intents = discord.Intents.default()
intents.guilds = True
intents.members = True  # needed to check roles reliably

client = discord.Client(intents=intents, allowed_mentions=discord.AllowedMentions.none())
tree = app_commands.CommandTree(client)


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # Always respond ephemerally and never ping
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


def is_admin_member(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    m = interaction.guild.get_member(interaction.user.id)
    if m is None:
        return False
    return any(r.id in STAFF_ROLE_IDS for r in m.roles)


def require_admin():
    """
    Returns a discord.app_commands check.

    Staff gating:
      - If STAFF_ROLE_IDS (or back-compat ECON_ADMIN_ROLE_IDS) is configured:
        user must have one of those roles.
      - If not configured:
        fall back to Discord permissions (Administrator or Manage Server).
    """
    async def predicate(interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if member is None and interaction.guild is not None:
            member = interaction.guild.get_member(interaction.user.id)

        if member is None:
            raise app_commands.CheckFailure("I couldn't verify your roles/permissions right now. Please try again.")

        # Prefer STAFF_ROLE_IDS; if empty but ECON_ADMIN_ROLE_IDS exists, treat that as staff roles (back-compat).
        staff_roles = set(STAFF_ROLE_IDS) if STAFF_ROLE_IDS else set(ECON_ADMIN_ROLE_IDS)

        if staff_roles:
            if any(r.id in staff_roles for r in getattr(member, "roles", [])):
                return True
            raise app_commands.CheckFailure("You don’t have permission to use that command.")

        perms = getattr(member, "guild_permissions", None)
        ok = bool(perms and (perms.administrator or perms.manage_guild))
        if ok:
            return True

        raise app_commands.CheckFailure(
            "Staff roles are not configured yet, and you don’t have admin permissions. "
            "Ask an admin to set STAFF_ROLE_IDS in Railway Variables."
        )

    return app_commands.check(predicate)

# -------------------------
# Command help (admin)
# -------------------------
COMMAND_HELP_LINES = [
    ("balance", "Show a character’s current money and owned assets. Anyone can use this; it’s also what the public bank dashboard reflects."),
    ("income", "Claim daily income for one of YOUR characters (once per day, Chicago time). Adds base income plus any income granted by owned assets."),
    ("econ_adjust", "Staff-only. Add or subtract money from a character. The bot will not allow balances to go negative."),
    ("econ_set_balance", "Staff-only. Set a character’s balance to an exact amount (non-negative). Useful for corrections."),
    ("econ_purchase", "Staff-only. Record a NEW asset purchase or UPGRADE an existing owned asset. Charges the full listed cost and updates asset income."),
    ("econ_commands", "Staff-only. Shows this command list with short descriptions (kept up to date as we add features)."),
]


@tree.command(name="econ_commands", description="Staff: show EconBot command list and what each command does.")
@require_admin()
async def econ_help_cmd(interaction: discord.Interaction):
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

    # Sync commands to the test guild only (fast iteration)
    guild_obj = discord.Object(id=GUILD_ID)
    try:
        tree.copy_global_to(guild=guild_obj)
        await tree.sync(guild=guild_obj)
    except Exception as e:
        print(f"[{VERSION}] Command sync failed: {e}")

    print(f"[test] Starting {VERSION}…")
    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID or 'GLOBAL'}; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")

    # Initial dashboard refresh
    asyncio.create_task(rebuild_dashboard())


def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
