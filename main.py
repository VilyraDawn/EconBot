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
VERSION = "EconBot_v22"

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


def _req(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return str(v).strip()


DISCORD_TOKEN = _req("DISCORD_TOKEN")
DATABASE_URL = _req("DATABASE_URL")

GUILD_ID = int(_req("GUILD_ID"))
LEGACY_SOURCE_GUILD_ID = int(_req("LEGACY_SOURCE_GUILD_ID"))

BANK_CHANNEL_ID = int(_req("BANK_CHANNEL_ID"))
ECON_LOG_CHANNEL_ID = int(_req("ECON_LOG_CHANNEL_ID"))

BANK_MESSAGE_IDS = [int(x.strip()) for x in _req("BANK_MESSAGE_IDS").split(",") if x.strip().isdigit()]
if not BANK_MESSAGE_IDS:
    raise RuntimeError("BANK_MESSAGE_IDS must contain at least one message id")

ECON_ADMIN_ROLE_IDS = {int(x.strip()) for x in _req("ECON_ADMIN_ROLE_IDS").split(",") if x.strip().isdigit()}
if not ECON_ADMIN_ROLE_IDS:
    raise RuntimeError("ECON_ADMIN_ROLE_IDS must contain at least one role id")


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


def is_admin_member(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False
    m = interaction.guild.get_member(interaction.user.id)
    if m is None:
        return False
    return any(r.id in ECON_ADMIN_ROLE_IDS for r in m.roles)


def require_admin():
    async def predicate(interaction: discord.Interaction) -> bool:
        return is_admin_member(interaction)
    return app_commands.check(predicate)


async def send_log(message: str):
    try:
        ch = client.get_channel(ECON_LOG_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            await ch.send(message, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass


# -------------------------
# Autocomplete
# -------------------------
async def character_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    try:
        pool = await get_pool()
        async with pool.acquire() as con:
            chars = await search_legacy_characters(con, current, limit=25)
        return [app_commands.Choice(name=c.name, value=c.name) for c in chars]
    except Exception:
        return []


async def owned_asset_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    try:
        # Need character selection to filter owned assets
        selected_character = None
        for opt in interaction.data.get("options", []):
            if opt.get("name") == "character":
                selected_character = opt.get("value")
                break
        if not selected_character:
            return []
        pool = await get_pool()
        async with pool.acquire() as con:
            legacy = await fetch_legacy_character_by_name(con, str(selected_character))
            if not legacy:
                return []
            rows = await con.fetch(
                """
                SELECT asset_name
                FROM economy.character_assets
                WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
                  AND asset_name ILIKE $4
                ORDER BY asset_name ASC
                LIMIT 25
                """,
                interaction.guild.id if interaction.guild else GUILD_ID,
                legacy.name,
                legacy.user_id,
                f"%{current}%",
            )
        return [app_commands.Choice(name=str(r["asset_name"]), value=str(r["asset_name"])) for r in rows]
    except Exception:
        return []


async def asset_type_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    try:
        pool = await get_pool()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT DISTINCT secondary_type
                FROM economy.asset_catalog
                WHERE secondary_type ILIKE $1
                ORDER BY secondary_type ASC
                LIMIT 25
                """,
                f"%{current}%",
            )
        return [app_commands.Choice(name=str(r["secondary_type"]), value=str(r["secondary_type"])) for r in rows]
    except Exception:
        return []


async def tier_name_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    try:
        selected_type = None
        for opt in interaction.data.get("options", []):
            if opt.get("name") == "asset_type":
                selected_type = opt.get("value")
                break
        if not selected_type:
            return []
        pool = await get_pool()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT tier_name
                FROM economy.asset_catalog
                WHERE secondary_type=$1 AND tier_name ILIKE $2
                ORDER BY tier ASC
                LIMIT 25
                """,
                str(selected_type),
                f"%{current}%",
            )
        return [app_commands.Choice(name=str(r["tier_name"]), value=str(r["tier_name"])) for r in rows]
    except Exception:
        return []


# -------------------------
# Embeds / Dashboard
# -------------------------
def build_balance_embed(character_name: str, balance_val: int, assets: List[Tuple[str, str]]):
    em = discord.Embed(title=character_name)
    em.add_field(name="Balance", value=format_denoms(balance_val), inline=False)

    if assets:
        lines = [f"{n} - {tier}" for (n, tier) in assets]
        assets_text = "\n".join(lines)
        em.add_field(name="__*Assets*__", value=assets_text, inline=False)
    else:
        em.add_field(name="__*Assets*__", value="None", inline=False)
    em.set_footer(text=VERSION)
    return em


async def rebuild_dashboard():
    """
    Updates the Bank of Vilyra messages with public balances & assets.
    """
    pool = await get_pool()
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        return

    bank_channel = client.get_channel(BANK_CHANNEL_ID)
    if not isinstance(bank_channel, discord.TextChannel):
        return

    async with pool.acquire() as con:
        # We only show characters that have a balance row OR at least one asset.
        rows = await con.fetch(
            """
            WITH chars AS (
              SELECT b.guild_id, b.character_name, b.character_user_id, b.balance_val
              FROM economy.balances b
              WHERE b.guild_id=$1
              UNION
              SELECT a.guild_id, a.character_name, a.character_user_id, 0::bigint AS balance_val
              FROM economy.character_assets a
              WHERE a.guild_id=$1
            )
            SELECT DISTINCT guild_id, character_name, character_user_id,
                (SELECT balance_val FROM economy.balances b2
                 WHERE b2.guild_id=chars.guild_id AND b2.character_name=chars.character_name AND b2.character_user_id=chars.character_user_id
                ) AS balance_val
            FROM chars
            ORDER BY character_name ASC
            """,
            GUILD_ID,
        )

        # Build embeds in pages
        embeds: List[discord.Embed] = []
        for r in rows:
            cname = str(r["character_name"])
            cuid = int(r["character_user_id"])
            bal = int(r["balance_val"] or 0)

            asset_rows = await con.fetch(
                """
                SELECT asset_name, tier_name
                FROM economy.character_assets
                WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
                ORDER BY asset_name ASC
                """,
                GUILD_ID, cname, cuid
            )
            assets = [(str(a["asset_name"]), str(a["tier_name"])) for a in asset_rows]
            embeds.append(build_balance_embed(cname, bal, assets))

        # Split into chunks per message (Discord embed limit per message: up to 10)
        chunks = [embeds[i:i+10] for i in range(0, len(embeds), 10)]
        # Ensure we have at least as many message slots as chunks; we will only fill up to len(BANK_MESSAGE_IDS)
        for idx, msg_id in enumerate(BANK_MESSAGE_IDS):
            try:
                msg = await bank_channel.fetch_message(msg_id)
            except Exception:
                continue

            if idx < len(chunks):
                await msg.edit(content="**Bank of Vilyra**", embeds=chunks[idx], allowed_mentions=discord.AllowedMentions.none())
            else:
                # clear extra messages if any
                await msg.edit(content="**Bank of Vilyra**", embeds=[], allowed_mentions=discord.AllowedMentions.none())


# -------------------------
# Commands
# -------------------------
@tree.command(name="balance", description="Show a character's balance and assets.")
@app_commands.describe(character="Character name")
@app_commands.autocomplete(character=character_autocomplete)
async def balance_cmd(interaction: discord.Interaction, character: str):
    await interaction.response.defer(thinking=False, ephemeral=True)
    pool = await get_pool()
    async with pool.acquire() as con:
        legacy = await fetch_legacy_character_by_name(con, character)
        if not legacy:
            await interaction.followup.send("Character not found in Legacy records.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        bal = await get_balance_val(con, GUILD_ID, legacy.name, legacy.user_id)
        asset_rows = await con.fetch(
            """
            SELECT asset_name, tier_name
            FROM economy.character_assets
            WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
            ORDER BY asset_name ASC
            """,
            GUILD_ID, legacy.name, legacy.user_id
        )
        assets = [(str(a["asset_name"]), str(a["tier_name"])) for a in asset_rows]
        em = build_balance_embed(legacy.name, bal, assets)

    await interaction.followup.send(embed=em, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
    # also update dashboard opportunistically
    asyncio.create_task(rebuild_dashboard())


@tree.command(name="income", description="Claim daily income for one of your characters (once per day).")
@app_commands.describe(character="Character name")
@app_commands.autocomplete(character=character_autocomplete)
async def income_cmd(interaction: discord.Interaction, character: str):
    await interaction.response.defer(thinking=False, ephemeral=True)
    today = now_chicago_date()

    pool = await get_pool()
    async with pool.acquire() as con:
        legacy = await fetch_legacy_character_by_name(con, character)
        if not legacy:
            await interaction.followup.send("Character not found in Legacy records.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        # Owner-only
        if legacy.user_id != interaction.user.id:
            await interaction.followup.send("You can only claim income for your own characters.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        async with con.transaction():
            # Prevent race conditions
            await con.execute("SELECT pg_advisory_xact_lock($1)", int(hash(f"{GUILD_ID}:{legacy.name}:{legacy.user_id}") & 0x7FFFFFFF))

            last = await con.fetchval(
                """
                SELECT last_claim_date
                FROM economy.income_claims
                WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
                """,
                GUILD_ID, legacy.name, legacy.user_id
            )
            if last is not None and isinstance(last, dt.date) and last >= today:
                await interaction.followup.send("Income already claimed today for this character.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                return

            asset_income = await sum_asset_income(con, GUILD_ID, legacy.name, legacy.user_id)
            total = int(BASE_DAILY_INCOME_VAL + asset_income)

            ok, new_bal = await adjust_balance_val_guarded(con, GUILD_ID, legacy.name, legacy.user_id, total)
            if not ok:
                # This should never happen for positive income, but keep the guard.
                await interaction.followup.send("Could not apply income (balance guard).", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                return

            await con.execute(
                """
                INSERT INTO economy.income_claims (guild_id, character_name, character_user_id, last_claim_date)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (guild_id, character_name, character_user_id)
                DO UPDATE SET last_claim_date=EXCLUDED.last_claim_date
                """,
                GUILD_ID, legacy.name, legacy.user_id, today
            )

            await log_tx(
                con,
                GUILD_ID,
                legacy.name,
                legacy.user_id,
                interaction.user.id,
                "INCOME",
                total,
                "Daily income claim",
                details={"base_val": BASE_DAILY_INCOME_VAL, "asset_income_val": asset_income, "date": str(today)},
            )

    await interaction.followup.send(
        f"✅ Income claimed for **{legacy.name}**.\nAdded: **{format_denoms(total)}**\nNew balance: **{format_denoms(new_bal)}**",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    asyncio.create_task(send_log(f"[INCOME] {interaction.user.id} claimed {total} Val for {legacy.name}"))
    asyncio.create_task(rebuild_dashboard())


@tree.command(name="econ_adjust", description="Staff: add/subtract currency from a character (cannot go negative).")
@require_admin()
@app_commands.describe(character="Character name", operation="ADD or SUBTRACT", amount_val="Amount in Val", reason="Reason")
@app_commands.autocomplete(character=character_autocomplete)
async def econ_adjust_cmd(interaction: discord.Interaction, character: str, operation: str, amount_val: int, reason: str):
    await interaction.response.defer(thinking=False, ephemeral=True)
    op = (operation or "").strip().upper()
    if op not in {"ADD", "SUBTRACT"}:
        await interaction.followup.send("Operation must be ADD or SUBTRACT.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return
    if amount_val <= 0:
        await interaction.followup.send("Amount must be a positive integer Val amount.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return
    if not reason.strip():
        await interaction.followup.send("Reason is required.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return

    delta = int(amount_val if op == "ADD" else -amount_val)

    pool = await get_pool()
    async with pool.acquire() as con:
        legacy = await fetch_legacy_character_by_name(con, character)
        if not legacy:
            await interaction.followup.send("Character not found in Legacy records.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock($1)", int(hash(f"{GUILD_ID}:{legacy.name}:{legacy.user_id}") & 0x7FFFFFFF))

            ok, new_bal = await adjust_balance_val_guarded(con, GUILD_ID, legacy.name, legacy.user_id, delta)
            if not ok:
                await interaction.followup.send("❌ Insufficient funds. This adjustment would make the balance negative.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                return

            await log_tx(
                con,
                GUILD_ID,
                legacy.name,
                legacy.user_id,
                interaction.user.id,
                "ADJUST",
                delta,
                reason,
                details={"operation": op, "amount_val": amount_val},
            )

    await interaction.followup.send(
        f"✅ Adjustment applied to **{legacy.name}**.\nDelta: **{format_denoms(delta if delta>0 else -delta)}** ({op})\nNew balance: **{format_denoms(new_bal)}**",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    asyncio.create_task(send_log(f"[ADJUST] {interaction.user.id} {op} {amount_val} Val for {legacy.name}. Reason: {reason}"))
    asyncio.create_task(rebuild_dashboard())


@tree.command(name="econ_set_balance", description="Staff: set a character's balance exactly (non-negative).")
@require_admin()
@app_commands.describe(character="Character name", new_balance_val="New balance in Val", reason="Reason")
@app_commands.autocomplete(character=character_autocomplete)
async def econ_set_balance_cmd(interaction: discord.Interaction, character: str, new_balance_val: int, reason: str):
    await interaction.response.defer(thinking=False, ephemeral=True)
    if new_balance_val < 0:
        await interaction.followup.send("New balance cannot be negative.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return
    if not reason.strip():
        await interaction.followup.send("Reason is required.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return

    pool = await get_pool()
    async with pool.acquire() as con:
        legacy = await fetch_legacy_character_by_name(con, character)
        if not legacy:
            await interaction.followup.send("Character not found in Legacy records.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock($1)", int(hash(f"{GUILD_ID}:{legacy.name}:{legacy.user_id}") & 0x7FFFFFFF))

            current = await get_balance_val(con, GUILD_ID, legacy.name, legacy.user_id)
            delta = int(new_balance_val) - int(current)
            await set_balance_val(con, GUILD_ID, legacy.name, legacy.user_id, int(new_balance_val))
            await log_tx(
                con,
                GUILD_ID,
                legacy.name,
                legacy.user_id,
                interaction.user.id,
                "SET_BALANCE",
                delta,
                reason,
                details={"old_balance_val": current, "new_balance_val": int(new_balance_val)},
            )

    await interaction.followup.send(
        f"✅ Balance set for **{legacy.name}**.\nNew balance: **{format_denoms(int(new_balance_val))}**",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )
    asyncio.create_task(send_log(f"[SET_BALANCE] {interaction.user.id} set {legacy.name} to {new_balance_val} Val. Reason: {reason}"))
    asyncio.create_task(rebuild_dashboard())


@tree.command(name="econ_purchase", description="Staff: purchase a new asset or upgrade an existing one.")
@require_admin()
@app_commands.describe(
    character="Character name",
    action="NEW or UPGRADE",
    asset_type="Property/asset type (from the asset table)",
    tier_name="Tier name (from the asset table)",
    asset_name="Asset name (required for NEW)",
    upgrade_asset="Owned asset name to upgrade (required for UPGRADE)",
    reason="Reason",
)
@app_commands.autocomplete(
    character=character_autocomplete,
    asset_type=asset_type_autocomplete,
    tier_name=tier_name_autocomplete,
    upgrade_asset=owned_asset_autocomplete,
)
async def econ_purchase_cmd(
    interaction: discord.Interaction,
    character: str,
    action: str,
    asset_type: str,
    tier_name: str,
    asset_name: str = "",
    upgrade_asset: str = "",
    reason: str = "",
):
    await interaction.response.defer(thinking=False, ephemeral=True)
    act = (action or "").strip().upper()
    if act not in {"NEW", "UPGRADE"}:
        await interaction.followup.send("Action must be NEW or UPGRADE.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return
    if not reason.strip():
        await interaction.followup.send("Reason is required.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return
    if not asset_type.strip() or not tier_name.strip():
        await interaction.followup.send("Asset type and tier name are required.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        return

    pool = await get_pool()
    async with pool.acquire() as con:
        legacy = await fetch_legacy_character_by_name(con, character)
        if not legacy:
            await interaction.followup.send("Character not found in Legacy records.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        # Look up catalog row EXACTLY by (secondary_type, tier_name)
        cat = await con.fetchrow(
            """
            SELECT asset_type, secondary_type, tier, tier_name, cost_val, income_val
            FROM economy.asset_catalog
            WHERE secondary_type=$1 AND tier_name=$2
            LIMIT 1
            """,
            str(asset_type),
            str(tier_name),
        )
        if not cat:
            await interaction.followup.send("That asset type / tier name combination was not found in the asset table.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        desired_tier = int(cat["tier"])
        cat_asset_type = str(cat["asset_type"])
        cat_secondary_type = str(cat["secondary_type"])
        cat_tier_name = str(cat["tier_name"])
        cost_val = int(cat["cost_val"])
        income_val = int(cat["income_val"])

        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock($1)", int(hash(f"{GUILD_ID}:{legacy.name}:{legacy.user_id}") & 0x7FFFFFFF))

            # Funds check + debit
            current_bal = await get_balance_val(con, GUILD_ID, legacy.name, legacy.user_id)
            if current_bal < cost_val:
                await interaction.followup.send("❌ Insufficient funds for this transaction.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                return

            if act == "NEW":
                asset_name = (asset_name or "").strip()
                if not asset_name:
                    await interaction.followup.send("Asset name is required for NEW purchases.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                # Insert asset (unique per (guild, character, user, asset_name))
                try:
                    await con.execute(
                        """
                        INSERT INTO economy.character_assets
                          (guild_id, character_name, character_user_id, asset_name, asset_type, secondary_type, tier, tier_name, cost_val, income_val)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        """,
                        GUILD_ID, legacy.name, legacy.user_id, asset_name,
                        cat_asset_type, cat_secondary_type, desired_tier, cat_tier_name, cost_val, income_val
                    )
                except asyncpg.UniqueViolationError:
                    await interaction.followup.send("That asset name already exists for this character. Choose a different name.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                ok, new_bal = await adjust_balance_val_guarded(con, GUILD_ID, legacy.name, legacy.user_id, -cost_val)
                if not ok:
                    await interaction.followup.send("❌ Insufficient funds (balance guard).", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                await log_tx(
                    con, GUILD_ID, legacy.name, legacy.user_id, interaction.user.id,
                    "PURCHASE", -cost_val, reason,
                    details={"action": "NEW", "asset_name": asset_name, "secondary_type": cat_secondary_type, "tier": desired_tier, "tier_name": cat_tier_name, "cost_val": cost_val, "income_val": income_val}
                )

                await interaction.followup.send(
                    f"✅ Purchased **{asset_name}** for **{legacy.name}**.\nTier: **{cat_tier_name}**\nCost: **{format_denoms(cost_val)}**\nNew balance: **{format_denoms(new_bal)}**",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

            else:
                # UPGRADE
                upgrade_asset = (upgrade_asset or "").strip()
                if not upgrade_asset:
                    await interaction.followup.send("Select an owned asset to upgrade.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                owned = await con.fetchrow(
                    """
                    SELECT asset_name, secondary_type, tier, tier_name
                    FROM economy.character_assets
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND asset_name=$4
                    """,
                    GUILD_ID, legacy.name, legacy.user_id, upgrade_asset
                )
                if not owned:
                    await interaction.followup.send("That owned asset was not found for this character.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                current_tier = int(owned["tier"])
                owned_type = str(owned["secondary_type"])

                if owned_type != cat_secondary_type:
                    await interaction.followup.send("Upgrade tier must be from the same property/asset type.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return
                if desired_tier <= current_tier:
                    await interaction.followup.send("Upgrade tier must be higher than the current tier.", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                # Apply upgrade (no rename)
                await con.execute(
                    """
                    UPDATE economy.character_assets
                    SET tier=$1, tier_name=$2, cost_val=$3, income_val=$4, updated_at=NOW()
                    WHERE guild_id=$5 AND character_name=$6 AND character_user_id=$7 AND asset_name=$8
                    """,
                    desired_tier, cat_tier_name, cost_val, income_val,
                    GUILD_ID, legacy.name, legacy.user_id, upgrade_asset
                )

                ok, new_bal = await adjust_balance_val_guarded(con, GUILD_ID, legacy.name, legacy.user_id, -cost_val)
                if not ok:
                    await interaction.followup.send("❌ Insufficient funds (balance guard).", ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
                    return

                await log_tx(
                    con, GUILD_ID, legacy.name, legacy.user_id, interaction.user.id,
                    "UPGRADE", -cost_val, reason,
                    details={"action": "UPGRADE", "asset_name": upgrade_asset, "secondary_type": cat_secondary_type, "from_tier": current_tier, "to_tier": desired_tier, "tier_name": cat_tier_name, "cost_val": cost_val}
                )

                await interaction.followup.send(
                    f"✅ Upgraded **{upgrade_asset}** for **{legacy.name}**.\nNew tier: **{cat_tier_name}**\nCost: **{format_denoms(cost_val)}**\nNew balance: **{format_denoms(new_bal)}**",
                    ephemeral=True,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

    asyncio.create_task(send_log(f"[PURCHASE] {interaction.user.id} {act} {asset_type} / {tier_name} for {character}. Cost {cost_val}. Reason: {reason}"))
    asyncio.create_task(rebuild_dashboard())



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
    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID}; legacy source guild: {LEGACY_SOURCE_GUILD_ID})")

    # Initial dashboard refresh
    asyncio.create_task(rebuild_dashboard())


def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
