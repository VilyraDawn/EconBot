# EconBot_v109 — Clean rebuild (guild-only commands, legacy DB partition, bank message IDs persisted in Postgres)
# NOTE: This is a full replacement for main.py (Railway runs /app/main.py).
# Constraints honored:
# - Character-based economy (not user-based)
# - No invented env vars (uses only the approved set)
# - No invented DB tables EXCEPT the explicitly approved bank message persistence table (econ_bank_messages)
# - No secondary_type, no ECON_ADMIN_ROLE_IDS, no duplicate purchase commands, no global slash sync (guild-only)
# - All slash commands defer immediately

from __future__ import annotations

import os
import json
import re
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

import discord
from discord import app_commands

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

try:
    import asyncpg
except Exception as e:
    raise RuntimeError("asyncpg is required for EconBot") from e


APP_VERSION = "EconBot_v111"

# Canon kingdoms (authoritative list for tax dropdowns & treasury seeding)
CANON_KINGDOMS: list[str] = ["Sethrathiel", "Velarith", "Lyvik", "Baelon", "Avalea"]
DEFAULT_KINGDOM_TAX_BP = 1000  # 10%
CHICAGO_TZ = ZoneInfo("America/Chicago") if ZoneInfo else timezone.utc


# -------------------------

# --- Internal throttles (prevent rate-limit cascades) ---
_STAFF_MEMBER_FETCH_LOCK = asyncio.Lock()

# Env helpers (NO inventions)
# -------------------------

def _get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default


def _int(name: str, default: Optional[int] = None) -> Optional[int]:
    raw = _get(name)
    if raw is None:
        return default
    digits = re.sub(r"[^0-9]", "", raw)
    if digits == "":
        return default
    try:
        return int(digits)
    except Exception:
        return default


def _int_list(name: str) -> List[int]:
    raw = _get(name, "")
    parts = [p.strip() for p in str(raw).replace("\n", ",").replace(";", ",").split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        digits = re.sub(r"[^0-9]", "", p)
        if digits:
            try:
                out.append(int(digits))
            except Exception:
                pass
    # de-dupe stable
    return sorted(list(dict.fromkeys(out)))


def _tier_rank(tier: str) -> Optional[int]:
    """Extract numeric rank from tier labels like '(3) Small Tavern'."""
    if tier is None:
        return None
    s = str(tier).strip()
    m = re.match(r"^\(\s*(\d+)\s*\)", s)
    if not m:
        m = re.match(r"^(\d+)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


async def cumulative_cost_to_tier(asset_type: str, target_tier: str) -> Optional[int]:
    """Cumulative cost: sum of all tier costs up to and including target tier for an asset type.

    Requires tiers to be ordered by a numeric rank embedded in the tier label, e.g. '(1) ...', '(2) ...'.
    Falls back to the target tier's own cost if ranks are not parseable.
    """
    target_rank = _tier_rank(target_tier)

    rows = await db_fetch(
        '''
        SELECT tier, cost_val
        FROM econ_asset_definitions
        WHERE asset_type=$1;
        ''',
        asset_type,
    )
    if not rows:
        return None

    # Normalize tier strings once
    norm_rows = []
    for r in rows:
        t = str(r["tier"]).strip()
        try:
            c = int(r["cost_val"])
        except Exception:
            continue
        norm_rows.append((t, c, _tier_rank(t)))

    # If the selected tier has a rank, sum all rows with rank <= target_rank
    if target_rank is not None:
        total = 0
        found = False
        for t, c, rk in norm_rows:
            if rk is None:
                continue
            if rk <= target_rank:
                total += c
                found = True
        if found:
            return total

    # Fallback: just use the selected tier's own cost
    target_norm = str(target_tier).strip()
    for t, c, rk in norm_rows:
        if t == target_norm:
            return c

    return None



def format_currency(total_cinth: int) -> str:
    """Compact currency with roll-up to highest denominations, dropping zeros, plus raw total."""
    try:
        total = int(total_cinth)
    except Exception:
        total = 0

    sign = "-" if total < 0 else ""
    n = abs(total)

    denominations = [
        (10000, "Novir"),
        (1000, "Oril"),
        (100, "Elsh"),
        (10, "Arce"),
        (1, "Cinth"),
    ]

    parts: List[str] = []
    for value, short in denominations:
        if n <= 0:
            break
        qty, n = divmod(n, value)
        if qty:
            parts.append(f"{qty} {short}")

    if not parts:
        parts = ["0 Cinth"]

    compact = " • ".join(parts)
    return f"{sign}{compact} (Total: {total:,} Copper Cinth)"

# Base daily income granted on /income claim (in Copper Cinth units)
BASE_DAILY_INCOME = 10




async def tier_cost_for(asset_type: str, tier: str) -> Optional[int]:
    """Return cost_val for the exact tier (NOT cumulative)."""
    row = await db_fetchrow(
        '''
        SELECT cost_val
        FROM econ_asset_definitions
        WHERE asset_type=$1 AND tier=$2
        LIMIT 1;
        ''',
        asset_type,
        tier,
    )
    if not row:
        return None
    try:
        return int(row["cost_val"])
    except Exception:
        return None


async def incremental_cost_between_tiers(asset_type: str, current_tier: str, target_tier: str) -> Optional[int]:
    """Upgrade cost from current_tier to target_tier for an asset_type.

    Rule: sum costs of tiers strictly above current tier through the target tier.
    Fallback: cumulative(target) - cumulative(current) if rank-based sum cannot be computed.
    """
    cur_rank = _tier_rank(current_tier)
    tgt_rank = _tier_rank(target_tier)
    if tgt_rank is None:
        return None

    rows = await db_fetch(
        '''
        SELECT tier, cost_val
        FROM econ_asset_definitions
        WHERE asset_type=$1;
        ''',
        asset_type,
    )
    if not rows:
        return None

    if cur_rank is not None:
        total = 0
        any_found = False
        for r in rows:
            t = str(r["tier"])
            tr = _tier_rank(t)
            if tr is None:
                continue
            if tr > cur_rank and tr <= tgt_rank:
                any_found = True
                total += int(r["cost_val"])
        if any_found:
            return int(total)

    cum_t = await cumulative_cost_to_tier(asset_type, target_tier)
    cum_c = await cumulative_cost_to_tier(asset_type, current_tier)
    if cum_t is None or cum_c is None:
        return None
    return int(cum_t - cum_c)

async def get_assets_for_character(character_name: str) -> List[Dict[str, Any]]:
    """Return full asset rows for a character."""
    rows = await db_fetch(
        '''
        SELECT asset_type, tier, asset_name
        FROM econ_assets
        WHERE guild_id=$1 AND character_name=$2
        ''',
        DATA_GUILD_ID,
        character_name,
    )
    # sort by asset_type, then tier rank, then name
    def _key(r: Dict[str, Any]) -> Tuple[str, int, str]:
        t = str(r.get("tier", ""))
        rk = _tier_rank(t)
        if rk is None:
            rk = 9999
        return (str(r.get("asset_type", "")), rk, str(r.get("asset_name", "")))
    return sorted(rows, key=_key)


async def render_character_card(
    guild: discord.Guild,
    character_name: str,
    owner_id: Optional[int] = None,
) -> List[str]:
    """Render a single character card (no pings). Returns list of lines."""
    if owner_id is None:
        owner_id = await get_character_owner(character_name)

    owner_display = "Unknown"
    if owner_id is not None:
        m = guild.get_member(int(owner_id))
        # Do not fetch members (avoids rate limits); rely on cache.
        owner_display = m.display_name if m else f"User {owner_id}"

    bal = await get_balance(character_name)
    inc = await recompute_daily_income(character_name)
    assets = await get_assets_for_character(character_name)
    kingdom = await get_character_kingdom(character_name)

    out: List[str] = []
    out.append("━━━━━━━━━━━━━━━━━━")
    out.append(f"**{character_name}**  ·  *{owner_display}*")
    if kingdom:
        out.append(f"🏰 **Kingdom:** {kingdom}")
    out.append(f"💰 **Balance:** {format_currency(bal)}")
    out.append(f"🌙 **Daily Income:** {format_currency(inc)}")

    if assets:
        out.append(f"🏷️ **Assets ({len(assets)}):**")
        for a in assets:
            out.append(f"- {a['asset_type']} | {a['tier']} | {a['asset_name']}")
    else:
        out.append("🏷️ **Assets:** (none)")

    out.append("")  # spacer
    return out


async def render_leaderboard_lines(
    guild: discord.Guild,
    rows: List[Tuple[str, int, int, int]],
) -> List[str]:
    """rows: [(character_name, owner_id, balance, income)]"""
    # Top balances
    top_bal = sorted(rows, key=lambda r: int(r[2]), reverse=True)[:10]
    top_inc = sorted(rows, key=lambda r: int(r[3]), reverse=True)[:10]

    # resolve owner display names (no pings)
    cache: Dict[int, str] = {}
    async def dn(uid: int) -> str:
        if uid in cache:
            return cache[uid]
        m = guild.get_member(uid)
        # Do not fetch members (avoids rate limits); rely on cache only.
        cache[uid] = m.display_name if m else f"User {uid}"
        return cache[uid]

    out: List[str] = []
    out.append("🏆 **Leaderboards**")
    out.append("━━━━━━━━━━━━━━━━━━")
    out.append("**Top Balances**")
    if not top_bal:
        out.append("- (none)")
    else:
        for i, (c, uid, bal, inc) in enumerate(top_bal, start=1):
            out.append(f"{i}. **{c}** — *{await dn(uid)}* — {format_currency(bal)}")
    out.append("")
    out.append("**Top Daily Income**")
    if not top_inc:
        out.append("- (none)")
    else:
        for i, (c, uid, bal, inc) in enumerate(top_inc, start=1):
            out.append(f"{i}. **{c}** — *{await dn(uid)}* — {format_currency(inc)}")
    out.append("")
    return out


# Approved env vars (per continuity doc)
DISCORD_TOKEN = _get("DISCORD_TOKEN")
DATABASE_URL = _get("DATABASE_URL")
GUILD_ID = _int("GUILD_ID")
LEGACY_SOURCE_GUILD_ID = _int("LEGACY_SOURCE_GUILD_ID", GUILD_ID)
BANK_CHANNEL_ID = _int("BANK_CHANNEL_ID")
ECON_LOG_CHANNEL_ID = _int("ECON_LOG_CHANNEL_ID")
STAFF_ROLE_IDS = set(_int_list("STAFF_ROLE_IDS"))
STAFF_ROLE_IDS_DEFAULT = {1473523681132019824, 1473523738891784232}  # fallback if env missing
if not STAFF_ROLE_IDS:
    STAFF_ROLE_IDS = set(STAFF_ROLE_IDS_DEFAULT)

BANK_REFRESH_ROLE_IDS = set(_int_list("BANK_REFRESH_ROLE_IDS"))
# BANK_MESSAGE_IDS is kept for backward compatibility, but v72 persists message IDs in Postgres (approved)
BANK_MESSAGE_IDS = _int_list("BANK_MESSAGE_IDS")

if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")
if not GUILD_ID:
    raise RuntimeError("Missing/invalid GUILD_ID")
if not LEGACY_SOURCE_GUILD_ID:
    raise RuntimeError("Missing/invalid LEGACY_SOURCE_GUILD_ID (or GUILD_ID)")

# All DB reads/writes (balances/assets/income/characters) use the legacy guild partition, per your confirmation.
DATA_GUILD_ID = int(LEGACY_SOURCE_GUILD_ID)



# -------------------------
# Asset definitions seed (authoritative from NEW Asset Table.xlsx)
# -------------------------
ASSET_DEFINITIONS_SEED: List[Tuple[str, str, int, int]] = [
    ("Guild Trade Workshop", "(1) Guild Apprentice", 300, 50),
    ("Guild Trade Workshop", "(2) Guild Journeyman", 600, 100),
    ("Guild Trade Workshop", "(3) Leased Workshop", 1200, 150),
    ("Guild Trade Workshop", "(4) Small Workshop", 2000, 200),
    ("Guild Trade Workshop", "(5) Large Workshop", 3000, 250),
    ("Market Stall", "(1) Consignment Arrangement", 300, 50),
    ("Market Stall", "(2) Small Alley Stand", 600, 100),
    ("Market Stall", "(3) Market Stall", 1200, 150),
    ("Market Stall", "(4) Small Shop", 2000, 200),
    ("Market Stall", "(5) Large Shop", 3000, 250),
    ("Farm/Ranch", "(1) Subsistence Surplus", 300, 50),
    ("Farm/Ranch", "(2) Leased Fields", 600, 100),
    ("Farm/Ranch", "(3) Owned Acre", 1200, 150),
    ("Farm/Ranch", "(4) Small Fields and Barn", 2000, 200),
    ("Farm/Ranch", "(5) Large Fields and Barn", 3000, 250),
    ("Tavern/Inn", "(1) One-Room Flophouse", 300, 50),
    ("Tavern/Inn", "(2) Leased Establishment", 600, 100),
    ("Tavern/Inn", "(3) Small Tavern", 1200, 150),
    ("Tavern/Inn", "(4) Large Tavern", 2000, 200),
    ("Tavern/Inn", "(5) Large Tavern and Inn", 3000, 250),
    ("Warehouse/Trade House", "(1) Small Storage Shed", 300, 50),
    ("Warehouse/Trade House", "(2) Large Storage Shed", 600, 100),
    ("Warehouse/Trade House", "(3) Small Trading Post", 1200, 150),
    ("Warehouse/Trade House", "(4) Large Trading Post", 2000, 200),
    ("Warehouse/Trade House", "(5) Large Warehouse and Trading Post", 3000, 250),
    ("House", "(1) Shack", 600, 0),
    ("House", "(2) Hut", 1200, 0),
    ("House", "(3) House", 2000, 0),
    ("House", "(4) Lodge", 3000, 0),
    ("House", "(5) Mansion", 5000, 0),
    ("Village", "(1) Chartered Assembly", 1200, 100),
    ("Village", "(2) Hamlet", 2400, 200),
    ("Village", "(3) Village", 4800, 300),
    ("Village", "(4) Town", 9600, 400),
    ("Village", "(5) Small City", 15000, 500),
    ("Weapons", "(1) Hit +1 / Dmg +1d4", 300, 0),
    ("Weapons", "(2) Hit +1 / Dmg +1d6", 600, 0),
    ("Weapons", "(3) Hit +2 / Dmg +1d8", 1200, 0),
    ("Weapons", "(4) Hit +2 / Dmg +1d10", 2400, 0),
    ("Weapons", "(5) Hit +2 / Dmg +1d12", 4800, 0),
    ("Armor", "(1) AC +1", 300, 0),
    ("Armor", "(2) AC +2", 600, 0),
    ("Armor", "(3) AC +2 / Adv Magic Atk", 1200, 0),
    ("Armor", "(4) AC +2 / Adv Magic and Melee Atk", 2400, 0),
    ("Armor", "(5) AC +3 / Adv Magic and Melee Atk", 4800, 0),
]

# -------------------------
# Discord client setup
# -------------------------

intents = discord.Intents.default()
intents.members = True  # required for role detection
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# -------------------------
# DB
# -------------------------

_POOL: Optional["asyncpg.Pool"] = None


async def db_pool() -> "asyncpg.Pool":
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, command_timeout=30)
    return _POOL


async def db_exec(sql: str, *args) -> str:
    pool = await db_pool()
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


async def db_fetch(sql: str, *args) -> List[asyncpg.Record]:
    pool = await db_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def db_fetchrow(sql: str, *args) -> Optional[asyncpg.Record]:
    pool = await db_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


# -------------------------
# Migrations (minimal, explicit)
# -------------------------
# ONLY new table approved by you:
# econ_bank_messages (to persist bank dashboard message IDs instead of env var spam)


async def ensure_schema() -> None:
    # Core tables expected to already exist (v38 doctrine). We do NOT create them here.
    # We *do* create the approved bank-message persistence table.
    await db_exec(
        """
        CREATE TABLE IF NOT EXISTS econ_bank_messages (
            guild_id BIGINT NOT NULL,
            idx INTEGER NOT NULL,
            message_id BIGINT NOT NULL,
            PRIMARY KEY (guild_id, idx)
        );
        """
    )

    # Kingdom taxation support (explicitly approved):
    # - characters.kingdom (required for income claims; populated by upstream character-creation bot)
    # - econ_assets.kingdom (optional override; if NULL, inherits character home kingdom)
    # - econ_kingdoms treasury + tax rates (basis points)
    try:
        await db_exec("ALTER TABLE characters ADD COLUMN IF NOT EXISTS kingdom TEXT;")
    except Exception as e:
        print(f"[warn] Could not add characters.kingdom (will require manual migration): {e}")

    try:
        await db_exec("ALTER TABLE econ_assets ADD COLUMN IF NOT EXISTS kingdom TEXT;")
    except Exception as e:
        print(f"[warn] Could not add econ_assets.kingdom (will require manual migration): {e}")

    await db_exec(
        """
        CREATE TABLE IF NOT EXISTS econ_kingdoms (
            guild_id BIGINT NOT NULL,
            kingdom TEXT NOT NULL,
            tax_rate_bp INT NOT NULL DEFAULT 0,
            treasury BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, kingdom)
        );
        """
    )

    # Seed canonical kingdoms with baseline 10% tax (do not override non-zero rates)
    try:
        for _k in CANON_KINGDOMS:
            await db_exec(
                """
                INSERT INTO econ_kingdoms (guild_id, kingdom, tax_rate_bp, treasury)
                VALUES ($1, $2, $3, 0)
                ON CONFLICT (guild_id, kingdom) DO UPDATE
                SET tax_rate_bp = CASE WHEN econ_kingdoms.tax_rate_bp = 0 THEN EXCLUDED.tax_rate_bp ELSE econ_kingdoms.tax_rate_bp END;
                """,
                DATA_GUILD_ID,
                _k,
                DEFAULT_KINGDOM_TAX_BP,
            )
    except Exception as e:
        print(f"[warn] Could not seed econ_kingdoms baseline rates: {e}")



    # Adjust econ_assets unique constraint so the same asset_name can be reused across different asset_type/tier.
    # Desired uniqueness: (guild_id, user_id, character_name, asset_type, tier, asset_name)
    try:
        rows = await db_fetch(
            '''
            SELECT conname, pg_get_constraintdef(c.oid) AS def
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname = 'econ_assets' AND c.contype = 'u';
            '''
        )
        for r in rows:
            conname = str(r["conname"])
            cdef = str(r["def"]).replace('"', "")
            if "UNIQUE" in cdef and "(guild_id, user_id, character_name, asset_name)" in cdef:
                await db_exec(f'ALTER TABLE econ_assets DROP CONSTRAINT IF EXISTS "{conname}";')
                print(f"[test] Dropped old econ_assets unique constraint: {conname}")

        exists = await db_fetchrow(
            '''
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname='econ_assets' AND c.contype='u' AND c.conname='econ_assets_unique_name_per_type_tier'
            LIMIT 1;
            '''
        )
        if not exists:
            await db_exec(
                '''
                ALTER TABLE econ_assets
                ADD CONSTRAINT econ_assets_unique_name_per_type_tier
                UNIQUE (guild_id, user_id, character_name, asset_type, tier, asset_name);
                '''
            )
            print("[test] Added unique constraint econ_assets_unique_name_per_type_tier")
    except Exception as e:
        print(f"[warn] econ_assets unique-constraint adjustment skipped/failed: {e}")


async def _get_member(interaction: discord.Interaction) -> Optional[discord.Member]:
    """Resolve the invoking member for staff permission checks.

    Priority order:
    1) Use interaction.user if it's already a discord.Member (includes roles).
    2) Use guild cache lookup (no HTTP).
    3) As a last resort, fetch the member over HTTP *once* (serialized) to avoid false denials.
       This is intentionally gated behind a lock to reduce 429s.
    """
    if isinstance(interaction.user, discord.Member):
        return interaction.user

    if interaction.guild is None:
        return None

    try:
        cached = interaction.guild.get_member(interaction.user.id)
        if cached is not None:
            return cached
    except Exception:
        pass

    # Last resort: single serialized HTTP fetch (helps when members intent/caching is insufficient).
    try:
        async with _STAFF_MEMBER_FETCH_LOCK:
            return await interaction.guild.fetch_member(interaction.user.id)
    except Exception:
        return None

    # As a last resort, do NOT fetch over HTTP here; return None and staff gate will explain.
    return None


async def is_staff(interaction: discord.Interaction) -> Tuple[bool, Dict[str, Any]]:
    """Return (is_staff, debug_dict). Uses role IDs and falls back to guild permissions."""
    member = await _get_member(interaction)

    role_ids: List[int] = []
    admin = False
    manage_guild = False
    manage_messages = False

    if member is not None:
        try:
            role_ids = [int(r.id) for r in getattr(member, "roles", [])]
        except Exception:
            role_ids = []
        try:
            perms = getattr(member, "guild_permissions", None)
            if perms is not None:
                admin = bool(getattr(perms, "administrator", False))
                manage_guild = bool(getattr(perms, "manage_guild", False))
                manage_messages = bool(getattr(perms, "manage_messages", False))
        except Exception:
            pass

    allowed = False
    if admin or manage_guild or manage_messages:
        allowed = True

    if not allowed and STAFF_ROLE_IDS and role_ids:
        allowed = any((rid in STAFF_ROLE_IDS) for rid in role_ids)

    debug = {
        "user_id": int(interaction.user.id) if interaction.user else None,
        "guild_id": int(interaction.guild_id) if interaction.guild_id else None,
        "detected_role_ids": sorted(role_ids),
        "configured_staff_role_ids": sorted(list(STAFF_ROLE_IDS)),
        "admin": admin,
        "manage_guild": manage_guild,
        "manage_messages": manage_messages,
    }
    return allowed, debug


def staff_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        ok, dbg = await is_staff(interaction)
        if not ok:
            msg = (
                "You do not have permission to run this staff command.\n\n"
                "This bot is configured for **role-based staff access only**.\n"
                "If you expect access, verify **STAFF_ROLE_IDS** contains your staff role IDs, "
                "and ensure the bot has **Server Members Intent** enabled.\n\n"
                f"--- Debug ---\n"
                f"Your user_id: {dbg['user_id']}\n"
                f"Detected role IDs: {dbg['detected_role_ids']}\n"
                f"Configured STAFF_ROLE_IDS (effective): {dbg['staff_role_ids']}\n"
                f"Guild ID (interaction): {dbg['guild_id']}\n"
            )
            # best-effort ephemeral response
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(msg, ephemeral=True)
                else:
                    await interaction.response.send_message(msg, ephemeral=True)
            except Exception:
                pass
        return ok
    return app_commands.check(predicate)


# -------------------------
# Characters (from Postgres "characters")
# -------------------------

async def character_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    # Pull from Postgres characters table using DATA_GUILD_ID partition
    like = f"%{(current or '').lower()}%"
    rows = await db_fetch(
        """
        SELECT name
        FROM characters
        WHERE guild_id=$1 AND archived=FALSE AND LOWER(name) LIKE $2
        ORDER BY name ASC
        LIMIT 25;
        """,
        DATA_GUILD_ID,
        like,
    )
    return [app_commands.Choice(name=r["name"], value=r["name"]) for r in rows]



async def ac_asset_for_character(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Autocomplete assets for the selected character. Format: 'Type | Tier | Name'."""
    try:
        char = getattr(interaction.namespace, "character", None)
        if not char:
            return []
        rows = await get_assets_for_character(str(char))
        needle = (current or "").lower()
        out: List[app_commands.Choice[str]] = []
        for r in rows:
            label = f"{r['asset_type']} | {r['tier']} | {r['asset_name']}"
            if needle and needle not in label.lower():
                continue
            out.append(app_commands.Choice(name=label[:100], value=label[:100]))
            if len(out) >= 25:
                break
        return out
    except Exception:
        return []


async def ac_target_tier(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Autocomplete higher tiers for the selected asset's type."""
    try:
        asset_label = getattr(interaction.namespace, "asset", None)
        if not asset_label:
            return []
        parts = [p.strip() for p in str(asset_label).split("|")]
        if len(parts) < 3:
            return []
        asset_type = parts[0]
        cur_tier = parts[1]
        cur_rank = _tier_rank(cur_tier)

        tiers = await list_tiers_for_type(asset_type)
        needle = (current or "").lower()
        out: List[app_commands.Choice[str]] = []
        for t in tiers:
            tr = _tier_rank(t)
            if cur_rank is not None and tr is not None and tr <= cur_rank:
                continue
            if needle and needle not in t.lower():
                continue
            out.append(app_commands.Choice(name=t[:100], value=t[:100]))
            if len(out) >= 25:
                break
        return out
    except Exception:
        return []

async def get_character_owner(character_name: str) -> Optional[int]:
    row = await db_fetchrow(
        """
        SELECT user_id
        FROM characters
        WHERE guild_id=$1 AND name=$2 AND archived=FALSE
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        character_name,
    )
    return int(row["user_id"]) if row else None



async def get_character_kingdom(character_name: str) -> Optional[str]:
    row = await db_fetchrow(
        """
        SELECT kingdom
        FROM characters
        WHERE guild_id=$1 AND name=$2 AND archived=FALSE
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        character_name,
    )
    if not row:
        return None
    k = row.get("kingdom")
    if k is None:
        return None
    k = str(k).strip()
    return k if k else None


# -------------------------
# Economy core helpers
# -------------------------

async def get_balance(character_name: str) -> int:
    row = await db_fetchrow(
        """
        SELECT balance_val
        FROM econ_balances
        WHERE guild_id=$1 AND character_name=$2
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        character_name,
    )
    return int(row["balance_val"]) if row else 0


async def set_balance(character_name: str, new_val: int) -> None:
    await db_exec(
        """
        INSERT INTO econ_balances (guild_id, character_name, balance_val, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (guild_id, character_name)
        DO UPDATE SET balance_val=EXCLUDED.balance_val, updated_at=NOW();
        """,
        DATA_GUILD_ID,
        character_name,
        int(new_val),
    )


async def adjust_balance(character_name: str, delta: int) -> int:
    cur = await get_balance(character_name)
    new_val = cur + int(delta)
    await set_balance(character_name, new_val)
    return new_val


async def log_audit(interaction: discord.Interaction, action: str, details: Dict[str, Any]) -> None:
    try:
        await db_exec(
            """
            INSERT INTO econ_audit_log (ts, guild_id, actor_user_id, action, details)
            VALUES (NOW(), $1, $2, $3, $4::jsonb);
            """,
            DATA_GUILD_ID,
            int(interaction.user.id),
            action,
            json.dumps(details),
        )
    except Exception as e:
        print(f"[warn] audit log insert failed: {e}")


# -------------------------
# Assets (definitions in econ_asset_definitions; purchases in econ_assets)
# -------------------------

async def list_asset_types() -> List[str]:
    try:
        rows = await db_fetch(
            """
            SELECT DISTINCT asset_type
            FROM econ_asset_definitions
            ORDER BY asset_type ASC;
            """
        )
        return [str(r["asset_type"]) for r in rows]
    except Exception as e:
        print(f"[warn] list_asset_types failed: {e}")
        return []


async def list_tiers_for_type(asset_type: str) -> List[str]:
    try:
        rows = await db_fetch(
            """
            SELECT tier
            FROM econ_asset_definitions
            WHERE asset_type=$1
            ORDER BY tier ASC;
            """,
            asset_type,
        )
        return [str(r["tier"]) for r in rows]
    except Exception as e:
        print(f"[warn] list_tiers_for_type({asset_type}) failed: {e}")
        return []


async def get_asset_def(asset_type: str, tier: str) -> Optional[Tuple[int, int]]:
    row = await db_fetchrow(
        """
        SELECT cost_val, add_income_val
        FROM econ_asset_definitions
        WHERE asset_type=$1 AND tier=$2
        LIMIT 1;
        """,
        asset_type,
        tier,
    )
    if not row:
        return None
    return int(row["cost_val"]), int(row["add_income_val"])



def _selected_option_from_interaction(interaction: discord.Interaction, option_name: str) -> Optional[str]:
    """
    Robustly retrieve a selected option value during autocomplete.
    discord.py sometimes lacks namespace fields during autocomplete depending on client/event.
    """
    # 1) Try namespace
    try:
        ns = getattr(interaction, "namespace", None)
        if ns is not None and hasattr(ns, option_name):
            v = getattr(ns, option_name)
            if v is not None:
                return str(v)
    except Exception:
        pass
    # 2) Try interaction.data options payload
    try:
        data = getattr(interaction, "data", None) or {}
        opts = data.get("options") or []
        # options can be nested for groups; handle shallow only (we don't use groups here)
        for o in opts:
            if o.get("name") == option_name and "value" in o:
                return str(o.get("value"))
    except Exception:
        pass
    return None
async def seed_asset_definitions() -> None:
    """Upsert the authoritative asset definitions set into econ_asset_definitions."""
    try:
        await db_exec(
            """
            CREATE TABLE IF NOT EXISTS econ_asset_definitions (
              asset_type TEXT NOT NULL,
              tier TEXT NOT NULL,
              cost_val BIGINT NOT NULL,
              add_income_val BIGINT NOT NULL,
              PRIMARY KEY (asset_type, tier)
            );
            """
        )
        for asset_type, tier, cost_val, add_income_val in ASSET_DEFINITIONS_SEED:
            await db_exec(
                """
                INSERT INTO econ_asset_definitions (asset_type, tier, cost_val, add_income_val)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (asset_type, tier)
                DO UPDATE SET cost_val=EXCLUDED.cost_val, add_income_val=EXCLUDED.add_income_val;
                """,
                asset_type, tier, int(cost_val), int(add_income_val)
            )
        row = await db_fetchrow("SELECT COUNT(*) AS c FROM econ_asset_definitions;")
        total = int(row["c"]) if row and "c" in row else 0
        if total != len(ASSET_DEFINITIONS_SEED):
            print(f"[warn] econ_asset_definitions rowcount={total} differs from seed={len(ASSET_DEFINITIONS_SEED)}.")
        else:
            print(f"[test] econ_asset_definitions seeded/verified: {total} row(s).")
    except Exception as e:
        print(f"[warn] seed_asset_definitions failed: {e}")



async def asset_type_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    current_l = (current or "").lower()
    types = await list_asset_types()
    if not types:
        print('[warn] econ_asset_definitions returned 0 asset types (table empty or not populated).')
    out = [t for t in types if current_l in t.lower()][:25]
    return [app_commands.Choice(name=t, value=t) for t in out]


async def tier_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    # Need selected asset_type to filter tiers
    asset_type = _selected_option_from_interaction(interaction, "asset_type") or ""
    if not asset_type:
        return []
    tiers = await list_tiers_for_type(asset_type)
    current_l = (current or "").lower()
    out = [t for t in tiers if current_l in t.lower()][:25]
    return [app_commands.Choice(name=t, value=t) for t in out]


async def recompute_daily_income(character_name: str) -> int:
    # Daily income = sum(add_income_val) across current assets' tiers
    rows = await db_fetch(
        """
        SELECT a.asset_type, a.tier, d.add_income_val
        FROM econ_assets a
        JOIN econ_asset_definitions d
          ON d.asset_type=a.asset_type AND d.tier=a.tier
        WHERE a.guild_id=$1 AND a.character_name=$2;
        """,
        DATA_GUILD_ID,
        character_name,
    )
    return sum(int(r["add_income_val"]) for r in rows)


# -------------------------
# Kingdom taxation helpers
# -------------------------

def _bp_to_percent(bp: int) -> str:
    # 100 bp = 1%
    return f"{bp / 100:.0f}%" if bp % 100 == 0 else f"{bp / 100:.2f}%"

def _calc_tax(amount_cinth: int, tax_rate_bp: int) -> int:
    # Whole-cinth rule: ALWAYS round DOWN (floor) to nearest cinth.
    if amount_cinth <= 0 or tax_rate_bp <= 0:
        return 0
    return (int(amount_cinth) * int(tax_rate_bp)) // 10000

async def get_character_kingdom(character_name: str) -> Optional[str]:
    # Home kingdom lives in characters table (populated by upstream bot).
    try:
        row = await db_fetchrow(
            """
            SELECT kingdom
            FROM characters
            WHERE guild_id=$1 AND name=$2 AND archived=FALSE
            LIMIT 1;
            """,
            DATA_GUILD_ID,
            character_name,
        )
        if not row:
            return None
        hk = row.get("kingdom")
        return str(hk).strip() if hk is not None and str(hk).strip() else None
    except Exception:
        return None

async def get_kingdom_tax_bp(kingdom: str) -> int:
    row = await db_fetchrow(
        """
        SELECT tax_rate_bp
        FROM econ_kingdoms
        WHERE guild_id=$1 AND kingdom=$2
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        kingdom,
    )
    return int(row["tax_rate_bp"]) if row else 0

async def upsert_kingdom_tax_bp(kingdom: str, tax_rate_bp: int) -> None:
    await db_exec(
        """
        INSERT INTO econ_kingdoms (guild_id, kingdom, tax_rate_bp, treasury)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (guild_id, kingdom)
        DO UPDATE SET tax_rate_bp=EXCLUDED.tax_rate_bp;
        """,
        DATA_GUILD_ID,
        kingdom,
        int(tax_rate_bp),
    )

async def add_to_kingdom_treasury(kingdom: str, amount_cinth: int) -> None:
    if amount_cinth <= 0:
        return
    await db_exec(
        """
        INSERT INTO econ_kingdoms (guild_id, kingdom, tax_rate_bp, treasury)
        VALUES ($1, $2, 0, $3)
        ON CONFLICT (guild_id, kingdom)
        DO UPDATE SET treasury=econ_kingdoms.treasury + EXCLUDED.treasury;
        """,
        DATA_GUILD_ID,
        kingdom,
        int(amount_cinth),
    )

async def fetch_kingdom_treasuries() -> List[Tuple[str, int, int]]:
    rows = await db_fetch(
        """
        SELECT kingdom, tax_rate_bp, treasury
        FROM econ_kingdoms
        WHERE guild_id=$1
        ORDER BY kingdom ASC;
        """,
        DATA_GUILD_ID,
    )
    out: List[Tuple[str, int, int]] = []
    for r in rows:
        out.append((str(r["kingdom"]), int(r["tax_rate_bp"]), int(r["treasury"])))
    return out

async def render_treasury_lines() -> List[str]:
    treas = await fetch_kingdom_treasuries()
    if not treas:
        return [
            "🏰 **Kingdom Treasuries**",
            "━━━━━━━━━━━━━━━━━━",
            "_No kingdoms configured yet._",
            "",
        ]
    out: List[str] = [
        "🏰 **Kingdom Treasuries**",
        "━━━━━━━━━━━━━━━━━━",
    ]
    for kingdom, bp, treasury in treas:
        out.append(f"• **{kingdom}** — Treasury: **{format_currency(treasury)}** — Tax: **{_bp_to_percent(bp)}**")
    out.append("")
    return out



# -------------------------
# Bank dashboard persistence (approved)
# -------------------------

async def bank_message_ids_from_db() -> List[int]:
    rows = await db_fetch(
        """
        SELECT idx, message_id
        FROM econ_bank_messages
        WHERE guild_id=$1
        ORDER BY idx ASC;
        """,
        DATA_GUILD_ID,
    )
    if not rows:
        return []
    # fill by idx order
    return [int(r["message_id"]) for r in rows]


async def save_bank_message_ids(message_ids: List[int]) -> None:
    # Upsert by idx
    pool = await db_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # delete existing for guild, then insert (keeps idx stable)
            await conn.execute("DELETE FROM econ_bank_messages WHERE guild_id=$1;", DATA_GUILD_ID)
            for i, mid in enumerate(message_ids):
                await conn.execute(
                    """
                    INSERT INTO econ_bank_messages (guild_id, idx, message_id)
                    VALUES ($1, $2, $3);
                    """,
                    DATA_GUILD_ID,
                    int(i),
                    int(mid),
                )


async def render_bank_pages(guild: discord.Guild) -> List[str]:
    """
    Returns full message pages (content strings) for the bank dashboard.

    Page 1: header + kingdom treasuries + leaderboards
    Pages 2+: full character cards (never split across messages)
    """
    chars = await db_fetch(
        '''
        SELECT user_id, name
        FROM characters
        WHERE guild_id=$1 AND archived=FALSE
        ORDER BY name ASC;
        ''',
        DATA_GUILD_ID,
    )

    now = datetime.now(CHICAGO_TZ)

    header_lines: List[str] = [
        f"🏦 **Bank of Vilyra** — {now.strftime('%Y-%m-%d %H:%M')} (Chicago)",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    header_lines += await render_treasury_lines()
    header_lines.append("")

    if not chars:
        return ["\n".join(header_lines + ["No characters found in DB."])]

    # Precompute balance+income for leaderboards (single DB authority)
    rows: List[Tuple[str, int, int, int]] = []
    for r in chars:
        cname = str(r["name"])
        uid = int(r["user_id"])
        bal = await get_balance(cname)
        inc = await recompute_daily_income(cname)
        rows.append((cname, uid, bal, inc))

    header_lines += await render_leaderboard_lines(guild, rows)
    header_lines.append("")
    header_lines.append("📜 **Ledger Entries**")
    header_lines.append("━━━━━━━━━━━━━━━━━━")
    header_lines.append("_See the following messages for full character cards._")

    # Build card blocks (each block is a complete card)
    rows_sorted = sorted(rows, key=lambda r: (int(r[1]), str(r[0]).lower()))
    card_blocks: List[str] = []
    for cname, uid, _, _ in rows_sorted:
        card_lines = await render_character_card(guild, cname, owner_id=uid)
        card_text = "\n".join(card_lines).strip()

        # Ensure a single card never exceeds a safe limit; truncate asset list if needed
        max_card_len = 1800
        if len(card_text) > max_card_len:
            lines = card_text.split("\n")
            removed = 0
            while len("\n".join(lines)) > max_card_len and any(l.startswith("- ") for l in lines):
                for i in range(len(lines) - 1, -1, -1):
                    if lines[i].startswith("- "):
                        lines.pop(i)
                        removed += 1
                        break
                else:
                    break
            if removed > 0:
                try:
                    insert_at = len(lines)
                    for i in range(len(lines) - 1, -1, -1):
                        if lines[i].startswith("🏷️ **Assets"):
                            insert_at = i + 1
                            break
                    lines.insert(insert_at, f"- …and {removed} more (not shown)")
                except Exception:
                    pass
            card_text = "\n".join(lines).strip()

        card_blocks.append(card_text)

    # Paginate cards so no card is split across messages
    pages: List[str] = []
    pages.append("\n".join(header_lines).strip())

    max_page_len = 1900
    current: List[str] = []
    current_len = 0

    def flush():
        nonlocal current, current_len
        if current:
            pages.append("\n\n".join(current).strip())
            current = []
            current_len = 0

    for block in card_blocks:
        add_len = len(block) + (2 if current else 0)  # account for \n\n
        if current_len + add_len > max_page_len:
            flush()
            current.append(block)
            current_len = len(block)
        else:
            current.append(block)
            current_len += add_len

    flush()
    return pages


async def refresh_bank_dashboard(create_missing: bool = True) -> None:
    if not BANK_CHANNEL_ID:
        return
    ch = client.get_channel(int(BANK_CHANNEL_ID))
    if ch is None or not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return

    mids = await bank_message_ids_from_db()
    if not mids and BANK_MESSAGE_IDS:
        mids = list(BANK_MESSAGE_IDS)

    pages = await render_bank_pages(ch.guild)
    if not pages:
        pages = ["(empty)"]

    msgs: List[discord.Message] = []
    for mid in mids:
        try:
            m = await ch.fetch_message(int(mid))
            msgs.append(m)
        except Exception:
            pass

    if create_missing and len(msgs) < len(pages):
        try:
            while len(msgs) < len(pages):
                m = await ch.send("Initializing Bank of Vilyra…")
                msgs.append(m)
            await save_bank_message_ids([int(m.id) for m in msgs])
            print(f"[test] Bank dashboard message IDs saved to Postgres: {len(msgs)}")
        except Exception as e:
            print(f"[warn] Bank dashboard create/persist failed: {e}")

    n = min(len(msgs), len(pages))
    for i in range(n):
        # Robust edit with small retries; avoids silent stale page 1 on transient failures / rate limits.
        for attempt in range(4):
            try:
                if msgs[i].content != pages[i]:
                    await msgs[i].edit(content=pages[i])
                # Space edits to reduce 429s when multiple pages exist.
                if i < n - 1:
                    await asyncio.sleep(1.2)
                break
            except discord.HTTPException as e:
                # 429/5xx can happen; backoff a bit
                await asyncio.sleep(0.8 * (attempt + 1))
            except Exception:
                await asyncio.sleep(0.5 * (attempt + 1))

    if len(msgs) > len(pages):
        for j in range(len(pages), len(msgs)):
            try:
                await msgs[j].edit(content="(unused bank page)")
            except Exception:
                pass

# Debounced bank refresh to avoid rate-limits when multiple actions occur quickly
_bank_refresh_task: Optional[asyncio.Task] = None
_bank_refresh_lock: asyncio.Lock = asyncio.Lock()

def trigger_bank_refresh() -> None:
    global _bank_refresh_task
    try:
        if _bank_refresh_task and not _bank_refresh_task.done():
            return

        async def _runner():
            async with _bank_refresh_lock:
                # small debounce window; collapse bursty updates
                await asyncio.sleep(1.5)
                try:
                    # Debounced refresh to avoid PATCH rate limits on bursty updates
                    await refresh_bank_dashboard(create_missing=True)
                except Exception as e:
                    print(f"[warn] Debounced bank refresh failed: {e}")

        _bank_refresh_task = asyncio.create_task(_runner())
    except Exception:
        # If we're not in a running loop yet, ignore; caller can use /econ_refresh_bank
        return




# -------------------------
# Commands
# -------------------------

@tree.command(name="balance", description="View a character's current balance.", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(character="Character name")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_balance(interaction: discord.Interaction, character: str):
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    if guild is None:
        await interaction.followup.send("This command must be used in a server.", ephemeral=True)
        return
    card_lines = await render_character_card(guild, character)
    # render as a single message if possible
    txt = "\n".join(card_lines).strip()
    if len(txt) > 1900:
        # truncate assets if too long for ephemeral message
        trimmed: List[str] = []
        for ln in card_lines:
            if len("\n".join(trimmed + [ln])) > 1800:
                trimmed.append("… (truncated)")
                break
            trimmed.append(ln)
        txt = "\n".join(trimmed).strip()
    await interaction.followup.send(txt, ephemeral=True)


@tree.command(name="income", description="Claim daily income for a character.", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(character="Character name")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_income(interaction: discord.Interaction, character: str):
    await interaction.response.defer(ephemeral=True)

    # Ownership: only the character owner can claim (based on characters table user_id)
    owner = await get_character_owner(character)
    if owner is None:
        await interaction.followup.send("Character not found in DB.", ephemeral=True)
        return
    if int(owner) != int(interaction.user.id):
        await interaction.followup.send("You are not the owner of that character.", ephemeral=True)
        return

    today = datetime.now(CHICAGO_TZ).date() if ZoneInfo else date.today()
    row = await db_fetchrow(
        """
        SELECT last_claim_date
        FROM econ_income_claims
        WHERE guild_id=$1 AND character_name=$2
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        character,
    )
    if row and row["last_claim_date"] == today:
        await interaction.followup.send("Daily income already claimed today.", ephemeral=True)
        return

    
    # Kingdom taxation:
    # - Base income is taxed to the character's home kingdom.
    # - Each asset's income is taxed to its own kingdom if set; otherwise inherits home kingdom.
    character_kingdom = await get_character_kingdom(character)
    if not character_kingdom:
        await interaction.followup.send(
            "This character has no **home kingdom** set in the `characters` table. Income cannot be claimed until it is set.",
            ephemeral=True,
        )
        return

    # Pull per-asset incomes so we can bucket taxes per kingdom (whole-cinth only).
    asset_rows = await db_fetch(
        """
        SELECT a.asset_type, a.tier, a.asset_name, COALESCE(a.kingdom, '') AS asset_kingdom, d.add_income_val
        FROM econ_assets a
        JOIN econ_asset_definitions d
          ON d.asset_type=a.asset_type AND d.tier=a.tier
        WHERE a.guild_id=$1 AND a.character_name=$2;
        """,
        DATA_GUILD_ID,
        character,
    )

    asset_income = sum(int(r["add_income_val"]) for r in asset_rows)
    base_income = int(BASE_DAILY_INCOME)
    gross_total = base_income + int(asset_income or 0)

    # Build kingdom buckets (gross amounts per kingdom)
    buckets: Dict[str, int] = {}
    buckets[character_kingdom] = buckets.get(character_kingdom, 0) + base_income
    for r in asset_rows:
        k = str(r["asset_kingdom"] or "").strip()
        if not k:
            k = character_kingdom
        buckets[k] = buckets.get(k, 0) + int(r["add_income_val"])

    # Compute tax per kingdom bucket (round DOWN) and update treasuries
    total_tax = 0
    for k, amt in buckets.items():
        bp = await get_kingdom_tax_bp(k)
        tax = _calc_tax(int(amt), int(bp))
        if tax > 0:
            await add_to_kingdom_treasury(k, tax)
        total_tax += int(tax)

    net_total = int(gross_total) - int(total_tax)
    if net_total < 0:
        net_total = 0  # safety; should not happen with floor-tax

    # Add NET income to balance
    new_bal = await adjust_balance(character, net_total)

    await db_exec(
        """
        INSERT INTO econ_income_claims (guild_id, character_name, last_claim_date)
        VALUES ($1, $2, $3)
        ON CONFLICT (guild_id, character_name)
        DO UPDATE SET last_claim_date=EXCLUDED.last_claim_date;
        """,
        DATA_GUILD_ID,
        character,
        today,
    )


    await log_audit(
        interaction,
        "income_claim",
        {
            "character": character,
            "character_kingdom": character_kingdom,
            "base_income": base_income,
            "asset_income": asset_income,
            "gross_total": gross_total,
            "tax_total": total_tax,
            "net_total": net_total,
            "new_balance": new_bal,
            "buckets": buckets,
        },
    )

    await interaction.followup.send(
        (
            f"Claimed daily income for **{character}**:\n"
            f"• Base: **{format_currency(base_income)}** (taxed to **{character_kingdom}**)\n"
            f"• Assets: **{format_currency(asset_income)}**\n"
            f"• Gross: **{format_currency(gross_total)}**\n"
            f"• Tax (rounded down): **{format_currency(total_tax)}**\n"
            f"• Net received: **{format_currency(net_total)}**\n\n"
            f"New balance: **{format_currency(new_bal)}**"
        ),
        ephemeral=True,
    )

    trigger_bank_refresh()



@tree.command(name="econ_commands", description="List EconBot commands.", guild=discord.Object(id=GUILD_ID))
@staff_only()
async def cmd_econ_commands(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    msg = (
        f"**EconBot Commands** ({APP_VERSION})\n\n"
        "**Player**\n"
        "• `/balance` — view balance\n"
        "• `/income` — claim daily income\n\n"
        "**Staff**\n"
        "• `/purchase_new` — record an asset purchase\n"
        "• `/upgrade_asset` — upgrade an existing asset\n"
        "• `/sell_asset` — sell/remove an existing asset\n"
        "• `/econ_adjust` — adjust balance by delta\n"
        "• `/econ_set_balance` — set balance to value\n"
        "• `/econ_refresh_bank` — refresh bank dashboard\n• `/econ_set_kingdom_tax` — set kingdom tax rate (10–50%)\n"
    )
    await interaction.followup.send(msg, ephemeral=True)


@tree.command(name="econ_set_kingdom_tax", description="Set a kingdom's income tax rate (10–50%).", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(kingdom="Kingdom name (must match character/asset kingdom values).", rate="Tax rate percent.")
@app_commands.choices(
    kingdom=[
        app_commands.Choice(name="Sethrathiel", value="Sethrathiel"),
        app_commands.Choice(name="Velarith", value="Velarith"),
        app_commands.Choice(name="Lyvik", value="Lyvik"),
        app_commands.Choice(name="Baelon", value="Baelon"),
        app_commands.Choice(name="Avalea", value="Avalea"),
    ],
    rate=[
        app_commands.Choice(name="10%", value=10),
        app_commands.Choice(name="20%", value=20),
        app_commands.Choice(name="30%", value=30),
        app_commands.Choice(name="40%", value=40),
        app_commands.Choice(name="50%", value=50),
    ],
)
async def cmd_set_kingdom_tax(interaction: discord.Interaction, kingdom: str, rate: app_commands.Choice[int]):
    await interaction.response.defer(ephemeral=True)
    k = (kingdom or "").strip()
    if not k:
        await interaction.followup.send("Kingdom name is required.", ephemeral=True)
        return
    pct = int(rate.value)
    bp = pct * 100  # convert percent to basis points
    await upsert_kingdom_tax_bp(k, bp)
    await log_audit(interaction, "set_kingdom_tax", {"kingdom": k, "percent": pct, "tax_rate_bp": bp})
    await interaction.followup.send(f"Set **{k}** tax rate to **{pct}%** (stored as **{bp} bp**).", ephemeral=True)



@tree.command(name="econ_adjust", description="(Staff) Adjust a character balance by delta.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(character="Character name", delta="Positive or negative amount")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_adjust(interaction: discord.Interaction, character: str, delta: int):
    await interaction.response.defer(ephemeral=True)

    delta = int(delta)
    cur_bal = await get_balance(character)
    proposed = int(cur_bal) + int(delta)

    if proposed < 0:
        await interaction.followup.send(
            "Denied: that adjustment would take the balance negative.\n"
            f"Available funds: **{format_currency(cur_bal)}**\n"
            f"Attempted adjustment: **{format_currency(delta)}**\n"
            f"Would result in: **{format_currency(proposed)}**",
            ephemeral=True,
        )
        return

    new_bal = await adjust_balance(character, delta)
    await log_audit(interaction, "adjust_balance", {"character": character, "delta": delta, "new_balance": new_bal})
    await interaction.followup.send(
        f"Adjusted **{character}** by **{format_currency(delta)}**. New balance: **{format_currency(new_bal)}**",
        ephemeral=True,
    )

    trigger_bank_refresh()


@tree.command(name="econ_set_balance", description="(Staff) Set a character balance to an exact value.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(character="Character name", value="New balance (must be >= 0)")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_set_balance(interaction: discord.Interaction, character: str, value: int):
    await interaction.response.defer(ephemeral=True)

    value = int(value)
    if value < 0:
        cur_bal = await get_balance(character)
        await interaction.followup.send(
            "Denied: balance cannot be set to a negative value.\n"
            f"Current balance: **{format_currency(cur_bal)}**\n"
            f"Attempted set value: **{format_currency(value)}**",
            ephemeral=True,
        )
        return

    await set_balance(character, value)
    await log_audit(interaction, "set_balance", {"character": character, "value": value})
    await interaction.followup.send(f"Set **{character}** balance to **{format_currency(value)}**.", ephemeral=True)
    trigger_bank_refresh()


@tree.command(name="econ_refresh_bank", description="(Staff) Refresh the bank dashboard messages.", guild=discord.Object(id=GUILD_ID))
@staff_only()
async def cmd_refresh_bank(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # Optionally allow BANK_REFRESH_ROLE_IDS to run refresh too, but staff_only already gates role-based access.
    try:
        # Debounced refresh to avoid PATCH rate limits on startup
        trigger_bank_refresh()
        await interaction.followup.send("Bank dashboard refreshed.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Bank refresh failed: {e}", ephemeral=True)


@tree.command(name="purchase_new", description="(Staff) Record an asset purchase for a character.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(
    character="Character purchasing the asset",
    asset_type="Asset category",
    tier="Tier being purchased",
    asset_name="Unique asset name (entered by staff)",
)
@app_commands.autocomplete(character=character_autocomplete, asset_type=asset_type_autocomplete, tier=tier_autocomplete)
async def cmd_purchase_new(interaction: discord.Interaction, character: str, asset_type: str, tier: str, asset_name: str):
    # Always defer quickly to avoid Discord timeouts
    await interaction.response.defer(ephemeral=True)

    owner = await get_character_owner(character)
    if owner is None:
        await interaction.followup.send("Character not found in DB.", ephemeral=True)
        return

    # Validate asset definition exists (and get add_income for audit)
    adef = await get_asset_def(asset_type, tier)
    if not adef:
        await interaction.followup.send("Invalid asset type/tier (not found in asset definitions).", ephemeral=True)
        return
    _tier_cost_val, add_income_val = adef

    # Cost is cumulative across tiers up to the selected target tier.
    cost_val = await cumulative_cost_to_tier(asset_type, tier)
    if cost_val is None:
        await interaction.followup.send("Unable to compute cumulative cost for this asset type/tier.", ephemeral=True)
        return

    cur_bal = await get_balance(character)
    if cur_bal < cost_val:
        await interaction.followup.send(
            f"Insufficient funds. Balance **{format_currency(cur_bal)}**, cost **{format_currency(cost_val)}**.",
            ephemeral=True,
        )
        return

    asset_name = (asset_name or "").strip()
    if not asset_name:
        await interaction.followup.send("Asset name cannot be empty.", ephemeral=True)
        return

    # Allow same asset_name across different asset_type/tier, but not duplicates within the same type+tier.
    exists = await db_fetchrow(
        """
        SELECT 1
        FROM econ_assets
        WHERE guild_id=$1 AND character_name=$2 AND asset_type=$3 AND tier=$4 AND asset_name=$5
        LIMIT 1;
        """,
        DATA_GUILD_ID,
        character,
        asset_type,
        tier,
        asset_name,
    )
    if exists:
        await interaction.followup.send(
            "That character already has an asset with the same **type, tier, and name**. Choose a different name or tier.",
            ephemeral=True,
        )
        return

    # Record asset
    try:
        await db_exec(
            """
            INSERT INTO econ_assets (guild_id, character_name, user_id, asset_name, asset_type, tier, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW());
            """,
            DATA_GUILD_ID,
            character,
            int(owner),
            asset_name,
            asset_type,
            tier,
        )
    except Exception as e:
        await interaction.followup.send(f"Failed to add asset (see logs for details): {e}", ephemeral=True)
        return

    # Deduct cost
    new_bal = await adjust_balance(character, -cost_val)

    # Income is computed dynamically from assets; we don't store a separate total.
    new_daily_income = await recompute_daily_income(character)

    await log_audit(
        interaction,
        "purchase_new",
        {
            "character": character,
            "owner_user_id": int(owner),
            "asset_type": asset_type,
            "tier": tier,
            "asset_name": asset_name,
            "cost": cost_val,
            "add_income": add_income_val,
            "new_balance": new_bal,
            "new_daily_income": new_daily_income,
        },
    )

    await interaction.followup.send(
        f"Recorded purchase for **{character}**:\n"
        f"• **{asset_type}** | **{tier}** | **{asset_name}**\n"
        f"Cost: **{format_currency(cost_val)}** (new balance **{format_currency(new_bal)}**)\n"
        f"Daily income now: **{format_currency(new_daily_income)}**",
        ephemeral=True,
    )

@tree.command(name="upgrade_asset", description="(Staff) Upgrade an existing asset to a higher tier.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.autocomplete(character=character_autocomplete, asset=ac_asset_for_character, target_tier=ac_target_tier)
async def cmd_upgrade_asset(interaction: discord.Interaction, character: str, asset: str, target_tier: str):
    await interaction.response.defer(ephemeral=True)

    parts = [p.strip() for p in str(asset).split("|")]
    if len(parts) < 3:
        await interaction.followup.send("Invalid asset selection.", ephemeral=True)
        return
    asset_type = parts[0]
    current_tier = parts[1]
    asset_name = "|".join(parts[2:]).strip()

    exists = await db_fetchrow(
        '''
        SELECT 1
        FROM econ_assets
        WHERE guild_id=$1 AND character_name=$2 AND asset_type=$3 AND tier=$4 AND asset_name=$5
        LIMIT 1;
        ''',
        DATA_GUILD_ID,
        character,
        asset_type,
        current_tier,
        asset_name,
    )
    if not exists:
        await interaction.followup.send("That asset no longer exists on this character.", ephemeral=True)
        return

    cur_rank = _tier_rank(current_tier)
    tgt_rank = _tier_rank(target_tier)
    if cur_rank is not None and tgt_rank is not None and tgt_rank <= cur_rank:
        await interaction.followup.send("Target tier must be higher than current tier.", ephemeral=True)
        return

    cost_val = await incremental_cost_between_tiers(asset_type, current_tier, target_tier)
    if cost_val is None or cost_val <= 0:
        await interaction.followup.send("Unable to calculate upgrade cost for that tier change.", ephemeral=True)
        return

    cur_bal = await get_balance(character)
    if cur_bal < cost_val:
        await interaction.followup.send(
            f"Insufficient funds. Available: **{format_currency(cur_bal)}**. Required: **{format_currency(cost_val)}**.",
            ephemeral=True,
        )
        return

    await adjust_balance(character, -int(cost_val))
    await db_exec(
        '''
        UPDATE econ_assets
        SET tier=$1
        WHERE guild_id=$2 AND character_name=$3 AND asset_type=$4 AND tier=$5 AND asset_name=$6;
        ''',
        target_tier,
        DATA_GUILD_ID,
        character,
        asset_type,
        current_tier,
        asset_name,
    )

    await log_audit(
        interaction,
        "upgrade_asset",
        {
            "character": character,
            "asset_type": asset_type,
            "asset_name": asset_name,
            "from_tier": current_tier,
            "to_tier": target_tier,
            "cost": int(cost_val),
        },
    )

    trigger_bank_refresh()

    await interaction.followup.send(
        (
            f"Upgraded **{character}** asset:\n"
            f"- {asset_type} | {current_tier} | {asset_name}\n"
            f"→ {asset_type} | {target_tier} | {asset_name}\n"
            f"Cost: **{format_currency(cost_val)}**\n"
            f"New balance: **{format_currency(await get_balance(character))}**"
        ),
        ephemeral=True,
    )


@tree.command(name="sell_asset", description="(Staff) Sell/remove an asset (optional refund).", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.autocomplete(character=character_autocomplete, asset=ac_asset_for_character)
@app_commands.describe(refund_percent="Optional refund percent of cumulative cost (0-100). Default 0.")
async def cmd_sell_asset(interaction: discord.Interaction, character: str, asset: str, refund_percent: Optional[int] = 100):
    await interaction.response.defer(ephemeral=True)

    refund_percent = int(refund_percent or 0)
    refund_percent = max(0, min(100, refund_percent))

    parts = [p.strip() for p in str(asset).split("|")]
    if len(parts) < 3:
        await interaction.followup.send("Invalid asset selection.", ephemeral=True)
        return
    asset_type = parts[0]
    tier = parts[1]
    asset_name = "|".join(parts[2:]).strip()

    row = await db_fetchrow(
        '''
        SELECT 1
        FROM econ_assets
        WHERE guild_id=$1 AND character_name=$2 AND asset_type=$3 AND tier=$4 AND asset_name=$5
        LIMIT 1;
        ''',
        DATA_GUILD_ID,
        character,
        asset_type,
        tier,
        asset_name,
    )
    if not row:
        await interaction.followup.send("That asset no longer exists on this character.", ephemeral=True)
        return

    refund_amount = 0
    if refund_percent > 0:
        tier_cost = await tier_cost_for(asset_type, tier)
        if tier_cost is None or tier_cost <= 0:
            await interaction.followup.send("Unable to calculate refund amount for this asset tier.", ephemeral=True)
            return
        refund_amount = int(round((tier_cost * refund_percent) / 100.0))


    await db_exec(
        '''
        DELETE FROM econ_assets
        WHERE guild_id=$1 AND character_name=$2 AND asset_type=$3 AND tier=$4 AND asset_name=$5;
        ''',
        DATA_GUILD_ID,
        character,
        asset_type,
        tier,
        asset_name,
    )

    if refund_amount:
        await adjust_balance(character, int(refund_amount))

    await log_audit(
        interaction,
        "sell_asset",
        {
            "character": character,
            "asset_type": asset_type,
            "tier": tier,
            "asset_name": asset_name,
            "refund_percent": refund_percent,
            "refund_amount": refund_amount,
        },
    )

    trigger_bank_refresh()

    msg = (
        f"Sold/removed asset from **{character}**:\n"
        f"- {asset_type} | {tier} | {asset_name}\n"
    )
    if refund_amount:
        msg += f"Refund: **{format_currency(refund_amount)}** ({refund_percent}%)\n"
    msg += f"New balance: **{format_currency(await get_balance(character))}**"

    await interaction.followup.send(msg, ephemeral=True)



# -------------------------
# Global app command error handler (prevents "stuck thinking" on exceptions)
# -------------------------

@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    # Always log the root error in Railway logs
    try:
        import traceback
        traceback.print_exception(type(error), error, error.__traceback__)
    except Exception:
        pass

    # Respond ephemerally so interactions don't hang forever
    msg = "⚠️ Internal error while running that command. Check Railway logs for details."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass

# -------------------------
# Startup / sync
# -------------------------


async def delete_all_guild_commands():
    """Delete ALL guild-scoped commands in the configured GUILD_ID on Discord.
    This is a server-side cleanup to eliminate stale signatures/duplicates.
    """
    try:
        guild_obj = discord.Object(id=GUILD_ID)
        guild_cmds = await tree.fetch_commands(guild=guild_obj)
        if guild_cmds:
            for c in guild_cmds:
                try:
                    await c.delete()
                except Exception as e:
                    print(f"[warn] Failed deleting GUILD /{getattr(c,'name','?')}: {e}")
            print(f"[test] Requested deletion of {len(guild_cmds)} GUILD command(s).")
        else:
            print("[test] No GUILD commands found to delete.")
    except Exception as e:
        print(f"[warn] Guild command deletion failed/skipped: {e}")

async def delete_all_global_commands() -> None:
    # You selected option B: bot deletes global commands automatically.
    try:
        global_cmds = await tree.fetch_commands()  # global
        if global_cmds:
            for c in global_cmds:
                try:
                    await c.delete()
                except Exception as e:
                    print(f"[warn] Failed deleting GLOBAL /{getattr(c,'name','?')}: {e}")
            print(f"[test] Requested deletion of {len(global_cmds)} GLOBAL command(s).")
        else:
            print("[test] No GLOBAL commands found.")
    except Exception as e:
        print(f"[warn] Global command deletion failed/skipped: {e}")


@client.event
async def on_ready():
    # Keep on_ready resilient: never allow an exception to abort command sync.
    print(f"[test] Starting {APP_VERSION}…")
    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID}; data guild: {DATA_GUILD_ID})")
    print(f"[debug] raw STAFF_ROLE_IDS env: {repr(_get('STAFF_ROLE_IDS',''))}")
    print(f"[debug] STAFF_ROLE_IDS_DEFAULT: {sorted(list(STAFF_ROLE_IDS_DEFAULT))}")
    print(f"[debug] STAFF_ROLE_IDS (effective): {sorted(list(STAFF_ROLE_IDS))}")

    guild_obj = discord.Object(id=GUILD_ID)

    # --- COMMAND SYNC (first, hardened) ---
    try:
        # Defensive: ensure new commands are present in local guild registry.
        # NOTE: decorated functions are Command objects.
        try:
            tree.add_command(cmd_upgrade_asset, guild=guild_obj)
        except Exception:
            pass
        try:
            tree.add_command(cmd_sell_asset, guild=guild_obj)
        except Exception:
            pass

        # Optional: copy any locally-registered global commands into guild scope (no global sync).
        try:
            tree.copy_global_to(guild=guild_obj)
        except Exception as e:
            print(f"[warn] copy_global_to failed/skipped: {e}")

# NOTE: We do NOT clear guild commands here.
# tree.sync(guild=...) overwrites the server-side guild command set to match the locally-registered guild commands.
# Clearing first would drop all existing commands unless we re-register every single one manually.

        synced = await tree.sync(guild=guild_obj)
        print(f"[test] Synced {len(synced)} guild command(s).")

        # Post-sync verification: what Discord now has.
        try:
            post = sorted([c.name for c in await tree.fetch_commands(guild=guild_obj)])
            print(f"[debug] Post-sync guild commands (server): {post}")
        except Exception as e:
            print(f"[warn] Could not fetch post-sync guild commands: {e}")

        # Cleanup global commands last (best-effort) to avoid duplicates from prior versions.
        try:
            await delete_all_global_commands()
        except Exception as e:
            print(f"[warn] Global command deletion failed/skipped: {e}")

    except Exception as e:
        print(f"[warn] Command sync block failed: {e}")

    # --- DB / SEED / BANK (best-effort, after sync) ---
    try:
        await ensure_schema()
    except Exception as e:
        print(f"[warn] ensure_schema failed: {e}")

    try:
        await seed_asset_definitions()
    except Exception as e:
        print(f"[warn] seed_asset_definitions failed: {e}")

def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
