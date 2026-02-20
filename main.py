# EconBot_v75 — Clean rebuild (guild-only commands, legacy DB partition, bank message IDs persisted in Postgres)
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


APP_VERSION = "EconBot_v87"
CHICAGO_TZ = ZoneInfo("America/Chicago") if ZoneInfo else timezone.utc


# -------------------------
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


async def incremental_cost_between_tiers(asset_type: str, current_tier: str, target_tier: str) -> Optional[int]:
    """Upgrade cost from current_tier -> target_tier for an asset_type.

    Cost rule: sum of tier costs strictly above current tier rank and <= target tier rank.
    Falls back to cumulative(target) - cumulative(current) when rank parsing works but no direct sums found.
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

    # Prefer rank-based sum when current tier rank is known
    if cur_rank is not None:
        total = 0
        found_any = False
        for r in rows:
            t = str(r["tier"]).strip()
            tr = _tier_rank(t)
            if tr is None:
                continue
            if tr > cur_rank and tr <= tgt_rank:
                try:
                    total += int(r["cost_val"])
                    found_any = True
                except Exception:
                    continue
        if found_any:
            return int(total)

    # Fallback: cumulative difference
    cum_t = await cumulative_cost_to_tier(asset_type, target_tier)
    cum_c = await cumulative_cost_to_tier(asset_type, current_tier)
    if cum_t is None or cum_c is None:
        return None
    return int(cum_t - cum_c)




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
        if m is None:
            try:
                m = await guild.fetch_member(int(owner_id))
            except Exception:
                m = None
        owner_display = m.display_name if m else f"User {owner_id}"

    bal = await get_balance(character_name)
    inc = await recompute_daily_income(character_name)
    assets = await get_assets_for_character(character_name)

    out: List[str] = []
    out.append("━━━━━━━━━━━━━━━━━━")
    out.append(f"**{character_name}**  ·  *{owner_display}*")
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
        if m is None:
            try:
                m = await guild.fetch_member(uid)
            except Exception:
                m = None
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


# -------------------------
# Staff gating (ROLE ONLY)
# -------------------------

async def _get_member(interaction: discord.Interaction) -> Optional[discord.Member]:
    # Prefer interaction.user when it is a Member
    if isinstance(interaction.user, discord.Member):
        return interaction.user
    # Fetch from guild if possible
    if interaction.guild is None:
        return None
    try:
        return await interaction.guild.fetch_member(interaction.user.id)
    except Exception:
        return None


async def is_staff(interaction: discord.Interaction) -> Tuple[bool, Dict[str, Any]]:
    member = await _get_member(interaction)
    role_ids: List[int] = []
    if member is not None:
        try:
            role_ids = [int(r.id) for r in getattr(member, "roles", [])]
        except Exception:
            role_ids = []
    allowed = False
    if STAFF_ROLE_IDS and role_ids:
        allowed = any(rid in STAFF_ROLE_IDS for rid in role_ids)
    debug = {
        "user_id": int(interaction.user.id),
        "detected_role_ids": role_ids,
        "staff_role_ids": sorted(list(STAFF_ROLE_IDS)),
        "guild_id": int(interaction.guild_id or 0),
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


async def render_bank_lines(guild: discord.Guild) -> List[str]:
    # Bank shows balances grouped as:
    # 1) Leaderboards (top balance + top income)
    # 2) Full character "card rows" including full asset list (no pings)

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
    out: List[str] = [
        f"🏦 **Bank of Vilyra** — {now.strftime('%Y-%m-%d %H:%M')} (Chicago)",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    if not chars:
        return out + ["No characters found in DB."]

    # Precompute balance+income for leaderboards
    rows: List[Tuple[str, int, int, int]] = []
    for r in chars:
        cname = str(r["name"])
        uid = int(r["user_id"])
        bal = await get_balance(cname)
        inc = await recompute_daily_income(cname)
        rows.append((cname, uid, bal, inc))

    out += await render_leaderboard_lines(guild, rows)

    out.append("📜 **Ledger Entries**")
    out.append("━━━━━━━━━━━━━━━━━━")
    out.append("")

    # Render full character cards (sorted by owner then name for readability)
    # build owner display cache
    cache: Dict[int, str] = {}
    async def dn(uid: int) -> str:
        if uid in cache:
            return cache[uid]
        m = guild.get_member(uid)
        if m is None:
            try:
                m = await guild.fetch_member(uid)
            except Exception:
                m = None
        cache[uid] = m.display_name if m else f"User {uid}"
        return cache[uid]

    rows_sorted = sorted(rows, key=lambda r: (int(r[1]), str(r[0]).lower()))
    for cname, uid, bal, inc in rows_sorted:
        out += await render_character_card(guild, cname, owner_id=uid)

    return out

async def ac_asset_for_character(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Autocomplete assets for selected character. Value format: 'Type | Tier | Name'."""
    try:
        char = getattr(interaction.namespace, "character", None) or getattr(interaction.namespace, "character_name", None)
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
    """Autocomplete target tiers for the selected asset (same type), filtered to higher tiers."""
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
        tiers = await get_tiers_for_asset_type(asset_type)
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



async def refresh_bank_dashboard(create_missing: bool = True) -> None:
    if not BANK_CHANNEL_ID:
        return
    ch = client.get_channel(int(BANK_CHANNEL_ID))
    if ch is None or not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return

    # Prefer DB-stored message IDs, fallback to env BANK_MESSAGE_IDS
    mids = await bank_message_ids_from_db()
    if not mids and BANK_MESSAGE_IDS:
        mids = list(BANK_MESSAGE_IDS)

    lines = await render_bank_lines(ch.guild)
    # split into chunks that fit; keep conservative
    chunks: List[str] = []
    cur = ""
    for ln in lines:
        add = (ln + "\n")
        if len(cur) + len(add) > 1700:
            chunks.append(cur.strip())
            cur = ""
        cur += add
    if cur.strip():
        chunks.append(cur.strip())
    if not chunks:
        chunks = ["(empty)"]

    # Fetch messages if IDs exist
    msgs: List[discord.Message] = []
    for mid in mids:
        try:
            m = await ch.fetch_message(int(mid))
            msgs.append(m)
        except Exception:
            pass

    # Create missing messages ONCE and persist in Postgres (approved)
    if create_missing and len(msgs) < len(chunks):
        # Create new messages to match number of chunks
        try:
            while len(msgs) < len(chunks):
                m = await ch.send("Initializing Bank of Vilyra…")
                msgs.append(m)
            # Persist message IDs so we do not spam on future restarts
            await save_bank_message_ids([int(m.id) for m in msgs])
            print(f"[test] Bank dashboard message IDs saved to Postgres: {len(msgs)}")
        except Exception as e:
            print(f"[warn] Bank dashboard create/persist failed: {e}")

    # If still insufficient messages, do not spam; just edit what exists.
    n = min(len(msgs), len(chunks))
    for i in range(n):
        try:
            await msgs[i].edit(content=chunks[i])
        except Exception:
            pass


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

    daily_income = await recompute_daily_income(character)
    # Add daily income to balance
    new_bal = await adjust_balance(character, daily_income)
    # Upsert claim
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

    await log_audit(interaction, "income_claim", {"character": character, "income": daily_income, "new_balance": new_bal})
    await interaction.followup.send(
        f"Claimed **{format_currency(daily_income)}** daily income for **{character}**.\nNew balance: **{format_currency(new_bal)}**",
        ephemeral=True,
    )


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
        "• `/econ_adjust` — adjust balance by delta\n"
        "• `/econ_set_balance` — set balance to value\n"
        "• `/econ_refresh_bank` — refresh bank dashboard\n"
    )
    await interaction.followup.send(msg, ephemeral=True)


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


@tree.command(name="econ_set_balance", description="(Staff) Set a character balance to an exact value.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(character="Character name", value="New balance (must be >= 0)")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_set_balance(interaction: discord.Interaction, character: str, value: int):
    await interaction.response.defer(ephemeral=True)

@tree.command(name="upgrade_asset", description="Upgrade an existing asset to a higher tier (deducts upgrade cost).")
@app_commands.autocomplete(character=character_autocomplete, asset=ac_asset_for_character, target_tier=ac_target_tier)
async def cmd_upgrade_asset(interaction: discord.Interaction, character: str, asset: str, target_tier: str):
    await interaction.response.defer(ephemeral=True)

    ok, dbg = await staff_check(interaction)
    if not ok:
        await interaction.followup.send(dbg, ephemeral=True)
        return

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
    if cost_val is None:
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

    try:
        await refresh_bank_messages()
    except Exception:
        pass

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


@tree.command(name="sell_asset", description="Sell/remove an asset (optional refund).")
@app_commands.autocomplete(character=character_autocomplete, asset=ac_asset_for_character)
@app_commands.describe(refund_percent="Optional refund percent of the asset's cumulative cost (0-100). Default 0.")
async def cmd_sell_asset(interaction: discord.Interaction, character: str, asset: str, refund_percent: Optional[int] = 0):
    await interaction.response.defer(ephemeral=True)

    ok, dbg = await staff_check(interaction)
    if not ok:
        await interaction.followup.send(dbg, ephemeral=True)
        return

    refund_percent = int(refund_percent or 0)
    if refund_percent < 0:
        refund_percent = 0
    if refund_percent > 100:
        refund_percent = 100

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
        base_cost = await cumulative_cost_to_tier(asset_type, tier)
        if base_cost is None:
            await interaction.followup.send("Unable to calculate refund amount for this asset.", ephemeral=True)
            return
        refund_amount = int(round((base_cost * refund_percent) / 100.0))

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

    try:
        await refresh_bank_messages()
    except Exception:
        pass

    msg = (
        f"Sold/removed asset from **{character}**:\n"
        f"- {asset_type} | {tier} | {asset_name}\n"
    )
    if refund_amount:
        msg += f"Refund: **{format_currency(refund_amount)}** ({refund_percent}%)\n"
    msg += f"New balance: **{format_currency(await get_balance(character))}**"

    await interaction.followup.send(msg, ephemeral=True)


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


@tree.command(name="econ_refresh_bank", description="(Staff) Refresh the bank dashboard messages.", guild=discord.Object(id=GUILD_ID))
@staff_only()
async def cmd_refresh_bank(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # Optionally allow BANK_REFRESH_ROLE_IDS to run refresh too, but staff_only already gates role-based access.
    try:
        await refresh_bank_dashboard(create_missing=True)
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

# -------------------------
# Startup / sync
# -------------------------

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
    await ensure_schema()
    await seed_asset_definitions()
    print(f"[test] Starting {APP_VERSION}…")
    print(f"[test] Logged in as {client.user} (commands guild: {GUILD_ID}; data guild: {DATA_GUILD_ID})")
    print(f"[debug] raw STAFF_ROLE_IDS env: {repr(_get('STAFF_ROLE_IDS',''))}")
    print(f"[debug] STAFF_ROLE_IDS_DEFAULT: {sorted(list(STAFF_ROLE_IDS_DEFAULT))}")
    print(f"[debug] STAFF_ROLE_IDS (effective): {sorted(list(STAFF_ROLE_IDS))}")

    # Delete global commands to remove duplicates
    await delete_all_global_commands()

    # Sync to guild only
    try:
        synced = await tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"[test] Synced {len(synced)} guild command(s).")
    except Exception as e:
        print(f"[warn] Guild sync failed: {e}")

    # Refresh bank (safe)
    try:
        await refresh_bank_dashboard(create_missing=True)
    except Exception as e:
        print(f"[warn] Initial bank refresh failed: {e}")


def main():
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
