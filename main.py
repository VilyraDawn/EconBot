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


APP_VERSION = "EconBot_v73"
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


# Approved env vars (per continuity doc)
DISCORD_TOKEN = _get("DISCORD_TOKEN")
DATABASE_URL = _get("DATABASE_URL")
GUILD_ID = _int("GUILD_ID")
LEGACY_SOURCE_GUILD_ID = _int("LEGACY_SOURCE_GUILD_ID", GUILD_ID)
BANK_CHANNEL_ID = _int("BANK_CHANNEL_ID")
ECON_LOG_CHANNEL_ID = _int("ECON_LOG_CHANNEL_ID")
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
    rows = await db_fetch(
        """
        SELECT DISTINCT asset_type
        FROM econ_asset_definitions
        ORDER BY asset_type ASC;
        """
    )
    return [str(r["asset_type"]) for r in rows]


async def list_tiers_for_type(asset_type: str) -> List[str]:
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
    # Bank shows balances grouped by character owner, WITHOUT pings/mentions.
    # We render display names (server nicknames) for visual appeal.
    chars = await db_fetch(
        """
        SELECT user_id, name
        FROM characters
        WHERE guild_id=$1 AND archived=FALSE
        ORDER BY user_id, name;
        """,
        DATA_GUILD_ID,
    )
    by_user: Dict[int, List[str]] = {}
    for r in chars:
        uid = int(r["user_id"])
        by_user.setdefault(uid, []).append(str(r["name"]))

    # Resolve display names
    name_cache: Dict[int, str] = {}

    async def display_name(uid: int) -> str:
        if uid in name_cache:
            return name_cache[uid]
        m = guild.get_member(uid)
        if m is None:
            try:
                m = await guild.fetch_member(uid)
            except Exception:
                m = None
        if m is None:
            name_cache[uid] = f"User {uid}"
        else:
            name_cache[uid] = m.display_name
        return name_cache[uid]

    # Build pretty markdown
    now = datetime.now(CHICAGO_TZ)
    header = f"🏦 **Bank of Vilyra** — {now.strftime('%Y-%m-%d %H:%M')} (Chicago)"
    lines: List[str] = [header, "━━━━━━━━━━━━━━━━━━", ""]

    if not by_user:
        return lines + ["No characters found in DB."]

    for uid in sorted(by_user.keys(), key=lambda x: (str(x))):
        dn = await display_name(uid)
        lines.append(f"**{dn}**")
        # Render characters in a neat table-like style
        for cname in by_user[uid]:
            bal = await get_balance(cname)
            lines.append(f"`{cname:<24}`  **{bal:>10,}**")
        lines.append("")  # spacer

    return lines


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
    bal = await get_balance(character)
    await interaction.followup.send(f"**{character}** balance: **{bal:,}**", ephemeral=True)


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
        f"Claimed **{daily_income:,}** daily income for **{character}**.\nNew balance: **{new_bal:,}**",
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
    new_bal = await adjust_balance(character, int(delta))
    await log_audit(interaction, "adjust_balance", {"character": character, "delta": int(delta), "new_balance": new_bal})
    await interaction.followup.send(f"Adjusted **{character}** by **{delta:,}**. New balance: **{new_bal:,}**", ephemeral=True)


@tree.command(name="econ_set_balance", description="(Staff) Set a character balance to an exact value.", guild=discord.Object(id=GUILD_ID))
@staff_only()
@app_commands.describe(character="Character name", value="New balance value")
@app_commands.autocomplete(character=character_autocomplete)
async def cmd_econ_set_balance(interaction: discord.Interaction, character: str, value: int):
    await interaction.response.defer(ephemeral=True)
    await set_balance(character, int(value))
    await log_audit(interaction, "set_balance", {"character": character, "value": int(value)})
    await interaction.followup.send(f"Set **{character}** balance to **{int(value):,}**.", ephemeral=True)


@tree.command(name="econ_refresh_bank", description="(Staff) Refresh the Bank dashboard channel messages.", guild=discord.Object(id=GUILD_ID))
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
    await interaction.response.defer(ephemeral=True)

    owner = await get_character_owner(character)
    if owner is None:
        await interaction.followup.send("Character not found in DB.", ephemeral=True)
        return

    # Validate asset definition exists
    adef = await get_asset_def(asset_type, tier)
    if not adef:
        await interaction.followup.send("Invalid asset type/tier (not found in asset definitions).", ephemeral=True)
        return
    cost_val, add_income_val = adef

    # Deduct cost (cumulative tier crossing not supported without tier order logic in definitions).
    # v38 doctrine says cumulative cost across tiers; if tier order is encoded in sheet as increasing tiers,
    # that logic should be in definitions. Here we treat cost_val as total-to-tier cost.
    cur_bal = await get_balance(character)
    if cur_bal < cost_val:
        await interaction.followup.send(f"Insufficient funds. Balance **{cur_bal:,}**, cost **{cost_val:,}**.", ephemeral=True)
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
        # uniqueness: (guild_id, user_id, character_name, asset_name)
        await interaction.followup.send(f"Failed to add asset (possibly duplicate asset name): {e}", ephemeral=True)
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
        f"• **{asset_type}** — Tier **{tier}** — **{asset_name}**\n"
        f"Cost: **{cost_val:,}** (new balance **{new_bal:,}**)\n"
        f"Daily income now: **{new_daily_income:,}**",
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
