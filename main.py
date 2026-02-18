import os
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
import asyncpg

# =========================
# Vilyra Economy Bot (V1) — Single-file version (EconBot_v4)
# =========================
#
# V4 change:
# - NO pings / mentions anywhere (no <@id>, no .mention usage)
#   Dashboard and logs will display usernames/IDs without pinging.
#
# V3 change retained:
# - Staff permission checks do not rely on member cache.
#
# V2 change retained:
# - LEGACY_SOURCE_GUILD_ID optional: read characters from main server while testing in test server.
#
# Commands:
# - /income      (once/day per character; +10 Val = 1 Arce)
# - /balance     (character balance card)
# - /econ_adjust (staff/admin add/subtract currency)
#
# Required ENV vars:
# - DISCORD_TOKEN
# - DATABASE_URL
# - GUILD_ID
# - BANK_CHANNEL_ID
# - ECON_LOG_CHANNEL_ID
# - STAFF_ROLE_IDS            (comma-separated role IDs)
#
# Optional ENV vars:
# - LEGACY_CHAR_SCHEMA        (default: public)
# - LEGACY_CHAR_TABLE         (default: characters)
# - LEGACY_SOURCE_GUILD_ID    (default: unset/0 => use current guild)
# - ENV                       (default: test)

TZ = ZoneInfo("America/Chicago")

# ---------- Config ----------
def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

DISCORD_TOKEN = _req("DISCORD_TOKEN")
DATABASE_URL = _req("DATABASE_URL")

GUILD_ID = int(_req("GUILD_ID"))
BANK_CHANNEL_ID = int(_req("BANK_CHANNEL_ID"))
ECON_LOG_CHANNEL_ID = int(_req("ECON_LOG_CHANNEL_ID"))

LEGACY_CHAR_SCHEMA = os.getenv("LEGACY_CHAR_SCHEMA", "public")
LEGACY_CHAR_TABLE = os.getenv("LEGACY_CHAR_TABLE", "characters")

# If set, bot will read legacy characters from this guild id (e.g., your main server),
# while still operating and writing economy data in the current/test guild.
LEGACY_SOURCE_GUILD_ID = int(os.getenv("LEGACY_SOURCE_GUILD_ID", "0"))

STAFF_ROLE_IDS = {
    int(x.strip())
    for x in os.getenv("STAFF_ROLE_IDS", "").split(",")
    if x.strip().isdigit()
}

ENV = os.getenv("ENV", "test")

# ---------- Currency helpers ----------
DENOMS = [
    ("Novir", 10_000),  # Mythic Crystal
    ("Orin",  1_000),   # Platinum
    ("Elsh",    100),   # Gold
    ("Arce",     10),   # Silver
    ("Cinth",     1),   # Copper
]

UNIT_MULTIPLIERS = {
    "VAL": 1,
    "CINTH": 1,
    "ARCE": 10,
    "ELSH": 100,
    "ORIN": 1_000,
    "NOVIR": 10_000,
}

def format_currency(val: int) -> str:
    if val == 0:
        return "0 Val"
    sign = "-" if val < 0 else ""
    n = abs(val)
    parts: list[str] = []
    for name, mult in DENOMS:
        q, n = divmod(n, mult)
        if q:
            parts.append(f"{q} {name}")
    return sign + ", ".join(parts)

def to_val(amount: int, unit: str) -> int:
    unit = unit.upper()
    if unit not in UNIT_MULTIPLIERS:
        raise ValueError(f"Unknown unit: {unit}")
    return amount * UNIT_MULTIPLIERS[unit]

# ---------- DB schema ----------
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS economy;

CREATE TABLE IF NOT EXISTS economy.transactions (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  character_name TEXT NOT NULL,
  character_user_id BIGINT NOT NULL,
  amount_val BIGINT NOT NULL,
  reason TEXT NOT NULL,
  actor_user_id BIGINT NOT NULL,
  kind TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_econ_tx_char
  ON economy.transactions (guild_id, character_name);

CREATE TABLE IF NOT EXISTS economy.income_claims (
  guild_id BIGINT NOT NULL,
  character_name TEXT NOT NULL,
  claim_date DATE NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (guild_id, character_name, claim_date)
);

CREATE TABLE IF NOT EXISTS economy.assets (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  character_name TEXT NOT NULL,
  asset_type TEXT NOT NULL,
  label TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS economy.meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""

META_BANK_MESSAGE_ID_KEY = "bank_message_id"

# ---------- DB pool ----------
_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool

# ---------- Legacy character access ----------
class LegacyCharacter:
    __slots__ = ("guild_id", "user_id", "name", "archived")
    def __init__(self, guild_id: int, user_id: int, name: str, archived: bool):
        self.guild_id = guild_id
        self.user_id = user_id
        self.name = name
        self.archived = archived

async def fetch_character_by_name(guild_id: int, name: str) -> LegacyCharacter | None:
    pool = await get_pool()
    q = f"""
    SELECT guild_id, user_id, name, archived
    FROM {LEGACY_CHAR_SCHEMA}.{LEGACY_CHAR_TABLE}
    WHERE guild_id = $1 AND name = $2
    LIMIT 1
    """
    async with pool.acquire() as con:
        row = await con.fetchrow(q, guild_id, name)
    if not row:
        return None
    return LegacyCharacter(
        guild_id=int(row["guild_id"]),
        user_id=int(row["user_id"]),
        name=str(row["name"]),
        archived=bool(row["archived"]),
    )

async def autocomplete_character_names(
    guild_id: int,
    current: str,
    *,
    requester_user_id: int,
    is_staff: bool,
    limit: int = 25,
) -> list[str]:
    """
    Players: only their own non-archived characters.
    Staff: all characters, including archived.
    """
    pool = await get_pool()
    base = f"""
    SELECT name
    FROM {LEGACY_CHAR_SCHEMA}.{LEGACY_CHAR_TABLE}
    WHERE guild_id = $1
      AND name ILIKE $2
    """
    params = [guild_id, f"%{current}%"]

    if not is_staff:
        base += " AND user_id = $3 AND archived = FALSE"
        params.append(requester_user_id)

    base += " ORDER BY name ASC LIMIT %d" % limit

    async with pool.acquire() as con:
        rows = await con.fetch(base, *params)
    return [str(r["name"]) for r in rows]

def legacy_guild_id(current_guild_id: int) -> int:
    return LEGACY_SOURCE_GUILD_ID or current_guild_id

# ---------- No-ping formatting helpers ----------
def user_label_from_cache(user_id: int) -> str:
    """
    No pings. If we have the user cached, show 'DisplayName (ID)'.
    Otherwise show 'User ID: <id>'.
    """
    u = bot.get_user(user_id)  # type: ignore[name-defined]
    if u:
        # u.name is stable and won't ping; display_name isn't available on User
        return f"{u.name} (ID {user_id})"
    return f"User ID {user_id}"

def actor_label(interaction: discord.Interaction) -> str:
    # No pings. Prefer display_name for guild members.
    if isinstance(interaction.user, discord.Member):
        return f"{interaction.user.display_name} (ID {interaction.user.id})"
    return f"{interaction.user.name} (ID {interaction.user.id})"  # type: ignore[union-attr]

# ---------- Permissions / logging ----------
def is_staff_member(interaction: discord.Interaction) -> bool:
    """
    Uses interaction.user roles (Member) instead of cache lookup.
    """
    if interaction.guild is None or interaction.user is None:
        return False

    if isinstance(interaction.user, discord.Member):
        member = interaction.user
    else:
        member = interaction.guild.get_member(interaction.user.id)

    if member is None:
        return False

    if member.guild_permissions.administrator:
        return True

    return any(role.id in STAFF_ROLE_IDS for role in getattr(member, "roles", []))

async def log_to_econ(bot_client: discord.Client, text: str) -> None:
    ch = bot_client.get_channel(ECON_LOG_CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        await ch.send(text)

# ---------- Bank dashboard ----------
async def _get_bank_message_id() -> int | None:
    pool = await get_pool()
    async with pool.acquire() as con:
        v = await con.fetchval("SELECT v FROM economy.meta WHERE k=$1", META_BANK_MESSAGE_ID_KEY)
    return int(v) if v else None

async def _set_bank_message_id(mid: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.meta (k, v) VALUES ($1, $2)
            ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v
            """,
            META_BANK_MESSAGE_ID_KEY, str(mid)
        )

async def render_bank_embed() -> discord.Embed:
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT character_user_id, character_name, COALESCE(SUM(amount_val),0) AS bal
            FROM economy.transactions
            WHERE guild_id=$1
            GROUP BY character_user_id, character_name
            ORDER BY character_user_id ASC, character_name ASC
            """,
            GUILD_ID
        )

    embed = discord.Embed(
        title="🏦 Bank of Vilyra",
        description="Character balances (Novir/Orin/Elsh/Arce/Cinth)."
    )

    if not rows:
        embed.add_field(name="Balances", value="_No economy data yet._", inline=False)
        return embed

    blocks: list[str] = []
    current_user: int | None = None
    current_lines: list[str] = []

    def flush():
        nonlocal current_user, current_lines
        if current_user is None:
            return
        header = user_label_from_cache(current_user)
        body = "\n".join(current_lines) if current_lines else "_No balances yet._"
        blocks.append(f"**{header}**\n{body}")
        current_user = None
        current_lines = []

    for r in rows:
        uid = int(r["character_user_id"])
        name = str(r["character_name"])
        bal = int(r["bal"])

        if current_user is None:
            current_user = uid
        if uid != current_user:
            flush()
            current_user = uid

        current_lines.append(f"• **{name}** — {format_currency(bal)}")

    flush()

    text = "\n\n".join(blocks)
    if len(text) > 3900:
        text = "Too many entries to show in one embed right now. (Paging comes next.)"

    embed.add_field(name="Balances", value=text, inline=False)
    return embed

async def update_bank_dashboard(bot_client: discord.Client) -> None:
    channel = bot_client.get_channel(BANK_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        return
    embed = await render_bank_embed()
    mid = await _get_bank_message_id()
    if mid:
        try:
            msg = await channel.fetch_message(mid)
            await msg.edit(embed=embed, content=None)
            return
        except Exception:
            pass
    msg = await channel.send(embed=embed)
    await _set_bank_message_id(msg.id)

# ---------- Discord bot ----------
INTENTS = discord.Intents.default()

class EconBot(discord.Client):
    def __init__(self):
        super().__init__(intents=INTENTS)
        self.tree = app_commands.CommandTree(self)
        self.guild_obj = discord.Object(id=GUILD_ID)

    async def setup_hook(self):
        pool = await get_pool()
        async with pool.acquire() as con:
            await con.execute(SCHEMA_SQL)
        await self.tree.sync(guild=self.guild_obj)

    async def on_ready(self):
        print(f"[{ENV}] Logged in as {self.user} (commands guild: {GUILD_ID}; legacy source guild: {LEGACY_SOURCE_GUILD_ID or 'current'})")

bot = EconBot()

async def character_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    staff = is_staff_member(interaction)
    legacy_gid = legacy_guild_id(interaction.guild.id)
    names = await autocomplete_character_names(
        legacy_gid,
        current,
        requester_user_id=interaction.user.id,
        is_staff=staff,
    )
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]

@bot.tree.command(
    name="income",
    description="Claim daily income (+10 Val / 1 Arce).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def income(interaction: discord.Interaction, character: str):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in the server.", ephemeral=True)

    staff = is_staff_member(interaction)
    legacy_gid = legacy_guild_id(interaction.guild.id)

    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    if legacy_char.archived and not staff:
        return await interaction.response.send_message("That character is archived.", ephemeral=True)

    if legacy_char.user_id != interaction.user.id and not staff:
        return await interaction.response.send_message(
            "You can only claim income for your own characters.",
            ephemeral=True,
        )

    today = datetime.now(TZ).date()
    current_gid = interaction.guild.id

    pool = await get_pool()
    async with pool.acquire() as con:
        try:
            await con.execute(
                """
                INSERT INTO economy.income_claims (guild_id, character_name, claim_date)
                VALUES ($1, $2, $3)
                """,
                current_gid,
                legacy_char.name,
                today,
            )
        except Exception:
            return await interaction.response.send_message(
                "Income already claimed for this character today.",
                ephemeral=True,
            )

        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7)
            """,
            current_gid,
            legacy_char.name,
            legacy_char.user_id,
            10,
            "Daily income (+10 Cinth / 1 Arce)",
            interaction.user.id,
            "income",
        )

        bal = await con.fetchval(
            """
            SELECT COALESCE(SUM(amount_val),0)
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            """,
            current_gid,
            legacy_char.name,
        )

    await interaction.response.send_message(
        f"✅ Income claimed for **{legacy_char.name}**: +1 Arce (10 Cinth).\n"
        f"New balance: **{format_currency(int(bal))}**",
        ephemeral=True,
    )

    await log_to_econ(bot, f"💰 /income by {actor_label(interaction)} → **{legacy_char.name}** (+1 Arce)")
    await update_bank_dashboard(bot)

@bot.tree.command(
    name="balance",
    description="Show a character balance card.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def balance(interaction: discord.Interaction, character: str):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in the server.", ephemeral=True)

    staff = is_staff_member(interaction)
    legacy_gid = legacy_guild_id(interaction.guild.id)

    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    if legacy_char.archived and not staff:
        return await interaction.response.send_message("That character is archived.", ephemeral=True)

    if legacy_char.user_id != interaction.user.id and not staff:
        return await interaction.response.send_message("You can only view your own characters.", ephemeral=True)

    current_gid = interaction.guild.id

    pool = await get_pool()
    async with pool.acquire() as con:
        bal = await con.fetchval(
            """
            SELECT COALESCE(SUM(amount_val),0)
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            """,
            current_gid,
            legacy_char.name,
        )
        assets = await con.fetch(
            """
            SELECT asset_type, label
            FROM economy.assets
            WHERE guild_id=$1 AND character_name=$2
            ORDER BY created_at ASC
            LIMIT 10
            """,
            current_gid,
            legacy_char.name,
        )

    e = discord.Embed(title="💳 Balance Card")
    e.add_field(name="Character", value=f"**{legacy_char.name}**", inline=True)
    e.add_field(name="Owner", value=user_label_from_cache(legacy_char.user_id), inline=True)
    e.add_field(name="Balance", value=f"**{format_currency(int(bal))}**", inline=False)

    if assets:
        lines = [f"• **{a['asset_type']}** — {a['label']}" for a in assets]
        e.add_field(name="Assets", value="\n".join(lines), inline=False)
    else:
        e.add_field(name="Assets", value="_None_", inline=False)

    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(
    name="econ_adjust",
    description="Admin: add/subtract currency from a character.",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    operation="Add or subtract",
    amount="Whole number amount",
    unit="Currency unit",
    reason="Required reason for audit log",
)
@app_commands.autocomplete(character=character_autocomplete)
@app_commands.choices(
    operation=[
        app_commands.Choice(name="Add", value="add"),
        app_commands.Choice(name="Subtract", value="sub"),
    ],
    unit=[
        app_commands.Choice(name="Cinth (1 Val)", value="CINTH"),
        app_commands.Choice(name="Arce (10 Val)", value="ARCE"),
        app_commands.Choice(name="Elsh (100 Val)", value="ELSH"),
        app_commands.Choice(name="Orin (1,000 Val)", value="ORIN"),
        app_commands.Choice(name="Novir (10,000 Val)", value="NOVIR"),
        app_commands.Choice(name="Val (base)", value="VAL"),
    ],
)
async def econ_adjust(
    interaction: discord.Interaction,
    character: str,
    operation: app_commands.Choice[str],
    amount: int,
    unit: app_commands.Choice[str],
    reason: str,
):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in the server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    if amount <= 0:
        return await interaction.response.send_message("Amount must be positive.", ephemeral=True)

    delta = to_val(amount, unit.value)
    if operation.value == "sub":
        delta = -delta

    current_gid = interaction.guild.id

    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            current_gid,
            legacy_char.name,
            legacy_char.user_id,
            delta,
            reason.strip(),
            interaction.user.id,
            "adjust",
            {"unit": unit.value, "amount": amount, "operation": operation.value},
        )

        bal = await con.fetchval(
            """
            SELECT COALESCE(SUM(amount_val),0)
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            """,
            current_gid,
            legacy_char.name,
        )

    sign = "+" if delta >= 0 else ""
    await interaction.response.send_message(
        f"✅ Updated **{legacy_char.name}**: {sign}{format_currency(delta)}\n"
        f"Reason: {reason}\n"
        f"New balance: **{format_currency(int(bal))}**",
        ephemeral=True,
    )

    await log_to_econ(
        bot,
        f"🛠️ /econ_adjust by {actor_label(interaction)} → **{legacy_char.name}** ({sign}{format_currency(delta)}): {reason}",
    )
    await update_bank_dashboard(bot)

def main():
    print(f"[{ENV}] Starting EconBot_v4…")
    bot.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
