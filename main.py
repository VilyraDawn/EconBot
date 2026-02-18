import os
import json
from datetime import datetime
import datetime as dt
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
import asyncpg
from discord.ext import tasks
import io
import csv
import asyncio

# =========================
# Vilyra Economy Bot — Single-file version (EconBot_v11)
# =========================
# This is a FULL, complete replacement main.py with the features we agreed on:
#
# ✅ Character source: existing legacy table (default public.characters)
#    - Uses character "name" as the unique ID within a guild
#    - Supports cross-guild testing via LEGACY_SOURCE_GUILD_ID
#
# ✅ Commands
#    - /income        (once/day per character; +10 Val = 1 Arce)
#    - /balance       (balance card)
#    - /econ_adjust   (staff/admin add/subtract)
#
# ✅ Bank dashboard
#    - Posts/edits a single embed message in BANK_CHANNEL_ID
#    - Lists balances grouped by owning user
#
# ✅ No pings / mentions anywhere
#    - No <@id>, no .mention in logs or dashboard
#
# ✅ Stable permission checks
#    - Uses interaction.user roles (no member cache reliance)
#
# ✅ JSONB metadata handling (asyncpg-safe)
#    - metadata is passed as JSON string with $8::jsonb
#
# ✅ Error handling
#    - Global app command error handler returns an ephemeral message (no silent timeouts)
#
# -------------------------
# Required ENV Vars:
#   DISCORD_TOKEN
#   DATABASE_URL
#   GUILD_ID
#   BANK_CHANNEL_ID
#   ECON_LOG_CHANNEL_ID
#   STAFF_ROLE_IDS              comma-separated role IDs
#
# Optional ENV Vars:
#   LEGACY_CHAR_SCHEMA          default: public
#   LEGACY_CHAR_TABLE           default: characters
#   LEGACY_SOURCE_GUILD_ID      default: 0 (use current guild)
#   ENV                         default: test
# =========================

TZ = ZoneInfo("America/Chicago")

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
LEGACY_SOURCE_GUILD_ID = int(os.getenv("LEGACY_SOURCE_GUILD_ID", "0"))

STAFF_ROLE_IDS = {
    int(x.strip())
    for x in os.getenv("STAFF_ROLE_IDS", "").split(",")
    if x.strip().isdigit()
}

ENV = os.getenv("ENV", "test")

DAILY_EXPORT_ENABLED = os.getenv("DAILY_EXPORT_ENABLED", "true").lower() in {"1","true","yes","y","on"}
DAILY_EXPORT_TIME_HHMM = os.getenv("DAILY_EXPORT_TIME_HHMM", "04:05")  # America/Chicago
DAILY_EXPORT_SCOPE = os.getenv("DAILY_EXPORT_SCOPE", "yesterday")  # "yesterday" or "all"

ASSET_CATALOG_SEED = [
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 1, "tier_name": "Apprentice", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 2, "tier_name": "Journeyman", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 3, "tier_name": "Leased Workshop", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 4, "tier_name": "Small Workshop", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Guild Trade Workshop", "tier": 5, "tier_name": "Large Workshop", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 1, "tier_name": "Consignment Arrangement", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 2, "tier_name": "Small Alley Stand", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 3, "tier_name": "Market Stall", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 4, "tier_name": "Small Shop", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Market Stall", "tier": 5, "tier_name": "Large Shop", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 1, "tier_name": "Subsistence Surplus", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 2, "tier_name": "Leased Fields", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 3, "tier_name": "Owned Acre", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 4, "tier_name": "Small Fields and Barn", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Farm/Ranch", "tier": 5, "tier_name": "Large Fields and Barn", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 1, "tier_name": "One-Room Flophouse", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 2, "tier_name": "Leased Establishment", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 3, "tier_name": "Small Tavern", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 4, "tier_name": "Large Tavern", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Tavern/Inn", "tier": 5, "tier_name": "Large Tavern and Inn", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 1, "tier_name": "Small Storage Shed", "cost_val": 300, "income_val": 50},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 2, "tier_name": "Large Storage Shed", "cost_val": 600, "income_val": 100},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 3, "tier_name": "Small Trading Post", "cost_val": 1200, "income_val": 150},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 4, "tier_name": "Large Trading Post", "cost_val": 2000, "income_val": 200},
    {"asset_type": "Business", "secondary_type": "Warehouse/Trade House", "tier": 5, "tier_name": "Large Warehouse and Trading Post", "cost_val": 3000, "income_val": 250},
    {"asset_type": "Holdings", "secondary_type": "House", "tier": 1, "tier_name": "Shack", "cost_val": 600, "income_val": 0},
    {"asset_type": "Holdings", "secondary_type": "House", "tier": 2, "tier_name": "Hut", "cost_val": 1200, "income_val": 0},
    {"asset_type": "Holdings", "secondary_type": "House", "tier": 3, "tier_name": "House", "cost_val": 2000, "income_val": 0},
    {"asset_type": "Holdings", "secondary_type": "House", "tier": 4, "tier_name": "Lodge", "cost_val": 3000, "income_val": 0},
    {"asset_type": "Holdings", "secondary_type": "House", "tier": 5, "tier_name": "Mansion", "cost_val": 5000, "income_val": 0},
    {"asset_type": "Holdings", "secondary_type": "Village", "tier": 1, "tier_name": "Chartered Assembly", "cost_val": 1200, "income_val": 100},
    {"asset_type": "Holdings", "secondary_type": "Village", "tier": 2, "tier_name": "Hamlet", "cost_val": 2400, "income_val": 200},
    {"asset_type": "Holdings", "secondary_type": "Village", "tier": 3, "tier_name": "Village", "cost_val": 4800, "income_val": 300},
    {"asset_type": "Holdings", "secondary_type": "Village", "tier": 4, "tier_name": "Town", "cost_val": 9600, "income_val": 400},
    {"asset_type": "Holdings", "secondary_type": "Village", "tier": 5, "tier_name": "Small City", "cost_val": 15000, "income_val": 500},
    {"asset_type": "Enchantments", "secondary_type": "Weapons", "tier": 1, "tier_name": "Hit +1 / Dmg +1d4", "cost_val": 300, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Weapons", "tier": 2, "tier_name": "Hit +1 / Dmg +1d6", "cost_val": 600, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Weapons", "tier": 3, "tier_name": "Hit +2 / Dmg +1d8", "cost_val": 1200, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Weapons", "tier": 4, "tier_name": "Hit +2 / Dmg +1d10", "cost_val": 2400, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Weapons", "tier": 5, "tier_name": "Hit +2 / Dmg +1d12", "cost_val": 4800, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Armor", "tier": 1, "tier_name": "AC +1", "cost_val": 300, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Armor", "tier": 2, "tier_name": "AC +2", "cost_val": 600, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Armor", "tier": 3, "tier_name": "AC +2 / Adv Magic Atk", "cost_val": 1200, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Armor", "tier": 4, "tier_name": "AC +2 / Adv Magic and Melee Atk", "cost_val": 2400, "income_val": 0},
    {"asset_type": "Enchantments", "secondary_type": "Armor", "tier": 5, "tier_name": "AC +3 / Adv Magic and Melee Atk", "cost_val": 4800, "income_val": 0},
]

# -------------------------
# Currency
# -------------------------
DENOMS = [
    ("Novir", 10_000),  # Mythic Crystal
    ("Orin", 1_000),    # Platinum
    ("Elsh", 100),      # Gold
    ("Arce", 10),       # Silver
    ("Cinth", 1),       # Copper
]

UNIT_MULTIPLIERS = {
    "VAL": 1,
    "CINTH": 1,
    "ARCE": 10,
    "ELSH": 100,
    "ORIN": 1_000,
    "NOVIR": 10_000,
}

ASSET_CATALOG = [
  {
    "asset_type": "Business",
    "secondary_type": "Guild Trade Workshop",
    "tier": 1,
    "tier_name": "Apprentice",
    "cost_val": 300,
    "income_val": 50
  },
  {
    "asset_type": "Business",
    "secondary_type": "Guild Trade Workshop",
    "tier": 2,
    "tier_name": "Journeyman",
    "cost_val": 600,
    "income_val": 100
  },
  {
    "asset_type": "Business",
    "secondary_type": "Guild Trade Workshop",
    "tier": 3,
    "tier_name": "Leased Workshop",
    "cost_val": 1200,
    "income_val": 150
  },
  {
    "asset_type": "Business",
    "secondary_type": "Guild Trade Workshop",
    "tier": 4,
    "tier_name": "Small Workshop",
    "cost_val": 2000,
    "income_val": 200
  },
  {
    "asset_type": "Business",
    "secondary_type": "Guild Trade Workshop",
    "tier": 5,
    "tier_name": "Large Workshop",
    "cost_val": 3000,
    "income_val": 250
  },
  {
    "asset_type": "Business",
    "secondary_type": "Market Stall",
    "tier": 1,
    "tier_name": "Consignment Arrangement",
    "cost_val": 300,
    "income_val": 50
  },
  {
    "asset_type": "Business",
    "secondary_type": "Market Stall",
    "tier": 2,
    "tier_name": "Small Alley Stand",
    "cost_val": 600,
    "income_val": 100
  },
  {
    "asset_type": "Business",
    "secondary_type": "Market Stall",
    "tier": 3,
    "tier_name": "Market Stall",
    "cost_val": 1200,
    "income_val": 150
  },
  {
    "asset_type": "Business",
    "secondary_type": "Market Stall",
    "tier": 4,
    "tier_name": "Small Shop",
    "cost_val": 2000,
    "income_val": 200
  },
  {
    "asset_type": "Business",
    "secondary_type": "Market Stall",
    "tier": 5,
    "tier_name": "Large Shop",
    "cost_val": 3000,
    "income_val": 250
  },
  {
    "asset_type": "Business",
    "secondary_type": "Farm/Ranch",
    "tier": 1,
    "tier_name": "Subsistence Surplus",
    "cost_val": 300,
    "income_val": 50
  },
  {
    "asset_type": "Business",
    "secondary_type": "Farm/Ranch",
    "tier": 2,
    "tier_name": "Leased Fields",
    "cost_val": 600,
    "income_val": 100
  },
  {
    "asset_type": "Business",
    "secondary_type": "Farm/Ranch",
    "tier": 3,
    "tier_name": "Owned Acre",
    "cost_val": 1200,
    "income_val": 150
  },
  {
    "asset_type": "Business",
    "secondary_type": "Farm/Ranch",
    "tier": 4,
    "tier_name": "Small Fields and Barn",
    "cost_val": 2000,
    "income_val": 200
  },
  {
    "asset_type": "Business",
    "secondary_type": "Farm/Ranch",
    "tier": 5,
    "tier_name": "Large Fields and Barn",
    "cost_val": 3000,
    "income_val": 250
  },
  {
    "asset_type": "Business",
    "secondary_type": "Tavern/Inn",
    "tier": 1,
    "tier_name": "One-Room Flophouse",
    "cost_val": 300,
    "income_val": 50
  },
  {
    "asset_type": "Business",
    "secondary_type": "Tavern/Inn",
    "tier": 2,
    "tier_name": "Leased Establishment",
    "cost_val": 600,
    "income_val": 100
  },
  {
    "asset_type": "Business",
    "secondary_type": "Tavern/Inn",
    "tier": 3,
    "tier_name": "Small Tavern",
    "cost_val": 1200,
    "income_val": 150
  },
  {
    "asset_type": "Business",
    "secondary_type": "Tavern/Inn",
    "tier": 4,
    "tier_name": "Large Tavern",
    "cost_val": 2000,
    "income_val": 200
  },
  {
    "asset_type": "Business",
    "secondary_type": "Tavern/Inn",
    "tier": 5,
    "tier_name": "Large Tavern and Inn",
    "cost_val": 3000,
    "income_val": 250
  },
  {
    "asset_type": "Business",
    "secondary_type": "Warehouse/Trade House",
    "tier": 1,
    "tier_name": "Small Storage Shed",
    "cost_val": 300,
    "income_val": 50
  },
  {
    "asset_type": "Business",
    "secondary_type": "Warehouse/Trade House",
    "tier": 2,
    "tier_name": "Large Storage Shed",
    "cost_val": 600,
    "income_val": 100
  },
  {
    "asset_type": "Business",
    "secondary_type": "Warehouse/Trade House",
    "tier": 3,
    "tier_name": "Small Trading Post",
    "cost_val": 1200,
    "income_val": 150
  },
  {
    "asset_type": "Business",
    "secondary_type": "Warehouse/Trade House",
    "tier": 4,
    "tier_name": "Large Trading Post",
    "cost_val": 2000,
    "income_val": 200
  },
  {
    "asset_type": "Business",
    "secondary_type": "Warehouse/Trade House",
    "tier": 5,
    "tier_name": "Large Warehouse and Trading Post",
    "cost_val": 3000,
    "income_val": 250
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "House",
    "tier": 1,
    "tier_name": "Shack",
    "cost_val": 600,
    "income_val": 0
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "House",
    "tier": 2,
    "tier_name": "Hut",
    "cost_val": 1200,
    "income_val": 0
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "House",
    "tier": 3,
    "tier_name": "House",
    "cost_val": 2000,
    "income_val": 0
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "House",
    "tier": 4,
    "tier_name": "Lodge",
    "cost_val": 3000,
    "income_val": 0
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "House",
    "tier": 5,
    "tier_name": "Mansion",
    "cost_val": 5000,
    "income_val": 0
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "Village",
    "tier": 1,
    "tier_name": "Chartered Assembly",
    "cost_val": 1200,
    "income_val": 100
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "Village",
    "tier": 2,
    "tier_name": "Hamlet",
    "cost_val": 2400,
    "income_val": 200
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "Village",
    "tier": 3,
    "tier_name": "Village",
    "cost_val": 4800,
    "income_val": 300
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "Village",
    "tier": 4,
    "tier_name": "Town",
    "cost_val": 9600,
    "income_val": 400
  },
  {
    "asset_type": "Holdings",
    "secondary_type": "Village",
    "tier": 5,
    "tier_name": "Small City",
    "cost_val": 15000,
    "income_val": 500
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Weapons",
    "tier": 1,
    "tier_name": "Hit +1 / Dmg +1d4",
    "cost_val": 300,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Weapons",
    "tier": 2,
    "tier_name": "Hit +1 / Dmg +1d6",
    "cost_val": 600,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Weapons",
    "tier": 3,
    "tier_name": "Hit +2 / Dmg +1d8",
    "cost_val": 1200,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Weapons",
    "tier": 4,
    "tier_name": "Hit +2 / Dmg +1d10",
    "cost_val": 2400,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Weapons",
    "tier": 5,
    "tier_name": "Hit +2 / Dmg +1d12",
    "cost_val": 4800,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Armor",
    "tier": 1,
    "tier_name": "AC +1",
    "cost_val": 300,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Armor",
    "tier": 2,
    "tier_name": "AC +2",
    "cost_val": 600,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Armor",
    "tier": 3,
    "tier_name": "AC +2 / Adv Magic Atk",
    "cost_val": 1200,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Armor",
    "tier": 4,
    "tier_name": "AC +2 / Adv Magic and Melee Atk",
    "cost_val": 2400,
    "income_val": 0
  },
  {
    "asset_type": "Enchantments",
    "secondary_type": "Armor",
    "tier": 5,
    "tier_name": "AC +3 / Adv Magic and Melee Atk",
    "cost_val": 4800,
    "income_val": 0
  }
]

def format_currency(val: int) -> str:
    """Format an integer amount in base Val into mixed denominations.

    - Uses *exact* integer math (no rounding).
    - Uses the fewest denominations possible (greedy from largest to smallest),
      but includes as many as needed to represent the exact value.
    """
    if val == 0:
        return "0 Val"

    sign = "-" if val < 0 else ""
    n = abs(int(val))

    parts: list[str] = []
    for name, mult in DENOMS:
        q, n = divmod(n, mult)  # integer div/mod => never rounds
        if q:
            parts.append(f"{q} {name}")

    # Should never happen because Cinth is 1, but keep a safe fallback
    if not parts:
        parts.append(f"{abs(int(val))} Cinth")

    return sign + ", ".join(parts)

def to_val(amount: int, unit: str) -> int:
    u = unit.upper()
    if u not in UNIT_MULTIPLIERS:
        raise ValueError(f"Unknown unit: {unit}")
    return amount * UNIT_MULTIPLIERS[u]

# -------------------------
# Concurrency + overdraft guard
# -------------------------
def _advisory_key(guild_id: int, character_name: str) -> int:
    """Stable 64-bit advisory lock key for a character within a guild."""
    import hashlib
    h = hashlib.sha256(f"{guild_id}:{character_name}".encode("utf-8")).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=True)

async def ensure_can_debit(
    con: asyncpg.Connection,
    guild_id: int,
    character_name: str,
    debit_val: int,
) -> tuple[bool, int, int]:
    """Return (ok, current_balance_val, shortfall_val)."""
    cur = await con.fetchval(
        """
        SELECT COALESCE(SUM(amount_val), 0)
        FROM economy.transactions
        WHERE guild_id = $1 AND character_name = $2
        """,
        guild_id,
        character_name,
    )
    cur = int(cur or 0)
    debit_val = int(debit_val or 0)
    if debit_val <= 0:
        return True, cur, 0
    if cur >= debit_val:
        return True, cur, 0
    return False, cur, debit_val - cur

# -------------------------
# DB schema (economy)
# -------------------------
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


CREATE TABLE IF NOT EXISTS economy.asset_catalog (
  asset_type TEXT NOT NULL,
  secondary_type TEXT NOT NULL,
  tier INT NOT NULL,
  tier_name TEXT NOT NULL,
  cost_val BIGINT NOT NULL,
  income_val BIGINT NOT NULL,
  PRIMARY KEY (asset_type, secondary_type, tier)
);

CREATE TABLE IF NOT EXISTS economy.asset_rules (
  asset_type TEXT NOT NULL,
  secondary_type TEXT NOT NULL,
  stackable BOOLEAN NOT NULL DEFAULT FALSE,
  max_instances INT NOT NULL DEFAULT 1,
  PRIMARY KEY (asset_type, secondary_type)
);

CREATE TABLE IF NOT EXISTS economy.character_assets (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  character_name TEXT NOT NULL,
  instance_no INT NOT NULL DEFAULT 1,
  asset_type TEXT NOT NULL,
  secondary_type TEXT NOT NULL,
  custom_name TEXT NOT NULL DEFAULT '',
  tier INT NOT NULL,
  tier_name TEXT NOT NULL,
  income_val BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (guild_id, character_name, asset_type, secondary_type, instance_no)
);

CREATE INDEX IF NOT EXISTS idx_character_assets_char
  ON economy.character_assets (guild_id, character_name);

CREATE TABLE IF NOT EXISTS economy.meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""

META_BANK_MESSAGE_ID_KEY = "bank_message_id"
META_LAST_EXPORT_DATE_KEY = "last_export_date"

# -------------------------
# DB pool
# -------------------------
_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool
async def upsert_asset_catalog(con: asyncpg.Connection) -> None:
    """Upsert the static asset catalog into DB so commands can reference it."""
    rows = [
        (a["asset_type"], a["secondary_type"], int(a["tier"]), str(a["tier_name"]), int(a["cost_val"]), int(a["income_val"]))
        for a in ASSET_CATALOG
    ]
    await con.executemany(
        """
        INSERT INTO economy.asset_catalog (asset_type, secondary_type, tier, tier_name, cost_val, income_val)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (asset_type, secondary_type, tier)
        DO UPDATE SET tier_name = EXCLUDED.tier_name,
                      cost_val = EXCLUDED.cost_val,
                      income_val = EXCLUDED.income_val
        """,
        rows,
    )

async def ensure_asset_rules(con: asyncpg.Connection) -> None:
    """Ensure every asset chain has at least a default rule row (non-stackable, max_instances=1)."""
    chains = {(a["asset_type"], a["secondary_type"]) for a in ASSET_CATALOG}
    rows = [(at, st) for at, st in sorted(chains)]
    await con.executemany(
        """
        INSERT INTO economy.asset_rules (asset_type, secondary_type)
        VALUES ($1, $2)
        ON CONFLICT (asset_type, secondary_type) DO NOTHING
        """,
        rows,
    )

async def get_asset_rule(con: asyncpg.Connection, asset_type: str, secondary_type: str) -> tuple[bool, int]:
    row = await con.fetchrow(
        """
        SELECT stackable, max_instances
        FROM economy.asset_rules
        WHERE asset_type=$1 AND secondary_type=$2
        """,
        asset_type,
        secondary_type,
    )
    if not row:
        return (False, 1)
    return (bool(row["stackable"]), int(row["max_instances"]))

async def get_catalog_row(con: asyncpg.Connection, asset_type: str, secondary_type: str, tier: int):
    return await con.fetchrow(
        """
        SELECT asset_type, secondary_type, tier, tier_name, cost_val, income_val
        FROM economy.asset_catalog
        WHERE asset_type=$1 AND secondary_type=$2 AND tier=$3
        """,
        asset_type,
        secondary_type,
        int(tier),
    )



# -------------------------
# Legacy characters (source)
# -------------------------
class LegacyCharacter:
    __slots__ = ("guild_id", "user_id", "name", "archived")
    def __init__(self, guild_id: int, user_id: int, name: str, archived: bool):
        self.guild_id = guild_id
        self.user_id = user_id
        self.name = name
        self.archived = archived

def legacy_guild_id(current_guild_id: int) -> int:
    return LEGACY_SOURCE_GUILD_ID or current_guild_id

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
    requester_user_id: int,
    is_staff: bool,
    limit: int = 25,
) -> list[str]:
    pool = await get_pool()
    sql = f"""
    SELECT name
    FROM {LEGACY_CHAR_SCHEMA}.{LEGACY_CHAR_TABLE}
    WHERE guild_id = $1
      AND name ILIKE $2
    """
    params: list[object] = [guild_id, f"%{current}%"]

    if not is_staff:
        sql += " AND user_id = $3 AND archived = FALSE"
        params.append(requester_user_id)

    sql += " ORDER BY name ASC LIMIT %d" % limit

    async with pool.acquire() as con:
        rows = await con.fetch(sql, *params)

    return [str(r["name"]) for r in rows]

# -------------------------
# No-ping / labels
# -------------------------
def actor_label(interaction: discord.Interaction) -> str:
    # No pings. Use display_name if Member.
    if isinstance(interaction.user, discord.Member):
        return f"{interaction.user.display_name} (ID {interaction.user.id})"
    # Fallback
    return f"User ID {interaction.user.id}"

def user_label(bot_client: discord.Client, user_id: int) -> str:
    # No pings. Use cached username if available.
    u = bot_client.get_user(user_id)
    if u is not None:
        return f"{u.name} (ID {user_id})"
    return f"User ID {user_id}"

# -------------------------
# Permissions
# -------------------------
def is_staff_member(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        return False

    if isinstance(interaction.user, discord.Member):
        member = interaction.user
    else:
        # In practice, guild app commands provide Member; this is just a fallback.
        member = interaction.guild.get_member(interaction.user.id)

    if member is None:
        return False

    if member.guild_permissions.administrator:
        return True

    return any(role.id in STAFF_ROLE_IDS for role in getattr(member, "roles", []))

# -------------------------
# Logging
# -------------------------
async def log_to_econ(bot_client: discord.Client, text: str) -> None:
    ch = bot_client.get_channel(ECON_LOG_CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        await ch.send(text)
async def export_transactions_csv_bytes(
    *,
    guild_id: int,
    scope: str = "yesterday",
) -> tuple[bytes, str]:
    """
    Returns (csv_bytes, filename).

    scope:
      - "yesterday": export only the prior local day in America/Chicago
      - "all": export entire ledger for that guild
    """
    pool = await get_pool()

    now_local = datetime.now(TZ)
    if scope == "yesterday":
        day = (now_local.date() - dt.timedelta(days=1))
        start = dt.datetime.combine(day, dt.time(0, 0), tzinfo=TZ)
        end = start + dt.timedelta(days=1)

        sql = """
        SELECT id, guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, created_at, metadata
        FROM economy.transactions
        WHERE guild_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at ASC, id ASC
        """
        args = (guild_id, start.astimezone(dt.timezone.utc), end.astimezone(dt.timezone.utc))
        filename = f"econ_transactions_{guild_id}_{day.isoformat()}.csv"
    else:
        sql = """
        SELECT id, guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, created_at, metadata
        FROM economy.transactions
        WHERE guild_id = $1
        ORDER BY created_at ASC, id ASC
        """
        args = (guild_id,)
        filename = f"econ_transactions_{guild_id}_ALL_{now_local.date().isoformat()}.csv"

    async with pool.acquire() as con:
        rows = await con.fetch(sql, *args)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "id", "guild_id", "character_name", "character_user_id",
        "amount_val", "reason", "actor_user_id", "kind", "created_at_utc", "metadata_json"
    ])

    for r in rows:
        meta = r["metadata"]
        # asyncpg returns jsonb as dict; serialize safely
        meta_json = json.dumps(meta, ensure_ascii=False) if isinstance(meta, (dict, list)) else (meta or "")
        created = r["created_at"]
        created_utc = created.astimezone(dt.timezone.utc).isoformat() if created else ""
        writer.writerow([
            int(r["id"]),
            int(r["guild_id"]),
            str(r["character_name"]),
            int(r["character_user_id"]),
            int(r["amount_val"]),
            str(r["reason"]),
            int(r["actor_user_id"]),
            str(r["kind"]),
            created_utc,
            meta_json,
        ])

    csv_bytes = buf.getvalue().encode("utf-8")
    return csv_bytes, filename

async def send_export_to_econ_log(
    bot_client: discord.Client,
    *,
    guild_id: int,
    scope: str,
    note: str,
) -> None:
    csv_bytes, filename = await export_transactions_csv_bytes(guild_id=guild_id, scope=scope)
    ch = bot_client.get_channel(ECON_LOG_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return

    file = discord.File(fp=io.BytesIO(csv_bytes), filename=filename)
    await ch.send(content=note, file=file)



# -------------------------
# Bank dashboard
# -------------------------
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
            ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
            """,
            META_BANK_MESSAGE_ID_KEY,
            str(mid),
        )


async def _get_last_export_date() -> str | None:
    """Returns YYYY-MM-DD (America/Chicago) of the last successful scheduled export."""
    pool = await get_pool()
    async with pool.acquire() as con:
        v = await con.fetchval("SELECT v FROM economy.meta WHERE k=$1", META_LAST_EXPORT_DATE_KEY)
    return str(v) if v else None

async def _set_last_export_date(date_str: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.meta (k, v) VALUES ($1, $2)
            ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
            """,
            META_LAST_EXPORT_DATE_KEY,
            date_str,
        )

async def render_bank_embed(bot_client: discord.Client) -> discord.Embed:
    pool = await get_pool()
    async with pool.acquire() as con:
        bal_rows = await con.fetch(
            """
            SELECT character_user_id, character_name, COALESCE(SUM(amount_val), 0)::bigint AS bal
            FROM economy.transactions
            WHERE guild_id = $1
            GROUP BY character_user_id, character_name
            ORDER BY character_user_id ASC, character_name ASC
            """,
            GUILD_ID,
        )

        asset_rows = await con.fetch(
            """
            SELECT character_name, secondary_type, custom_name, tier, instance_no
            FROM economy.character_assets
            WHERE guild_id = $1
            ORDER BY character_name ASC, secondary_type ASC, instance_no ASC
            """,
            GUILD_ID,
        )

    assets_by_char: dict[str, list[tuple[str,str,int,int]]] = {}
    for r in asset_rows:
        cname = str(r["character_name"])
        assets_by_char.setdefault(cname, []).append(
            (str(r["secondary_type"]), str(r["custom_name"] or ""), int(r["tier"]), int(r["instance_no"]))
        )

    embed = discord.Embed(
        title="🏦 Bank of Vilyra",
        description="Character balances and assets (no pings).",
    )

    if not bal_rows:
        embed.add_field(name="Balances", value="_No economy data yet._", inline=False)
        return embed

    blocks: list[str] = []
    current_uid: int | None = None
    current_lines: list[str] = []

    def flush():
        nonlocal current_uid, current_lines
        if current_uid is None:
            return
        header = user_label(bot_client, current_uid)
        body = "\n".join(current_lines) if current_lines else "_No balances yet._"
        blocks.append(f"**{header}**\n{body}")
        current_uid = None
        current_lines = []

    for r in bal_rows:
        uid = int(r["character_user_id"])
        name = str(r["character_name"])
        bal = int(r["bal"])

        if current_uid is None:
            current_uid = uid
        if uid != current_uid:
            flush()
            current_uid = uid

        line = f"• **{name}** — {format_currency(bal)}"
        aset = assets_by_char.get(name) or []
        if aset:
            # show compact list; subtype + custom name only, with tier
            parts = []
            for (subtype, nm, tier, inst) in aset[:6]:
                disp = nm.strip() if nm and nm.strip() else "(unnamed)"
                inst_suffix = f" [#{inst}]" if inst > 1 else ""
                parts.append(f"{subtype} — {disp}{inst_suffix} (T{tier})")
            more = ""
            if len(aset) > 6:
                more = f" +{len(aset)-6} more"
            line += f"\n  Assets: " + "; ".join(parts) + more
        current_lines.append(line)

    flush()

    text = "\n\n".join(blocks)
    if len(text) > 3900:
        text = "Too many entries to show in one embed right now. (Paging can be added next.)"

    embed.add_field(name="Accounts", value=text, inline=False)
    return embed

    blocks: list[str] = []
    current_uid: int | None = None
    current_lines: list[str] = []

    def flush():
        nonlocal current_uid, current_lines
        if current_uid is None:
            return
        header = user_label(bot_client, current_uid)
        body = "\n".join(current_lines) if current_lines else "_No balances yet._"
        blocks.append(f"**{header}**\n{body}")
        current_uid = None
        current_lines = []

    for r in rows:
        uid = int(r["character_user_id"])
        name = str(r["character_name"])
        bal = int(r["bal"])

        if current_uid is None:
            current_uid = uid
        if uid != current_uid:
            flush()
            current_uid = uid

        current_lines.append(f"• **{name}** — {format_currency(bal)}")

    flush()

    text = "\n\n".join(blocks)
    if len(text) > 3900:
        text = "Too many entries to show in one embed right now. (Paging can be added next.)"

    embed.add_field(name="Balances", value=text, inline=False)
    return embed

async def update_bank_dashboard(bot_client: discord.Client) -> None:
    channel = bot_client.get_channel(BANK_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        return

    embed = await render_bank_embed(bot_client)
    mid = await _get_bank_message_id()

    if mid:
        try:
            msg = await channel.fetch_message(mid)
            await msg.edit(embed=embed, content=None)
            return
        except Exception:
            # Message deleted or fetch failed; recreate
            pass

    msg = await channel.send(embed=embed)
    await _set_bank_message_id(msg.id)

# -------------------------
# Discord client
# -------------------------
INTENTS = discord.Intents.default()

class EconBot(discord.Client):
    def __init__(self):
        super().__init__(intents=INTENTS)
        self.tree = app_commands.CommandTree(self)
        self._guild_obj = discord.Object(id=GUILD_ID)

    async def setup_hook(self):
        pool = await get_pool()
        async with pool.acquire() as con:
            await con.execute(SCHEMA_SQL)

        # Lightweight migrations (safe to re-run)
        async with pool.acquire() as con:
            await con.execute("ALTER TABLE IF EXISTS economy.character_assets ADD COLUMN IF NOT EXISTS custom_name TEXT NOT NULL DEFAULT ''")
            await con.execute("ALTER TABLE IF EXISTS economy.assets ADD COLUMN IF NOT EXISTS instance_no INT NOT NULL DEFAULT 1")
            await upsert_asset_catalog(con)
            await ensure_asset_rules(con)
        await self.tree.sync(guild=self._guild_obj)

    async def on_ready(self):
        print(f"[{ENV}] Logged in as {self.user} (commands guild: {GUILD_ID}; legacy source guild: {LEGACY_SOURCE_GUILD_ID or 'current'})")

        if DAILY_EXPORT_ENABLED and not self.scheduled_export_loop.is_running():
            self.scheduled_export_loop.start()

    @tasks.loop(minutes=1)
    async def scheduled_export_loop(self):
        """Once per day at DAILY_EXPORT_TIME_HHMM (America/Chicago), export transactions to Econ Log."""
        try:
            now_local = datetime.now(TZ)
            hhmm = now_local.strftime("%H:%M")

            # Only run at the exact configured minute
            if hhmm != DAILY_EXPORT_TIME_HHMM:
                return

            today = now_local.date().isoformat()
            last = await _get_last_export_date()
            if last == today:
                return  # already exported today

            scope = DAILY_EXPORT_SCOPE.lower().strip()
            if scope not in {"yesterday", "all"}:
                scope = "yesterday"

            note = f"📦 Daily economy export ({scope}) — {today} (America/Chicago). No pings."
            await send_export_to_econ_log(self, guild_id=GUILD_ID, scope=scope, note=note)
            await _set_last_export_date(today)

        except Exception as e:
            # Log failure without crashing the bot
            try:
                await log_to_econ(self, f"⚠️ Daily export failed: {type(e).__name__}: {e}")
            except Exception:
                pass

bot = EconBot()

# Error handler to avoid "application did not respond"
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        msg = f"❌ Command error: {type(error).__name__}: {error}"
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass

    try:
        await log_to_econ(bot, f"⚠️ Command error by {actor_label(interaction)}: {type(error).__name__}: {error}")
    except Exception:
        pass

async def character_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []


async def tier_name_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete tier_name from economy.asset_catalog filtered by selected asset_type."""
    try:
        asset_type = None
        for opt in interaction.data.get("options", []):
            if opt.get("name") == "asset_type":
                asset_type = opt.get("value")
                break
        if not asset_type:
            return []
        pool = await get_pool()
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT DISTINCT tier_name
                FROM economy.asset_catalog
                WHERE secondary_type=$1 AND tier_name ILIKE $2
                ORDER BY tier_name ASC
                LIMIT 25
                """,
                str(asset_type),
                f"%{current}%",
            )
        return [app_commands.Choice(name=str(r["tier_name"]), value=str(r["tier_name"])) for r in rows]
    except Exception:
        return []

async def owned_asset_autocomplete(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    """Autocomplete owned asset names when upgrading."""
    if interaction.guild is None:
        return []
    char_name = getattr(interaction.namespace, "character", None)
    if not isinstance(char_name, str) or not char_name.strip():
        return []

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, char_name)
    if not legacy_char:
        return []

    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT custom_name
            FROM economy.character_assets
            WHERE guild_id=$1
              AND character_name=$2
              AND character_user_id=$3
              AND custom_name ILIKE $4
            ORDER BY custom_name ASC
            LIMIT 25
            """,
            interaction.guild.id,
            legacy_char.name,
            legacy_char.user_id,
            f"{current}%",
        )
    return [app_commands.Choice(name=r["custom_name"], value=r["custom_name"]) for r in rows if r["custom_name"]]

    staff = is_staff_member(interaction)
    gid = legacy_guild_id(interaction.guild.id)

    names = await autocomplete_character_names(
        gid,
        current,
        requester_user_id=interaction.user.id,
        is_staff=staff,
    )
    return [app_commands.Choice(name=n, value=n) for n in names[:25]]

# -------------------------
# /income
# -------------------------
@bot.tree.command(
    name="income",
    description="Claim daily income (+10 Val / 1 Arce).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(character="Pick a character")
@app_commands.autocomplete(character=character_autocomplete)
async def income(interaction: discord.Interaction, character: str):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    if legacy_char.user_id != interaction.user.id:
        return await interaction.response.send_message("You can only claim income for characters you own.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    today = chicago_today()
    pool = await get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock($1)", _advisory_key(interaction.guild.id, legacy_char.name))

            already = await con.fetchval(
                """
                SELECT 1
                FROM economy.daily_income_claims
                WHERE guild_id = $1 AND character_name = $2 AND claim_date = $3
                """,
                interaction.guild.id,
                legacy_char.name,
                today,
            )
            if already:
                return await interaction.followup.send("You already claimed income for this character today.", ephemeral=True)

            base_income_val = to_val(1, "ARCE")
            assets_income_val = await con.fetchval(
                """
                SELECT COALESCE(SUM(income_val), 0)
                FROM economy.character_assets
                WHERE guild_id=$1 AND character_name=$2
                """,
                interaction.guild.id,
                legacy_char.name,
            )
            assets_income_val = int(assets_income_val or 0)
            total_income_val = int(base_income_val + assets_income_val)

            await con.execute(
                """
                INSERT INTO economy.daily_income_claims (guild_id, character_name, claim_date)
                VALUES ($1, $2, $3)
                """,
                interaction.guild.id,
                legacy_char.name,
                today,
            )

            await con.execute(
                """
                INSERT INTO economy.transactions
                  (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
                VALUES
                  ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                """,
                interaction.guild.id,
                legacy_char.name,
                legacy_char.user_id,
                total_income_val,
                "Daily income",
                interaction.user.id,
                "income",
                json.dumps({"base_income_val": base_income_val, "assets_income_val": assets_income_val}),
            )

            bal = await con.fetchval(
                """
                SELECT COALESCE(SUM(amount_val), 0)
                FROM economy.transactions
                WHERE guild_id = $1 AND character_name = $2
                """,
                interaction.guild.id,
                legacy_char.name,
            )

    await interaction.followup.send(
        f"✅ Income claimed for **{legacy_char.name}**: **{format_currency(total_income_val)}**\nNew balance: **{format_currency(int(bal))}**",
        ephemeral=True,
    )
    await log_to_econ(bot, f"💰 /income by {actor_label(interaction)} → **{legacy_char.name}** (+{format_currency(total_income_val)})")
    await update_bank_dashboard(bot)



# -------------------------
# /econ_ledger (staff)
# -------------------------
@bot.tree.command(
    name="econ_ledger",
    description="Staff: show recent transactions for a character (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    limit="How many transactions to show (max 20)",
)
@app_commands.autocomplete(character=character_autocomplete)
async def econ_ledger(interaction: discord.Interaction, character: str, limit: int = 10):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    limit = max(1, min(int(limit), 20))
    await interaction.response.defer(ephemeral=True)

    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT amount_val, reason, kind, actor_user_id, created_at
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            ORDER BY created_at DESC, id DESC
            LIMIT $3
            """,
            interaction.guild.id,
            legacy_char.name,
            limit,
        )

        bal = await con.fetchval(
            """
            SELECT COALESCE(SUM(amount_val), 0)
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            """,
            interaction.guild.id,
            legacy_char.name,
        )

    e = discord.Embed(title="📜 Economy Ledger (Recent)")
    e.add_field(name="Character", value=f"**{legacy_char.name}**", inline=True)
    e.add_field(name="Owner", value=user_label(bot, legacy_char.user_id), inline=True)
    e.add_field(name="Current Balance", value=f"**{format_currency(int(bal))}**", inline=False)

    if not rows:
        e.add_field(name="Entries", value="_No transactions yet._", inline=False)
    else:
        lines = []
        for r in rows:
            delta = int(r["amount_val"])
            sign = "+" if delta >= 0 else ""
            # show local time for readability
            ts = r["created_at"].astimezone(TZ).strftime("%Y-%m-%d %H:%M")
            actor = user_label(bot, int(r["actor_user_id"]))
            kind = str(r["kind"])
            reason = str(r["reason"])
            lines.append(f"`{ts}` **{sign}{format_currency(delta)}** · {kind} · {actor}\n{reason}")

        text = "\n\n".join(lines)
        if len(text) > 3900:
            text = text[:3900] + "…"
        e.add_field(name=f"Last {len(rows)} entries", value=text, inline=False)

    await interaction.followup.send(embed=e, ephemeral=True)

    await log_to_econ(bot, f"📜 /econ_ledger by {actor_label(interaction)} → **{legacy_char.name}** (last {limit})")

# -------------------------
# /econ_set_balance (staff)
# -------------------------
@bot.tree.command(
    name="econ_set_balance",
    description="Staff: set a character balance exactly (writes an audited adjustment).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    amount="Target amount (whole number)",
    unit="Unit for the target amount",
    reason="Reason (required)",
)
@app_commands.autocomplete(character=character_autocomplete)
@app_commands.choices(
    unit=[
        app_commands.Choice(name="Cinth (1 Val)", value="CINTH"),
        app_commands.Choice(name="Arce (10 Val)", value="ARCE"),
        app_commands.Choice(name="Elsh (100 Val)", value="ELSH"),
        app_commands.Choice(name="Orin (1,000 Val)", value="ORIN"),
        app_commands.Choice(name="Novir (10,000 Val)", value="NOVIR"),
        app_commands.Choice(name="Val (base)", value="VAL"),
    ],
)
async def econ_set_balance(
    interaction: discord.Interaction,
    character: str,
    amount: int,
    unit: app_commands.Choice[str],
    reason: str,
):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    if amount < 0:
        return await interaction.response.send_message("Amount must be 0 or greater.", ephemeral=True)

    if not reason.strip():
        return await interaction.response.send_message("Reason is required.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    target_val = to_val(int(amount), unit.value)

    pool = await get_pool()
    async with pool.acquire() as con:
        current_val = await con.fetchval(
            """
            SELECT COALESCE(SUM(amount_val), 0)
            FROM economy.transactions
            WHERE guild_id=$1 AND character_name=$2
            """,
            interaction.guild.id,
            legacy_char.name,
        )
        current_val = int(current_val or 0)

        delta = target_val - current_val

        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
            VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            """,
            interaction.guild.id,
            legacy_char.name,
            legacy_char.user_id,
            delta,
            reason.strip(),
            interaction.user.id,
            "set_balance",
            json.dumps(
                {
                    "target_val": target_val,
                    "previous_val": current_val,
                    "delta": delta,
                    "unit": unit.value,
                    "amount": amount,
                }
            ),
        )

    sign = "+" if delta >= 0 else ""
    await interaction.followup.send(
        f"✅ Balance set for **{legacy_char.name}**\n"
        f"Previous: **{format_currency(current_val)}**\n"
        f"Target: **{format_currency(target_val)}**\n"
        f"Applied delta: **{sign}{format_currency(delta)}**\n"
        f"Reason: {reason}",
        ephemeral=True,
    )

    await log_to_econ(
        bot,
        f"🎯 /econ_set_balance by {actor_label(interaction)} → **{legacy_char.name}** "
        f"(prev {format_currency(current_val)} → target {format_currency(target_val)}; delta {sign}{format_currency(delta)}): {reason}",
    )
    await update_bank_dashboard(bot)

# -------------------------
# /econ_export (staff)
# -------------------------
@bot.tree.command(
    name="econ_export",
    description="Staff: export transactions as CSV to the Econ Log (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(scope='Export scope: "yesterday" or "all"')
@app_commands.choices(
    scope=[
        app_commands.Choice(name="Yesterday only (local)", value="yesterday"),
        app_commands.Choice(name="All transactions", value="all"),
    ]
)
async def econ_export(interaction: discord.Interaction, scope: app_commands.Choice[str]):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    note = f"📤 Manual economy export ({scope.value}) requested by {actor_label(interaction)}. No pings."
    await send_export_to_econ_log(bot, guild_id=GUILD_ID, scope=scope.value, note=note)

    await interaction.followup.send("✅ Export posted to the Economy Command Log channel.", ephemeral=True)



# -------------------------
# Asset autocomplete (catalog)
# -------------------------
async def asset_autocomplete(interaction: discord.Interaction, current: str):
    pool = await get_pool()
    like = f"%{current}%"
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT DISTINCT asset_type, secondary_type
            FROM economy.asset_catalog
            WHERE secondary_type ILIKE $1 OR asset_type ILIKE $1
            ORDER BY asset_type ASC, secondary_type ASC
            LIMIT 25
            """,
            like,
        )
    choices = []
    for r in rows:
        at = str(r["asset_type"])
        st = str(r["secondary_type"])
        value = f"{at}||{st}"
        name = f"{at} — {st}"
        choices.append(app_commands.Choice(name=name, value=value))
    return choices

# -------------------------
# /econ_purchase (staff)
# -------------------------
@bot.tree.command(
    name="econ_purchase",
    description="Staff: transact an asset purchase/upgrade (no overdrafts).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    asset="Asset type and sub-type",
    tier="Tier to purchase",
    quantity="Quantity (only applies if this asset is stackable)",
    asset_name="Custom name for the asset (required when creating a new asset)",
    upgrade_asset="Select an owned asset to upgrade (by name)",
    reason="Reason (required)",
)
@app_commands.autocomplete(character=character_autocomplete, upgrade_asset=owned_asset_autocomplete)
async def econ_purchase(
    interaction: discord.Interaction,
    character: str,
    asset: str,
    tier: app_commands.Choice[int],
    quantity: int = 1,
    asset_name: str = "",
    upgrade_asset: str = "",
    reason: str = "",
):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    if not reason.strip():
        return await interaction.response.send_message("Reason is required.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.followup.send("Character not found.", ephemeral=True)

    if "|" not in asset:
        return await interaction.followup.send("Invalid asset selection.", ephemeral=True)

    asset_type, secondary_type = [a.strip() for a in asset.split("|", 1)]
    desired_tier = int(tier.value)

    action = (action or "").strip().upper()
    if action not in {"NEW","UPGRADE"}:
        await interaction.followup.send("Action must be NEW or UPGRADE.", ephemeral=True)
        return

    pool = await get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock($1)", _advisory_key(interaction.guild.id, legacy_char.name))

            cat = await con.fetchrow(
                """
                SELECT tier, tier_name, cost_val, income_val
                FROM economy.asset_catalog
                WHERE secondary_type=$1 AND tier_name=$2
                """,
                asset_type,
                tier_name,
            )
            if not cat:
                return await interaction.followup.send("That tier does not exist for this asset in the catalog.", ephemeral=True)

            desired_tier = int(cat["tier"])
            tier_name = str(cat["tier_name"])
            cost_val = int(cat["cost_val"])
            income_val = int(cat["income_val"])

            upgrade_asset = (upgrade_asset or "").strip()
            if action == "UPGRADE":
                if not upgrade_asset:
                    await interaction.followup.send("Select an owned asset to upgrade.", ephemeral=True)
                    return
                
                row = await con.fetchrow(
                    """
                    SELECT tier
                    FROM economy.character_assets
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND custom_name=$4
                    """,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                    upgrade_asset,
                )
                if not row:
                    return await interaction.followup.send("That owned asset name was not found for this character.", ephemeral=True)

                ok, cur, shortfall = await ensure_can_debit(con, interaction.guild.id, legacy_char.name, cost_val)
                if not ok:
                    return await interaction.followup.send(
                        f"❌ Insufficient funds for **{legacy_char.name}**.\n"
                        f"Balance: **{format_currency(cur)}**\n"
                        f"Required: **{format_currency(cost_val)}**\n"
                        f"Shortfall: **{format_currency(shortfall)}**",
                        ephemeral=True,
                    )

                await con.execute(
                    """
                    INSERT INTO economy.transactions
                      (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
                    VALUES
                      ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    """,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                    -cost_val,
                    reason.strip(),
                    interaction.user.id,
                    "purchase_upgrade",
                    json.dumps({"asset_type": asset_type, "secondary_type": secondary_type, "custom_name": upgrade_asset, "tier": desired_tier}),
                )

                await con.execute(
                    """
                    UPDATE economy.character_assets
                    SET tier=$1, tier_name=$2, income_val=$3, updated_at=NOW()
                    WHERE guild_id=$4 AND character_name=$5 AND character_user_id=$6 AND custom_name=$7
                    """,
                    desired_tier,
                    tier_name,
                    income_val,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                    upgrade_asset,
                )

                        else:
                # NEW purchase
                asset_name = (asset_name or "").strip()
                if action != "NEW":
                    await interaction.followup.send("Choose NEW or UPGRADE.", ephemeral=True)
                    return
                if not asset_name:
                    return await interaction.followup.send("Asset name is required when creating a new asset.", ephemeral=True)

                rule = await con.fetchrow(
                    """
                    SELECT stackable, max_instances
                    FROM economy.asset_rules
                    WHERE asset_type=$1 AND secondary_type=$2
                    """,
                    asset_type,
                    secondary_type,
                )
                stackable = bool(rule["stackable"]) if rule else False
                max_instances = int(rule["max_instances"]) if rule and rule["max_instances"] is not None else 1

                                qty = 1

                exists = await con.fetchval(
                    """
                    SELECT 1 FROM economy.character_assets
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND custom_name=$4
                    """,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                    asset_name,
                )
                if exists:
                    return await interaction.followup.send("That asset name already exists for this character. Choose a different name.", ephemeral=True)

                if stackable:
                    have = await con.fetchval(
                        """
                        SELECT COUNT(*)
                        FROM economy.character_assets
                        WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3 AND asset_type=$4 AND secondary_type=$5
                        """,
                        interaction.guild.id,
                        legacy_char.name,
                        legacy_char.user_id,
                        asset_type,
                        secondary_type,
                    )
                    have = int(have or 0)
                    if have + qty > max_instances:
                        return await interaction.followup.send(
                            f"Cannot purchase {qty}. Max instances is {max_instances}. You currently have {have}.",
                            ephemeral=True,
                        )

                total_cost = cost_val * qty
                ok, cur, shortfall = await ensure_can_debit(con, interaction.guild.id, legacy_char.name, total_cost)
                if not ok:
                    return await interaction.followup.send(
                        f"❌ Insufficient funds for **{legacy_char.name}**.\n"
                        f"Balance: **{format_currency(cur)}**\n"
                        f"Required: **{format_currency(total_cost)}**\n"
                        f"Shortfall: **{format_currency(shortfall)}**",
                        ephemeral=True,
                    )

                await con.execute(
                    """
                    INSERT INTO economy.transactions
                      (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
                    VALUES
                      ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    """,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                    -total_cost,
                    reason.strip(),
                    interaction.user.id,
                    "purchase_new",
                    json.dumps({"asset_type": asset_type, "secondary_type": secondary_type, "custom_name": asset_name, "tier": desired_tier, "quantity": qty}),
                )

                next_inst = await con.fetchval(
                    """
                    SELECT COALESCE(MAX(instance_no),0)+1
                    FROM economy.character_assets
                    WHERE guild_id=$1 AND character_name=$2 AND character_user_id=$3
                    """,
                    interaction.guild.id,
                    legacy_char.name,
                    legacy_char.user_id,
                )
                next_inst = int(next_inst or 1)

                for i in range(qty):
                    nm = asset_name if qty == 1 else f"{asset_name} #{i+1}"
                    inst = next_inst + i
                    await con.execute(
                        """
                        INSERT INTO economy.character_assets
                          (guild_id, character_name, character_user_id, instance_no, asset_type, secondary_type, custom_name, tier, tier_name, income_val)
                        VALUES
                          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                        """,
                        interaction.guild.id,
                        legacy_char.name,
                        legacy_char.user_id,
                        inst,
                        asset_type,
                        secondary_type,
                        nm,
                        desired_tier,
                        tier_name,
                        income_val,
                    )

    await interaction.followup.send(
        f"✅ Purchase recorded for **{legacy_char.name}** — **{tier_name}**.",
        ephemeral=True,
    )

    await log_to_econ(
        bot,
        f"🛒 /econ_purchase by {actor_label(interaction)} → **{legacy_char.name}**: {secondary_type} / {tier_name} ({reason.strip()})",
    )
    await update_bank_dashboard(bot)



# -------------------------
# /econ_asset_rule (staff) — set stackable/max
# -------------------------
@bot.tree.command(
    name="econ_asset_rule",
    description="Staff: set whether an asset is stackable and its max instances (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    asset="Pick an asset chain",
    stackable="Allow multiple instances?",
    max_instances="Maximum number of instances allowed (>=1)",
)
@app_commands.autocomplete(asset=asset_autocomplete)
async def econ_asset_rule(
    interaction: discord.Interaction,
    asset: str,
    stackable: bool,
    max_instances: int = 1,
):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)

    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)

    if max_instances < 1:
        return await interaction.response.send_message("max_instances must be at least 1.", ephemeral=True)

    try:
        asset_type, secondary_type = asset.split("||", 1)
    except ValueError:
        return await interaction.response.send_message("Invalid asset selection.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO economy.asset_rules (asset_type, secondary_type, stackable, max_instances)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (asset_type, secondary_type)
            DO UPDATE SET stackable = EXCLUDED.stackable,
                          max_instances = EXCLUDED.max_instances
            """,
            asset_type,
            secondary_type,
            stackable,
            max_instances,
        )

    await interaction.followup.send(
        f"✅ Rule updated for **{asset_type} / {secondary_type}**: stackable={stackable}, max_instances={max_instances}",
        ephemeral=True,
    )

    await log_to_econ(
        bot,
        f"⚙️ /econ_asset_rule by {actor_label(interaction)} → {asset_type}/{secondary_type}: stackable={stackable}, max={max_instances}",
    )


# -------------------------
# Asset edit utilities (staff)
# -------------------------
async def fetch_asset_instance(con: asyncpg.Connection, guild_id: int, character_name: str, secondary_type: str, instance_no: int):
    return await con.fetchrow(
        """
        SELECT id, asset_type, secondary_type, custom_name, tier, tier_name, income_val, instance_no
        FROM economy.character_assets
        WHERE guild_id=$1 AND character_name=$2 AND secondary_type=$3 AND instance_no=$4
        """,
        guild_id,
        character_name,
        secondary_type,
        instance_no,
    )

@bot.tree.command(
    name="econ_asset_rename",
    description="Staff: rename a specific owned asset instance (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    secondary_type="Asset sub-type (e.g., Tavern/Inn)",
    instance_no="Instance number (1,2,3...)",
    new_name="New custom name",
    reason="Reason (required)",
)
@app_commands.autocomplete(character=character_autocomplete)
async def econ_asset_rename(interaction: discord.Interaction, character: str, secondary_type: str, instance_no: int = 1, new_name: str = "", reason: str = ""):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)
    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)
    if not new_name.strip():
        return await interaction.response.send_message("New name is required.", ephemeral=True)
    if not reason.strip():
        return await interaction.response.send_message("Reason is required.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    pool = await get_pool()
    async with pool.acquire() as con:
        row = await fetch_asset_instance(con, interaction.guild.id, legacy_char.name, secondary_type, int(instance_no))
        if not row:
            await interaction.followup.send("Asset instance not found.", ephemeral=True)
            return

        old = str(row["custom_name"] or "")
        await con.execute(
            """
            UPDATE economy.character_assets
            SET custom_name=$1, updated_at=NOW()
            WHERE guild_id=$2 AND character_name=$3 AND secondary_type=$4 AND instance_no=$5
            """,
            new_name.strip(),
            interaction.guild.id,
            legacy_char.name,
            secondary_type,
            int(instance_no),
        )

        # audit as a 0-val transaction
        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
            VALUES
              ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
            """,
            interaction.guild.id,
            legacy_char.name,
            legacy_char.user_id,
            0,
            reason.strip(),
            interaction.user.id,
            "asset_rename",
            json.dumps({"secondary_type": secondary_type, "instance_no": int(instance_no), "old_name": old, "new_name": new_name.strip()}),
        )

    await interaction.followup.send(
        f"✅ Renamed **{secondary_type}** [#{int(instance_no)}] for **{legacy_char.name}**\nOld: {old or '(unnamed)'}\nNew: {new_name.strip()}",
        ephemeral=True,
    )
    await log_to_econ(bot, f"✏️ /econ_asset_rename by {actor_label(interaction)} → **{legacy_char.name}**: {secondary_type} [#{int(instance_no)}] \"{old or '(unnamed)'}\" → \"{new_name.strip()}\" ({reason.strip()})")
    await update_bank_dashboard(bot)

@bot.tree.command(
    name="econ_asset_remove",
    description="Staff: remove a specific owned asset instance (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    secondary_type="Asset sub-type",
    instance_no="Instance number (1,2,3...)",
    reason="Reason (required)",
)
@app_commands.autocomplete(character=character_autocomplete)
async def econ_asset_remove(interaction: discord.Interaction, character: str, secondary_type: str, instance_no: int = 1, reason: str = ""):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)
    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)
    if not reason.strip():
        return await interaction.response.send_message("Reason is required.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    pool = await get_pool()
    async with pool.acquire() as con:
        row = await fetch_asset_instance(con, interaction.guild.id, legacy_char.name, secondary_type, int(instance_no))
        if not row:
            await interaction.followup.send("Asset instance not found.", ephemeral=True)
            return

        await con.execute(
            """
            DELETE FROM economy.character_assets
            WHERE guild_id=$1 AND character_name=$2 AND secondary_type=$3 AND instance_no=$4
            """,
            interaction.guild.id,
            legacy_char.name,
            secondary_type,
            int(instance_no),
        )

        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
            VALUES
              ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
            """,
            interaction.guild.id,
            legacy_char.name,
            legacy_char.user_id,
            0,
            reason.strip(),
            interaction.user.id,
            "asset_remove",
            json.dumps({"secondary_type": secondary_type, "instance_no": int(instance_no), "removed": True}),
        )

    nm = str(row["custom_name"] or "").strip() or "(unnamed)"
    await interaction.followup.send(
        f"✅ Removed **{secondary_type}** [#{int(instance_no)}] ({nm}) from **{legacy_char.name}**",
        ephemeral=True,
    )
    await log_to_econ(bot, f"🗑️ /econ_asset_remove by {actor_label(interaction)} → **{legacy_char.name}**: {secondary_type} [#{int(instance_no)}] ({nm}) ({reason.strip()})")
    await update_bank_dashboard(bot)

@bot.tree.command(
    name="econ_asset_set_tier",
    description="Staff: correct an asset tier (upgrade/downgrade) without renaming (no pings).",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    character="Pick a character",
    secondary_type="Asset sub-type",
    instance_no="Instance number (1,2,3...)",
    tier="Target tier (1-5)",
    reason="Reason (required)",
)
@app_commands.autocomplete(character=character_autocomplete)
@app_commands.choices(
    tier=[
        app_commands.Choice(name="Tier 1", value=1),
        app_commands.Choice(name="Tier 2", value=2),
        app_commands.Choice(name="Tier 3", value=3),
        app_commands.Choice(name="Tier 4", value=4),
        app_commands.Choice(name="Tier 5", value=5),
    ]
)
async def econ_asset_set_tier(interaction: discord.Interaction, character: str, secondary_type: str, instance_no: int = 1, tier: app_commands.Choice[int] = None, reason: str = ""):
    if interaction.guild is None:
        return await interaction.response.send_message("Use this in a server.", ephemeral=True)
    if not is_staff_member(interaction):
        return await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)
    if not reason.strip():
        return await interaction.response.send_message("Reason is required.", ephemeral=True)
    if tier is None:
        return await interaction.response.send_message("Tier is required.", ephemeral=True)

    legacy_gid = legacy_guild_id(interaction.guild.id)
    legacy_char = await fetch_character_by_name(legacy_gid, character)
    if not legacy_char:
        return await interaction.response.send_message("Character not found.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    desired_tier = int(tier.value)

    pool = await get_pool()
    async with pool.acquire() as con:
        row = await fetch_asset_instance(con, interaction.guild.id, legacy_char.name, secondary_type, int(instance_no))
        if not row:
            await interaction.followup.send("Asset instance not found.", ephemeral=True)
            return

        asset_type = str(row["asset_type"])
        # Look up catalog for income and tier name
        cat = await con.fetchrow(
            """
            SELECT tier_name, income_val
            FROM economy.asset_catalog
            WHERE asset_type=$1 AND secondary_type=$2 AND tier=$3
            """,
            asset_type,
            secondary_type,
            desired_tier,
        )
        if not cat:
            await interaction.followup.send("That tier does not exist in the asset catalog for this sub-type.", ephemeral=True)
            return

        old_tier = int(row["tier"])
        await con.execute(
            """
            UPDATE economy.character_assets
            SET tier=$1, tier_name=$2, income_val=$3, updated_at=NOW()
            WHERE guild_id=$4 AND character_name=$5 AND secondary_type=$6 AND instance_no=$7
            """,
            desired_tier,
            str(cat["tier_name"]),
            int(cat["income_val"]),
            interaction.guild.id,
            legacy_char.name,
            secondary_type,
            int(instance_no),
        )

        await con.execute(
            """
            INSERT INTO economy.transactions
              (guild_id, character_name, character_user_id, amount_val, reason, actor_user_id, kind, metadata)
            VALUES
              ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
            """,
            interaction.guild.id,
            legacy_char.name,
            legacy_char.user_id,
            0,
            reason.strip(),
            interaction.user.id,
            "asset_set_tier",
            json.dumps({"secondary_type": secondary_type, "instance_no": int(instance_no), "old_tier": old_tier, "new_tier": desired_tier}),
        )

    await interaction.followup.send(
        f"✅ Updated **{secondary_type}** [#{int(instance_no)}] for **{legacy_char.name}**: T{old_tier} → T{desired_tier}",
        ephemeral=True,
    )
    await log_to_econ(bot, f"🧰 /econ_asset_set_tier by {actor_label(interaction)} → **{legacy_char.name}**: {secondary_type} [#{int(instance_no)}] T{old_tier}→T{desired_tier} ({reason.strip()})")
    await update_bank_dashboard(bot)

def main():
    print(f"[{ENV}] Starting EconBot_v11…")
    bot.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()


async def seed_asset_catalog(con: asyncpg.Connection):
    """Insert/update the full asset catalog. Safe to run on every boot."""
    # Ensure uniqueness at DB level
    await con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_catalog ON economy.asset_catalog (asset_type, secondary_type, tier)")
    for row in ASSET_CATALOG_SEED:
        await con.execute(
            """
            INSERT INTO economy.asset_catalog (asset_type, secondary_type, tier, tier_name, cost_val, income_val)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (asset_type, secondary_type, tier)
            DO UPDATE SET tier_name=EXCLUDED.tier_name, cost_val=EXCLUDED.cost_val, income_val=EXCLUDED.income_val
            """,
            row["asset_type"],
            row["secondary_type"],
            int(row["tier"]),
            row["tier_name"],
            int(row["cost_val"]),
            int(row["income_val"]),
        )
