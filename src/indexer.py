#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import csv
import time
from decimal import Decimal, getcontext
from collections import defaultdict
from typing import Dict, Any, Tuple

from web3 import Web3
from eth_abi import decode as abi_decode

getcontext().prec = 80

# =========================================================
# CONFIG
# =========================================================
RPC_URL = os.environ["BASE_RPC_URL"]

CHAIN_ID = 8453
POOL = Web3.to_checksum_address("0xbe4C36B9542610dF83Ca690C8b5BC53BbbC5d542")
FROM_BLOCK = 43139450

OUR_WALLETS = {
    "0x5f0aea872b7d6dbcc181338f80048b130e443e3b": "our_pool_wallet",
    "0x44a3f0354f4c10eb9cd93e522b5e3210d126f054": "team_mm2",
}

STATE_FILE = "state/state.json"
OUT_DIR = "out"

BACKFILL_CHUNK = 800
SAFETY_WINDOW_BLOCKS = 50
CONFIRMATIONS_BUFFER = 2
TIMEOUT = 60
RETRIES = 6
RETRY_SLEEP = 1.0
SLEEP_BETWEEN_CHUNKS = 0.10
BUCKET_SIZE = 50

EXPECTED_TOKEN0 = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".lower()  # USDC
EXPECTED_TOKEN1 = "0x9EadbE35F3Ee3bF3e28180070C429298a1b02F93".lower()  # LMTS

# =========================================================
# WEB3
# =========================================================
w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": TIMEOUT}))
if not w3.is_connected():
    raise RuntimeError("RPC is not connected")

# =========================================================
# ABIs
# =========================================================
POOL_ABI = [
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "tickSpacing", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "int24"}]},
    {"name": "fee", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint24"}]},
    {"name": "slot0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [
        {"type": "uint160"},
        {"type": "int24"},
        {"type": "uint16"},
        {"type": "uint16"},
        {"type": "uint16"},
        {"type": "bool"},
    ]},
    {"name": "liquidity", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint128"}]},
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

pool_c = w3.eth.contract(address=POOL, abi=POOL_ABI)

# =========================================================
# TOPICS
# =========================================================
MINT_TOPIC0 = w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
BURN_TOPIC0 = w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()
INIT_TOPIC0 = w3.keccak(text="Initialize(uint160,int24)").hex()

# =========================================================
# MATH
# =========================================================
Q96 = Decimal(2) ** 96
D_1_0001 = Decimal("1.0001")


def sqrt_price_x96_to_price_token1_per_token0(sqrt_price_x96: int, dec0: int, dec1: int) -> Decimal:
    ratio = (Decimal(sqrt_price_x96) / Q96) ** 2
    return ratio * (Decimal(10) ** Decimal(dec0 - dec1))


def tick_to_sqrt_price(tick: int) -> Decimal:
    return D_1_0001 ** (Decimal(tick) / Decimal(2))


def current_amounts_from_liquidity(liq: int, tick_lower: int, tick_upper: int, sqrt_price_x96: int):
    """
    Возвращает raw amount0 и raw amount1 как Decimal.
    """
    L = Decimal(liq)
    sqrtP = Decimal(sqrt_price_x96) / Q96
    sqrtL = tick_to_sqrt_price(tick_lower)
    sqrtU = tick_to_sqrt_price(tick_upper)

    if sqrtP <= sqrtL:
        amount0 = L * (sqrtU - sqrtL) / (sqrtL * sqrtU)
        amount1 = Decimal(0)
    elif sqrtP < sqrtU:
        amount0 = L * (sqrtU - sqrtP) / (sqrtP * sqrtU)
        amount1 = L * (sqrtP - sqrtL)
    else:
        amount0 = Decimal(0)
        amount1 = L * (sqrtU - sqrtL)

    return amount0, amount1


def human_amount(raw: int | Decimal, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** Decimal(decimals))


def format_dec(x: Decimal, places: int = 8) -> str:
    q = Decimal(10) ** -places
    return str(x.quantize(q))


def tick_to_bucket_floor(tick: int, bucket_size: int) -> int:
    if tick >= 0:
        return (tick // bucket_size) * bucket_size
    return -(((-tick + bucket_size - 1) // bucket_size) * bucket_size)


# =========================================================
# HELPERS
# =========================================================
def ensure_dirs():
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)


def safe_call(fn):
    last = None
    for _ in range(RETRIES):
        try:
            return fn.call()
        except Exception as e:
            last = e
            time.sleep(RETRY_SLEEP)
    raise last


def rpc_get_logs(params: dict):
    last = None
    for _ in range(RETRIES):
        try:
            return w3.eth.get_logs(params)
        except Exception as e:
            last = e
            time.sleep(RETRY_SLEEP)
    raise last


def get_logs_adaptive(from_block: int, to_block: int, topics: list, address: str):
    """
    Читает логи по диапазону. Если RPC ругается, делит диапазон пополам.
    """
    if from_block > to_block:
        return []

    params = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": Web3.to_checksum_address(address),
        "topics": [topics],
    }

    try:
        return rpc_get_logs(params)
    except Exception as e:
        if from_block == to_block:
            raise e

        mid = (from_block + to_block) // 2
        left_logs = get_logs_adaptive(from_block, mid, topics, address)
        right_logs = get_logs_adaptive(mid + 1, to_block, topics, address)
        return left_logs + right_logs


def chunked_range(start_block: int, end_block: int, step: int):
    cur = start_block
    while cur <= end_block:
        to_block = min(cur + step - 1, end_block)
        yield cur, to_block
        cur = to_block + 1


def topic_to_address(topic) -> str:
    raw = topic.hex() if hasattr(topic, "hex") else str(topic)
    raw = raw[2:] if raw.startswith("0x") else raw
    return Web3.to_checksum_address("0x" + raw[-40:])


def decode_int24_topic(topic) -> int:
    raw = topic.hex() if hasattr(topic, "hex") else str(topic)
    raw = raw[2:] if raw.startswith("0x") else raw
    raw_24 = raw[-6:]
    val = int(raw_24, 16)
    if val >= 2**23:
        val -= 2**24
    return val


def decode_mint_or_burn_data(data_hex: str):
    b = bytes.fromhex(data_hex[2:] if data_hex.startswith("0x") else data_hex)
    liquidity, amount0, amount1 = abi_decode(["uint128", "uint256", "uint256"], b)
    return int(liquidity), int(amount0), int(amount1)


def position_key(owner: str, tick_lower: int, tick_upper: int) -> str:
    return f"{owner.lower()}|{tick_lower}|{tick_upper}"


def parse_position_key(key: str) -> Tuple[str, int, int]:
    owner, tl, tu = key.split("|")
    return owner, int(tl), int(tu)


# =========================================================
# STATE
# =========================================================
def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {
            "meta": {
                "chain_id": CHAIN_ID,
                "pool": str(POOL),
            },
            "sync": {
                "last_scanned_block": None,
            },
            "positions": {},
        }

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: Dict[str, Any]):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


# =========================================================
# CORE
# =========================================================
def load_pool_info():
    token0 = Web3.to_checksum_address(safe_call(pool_c.functions.token0()))
    token1 = Web3.to_checksum_address(safe_call(pool_c.functions.token1()))
    tick_spacing = int(safe_call(pool_c.functions.tickSpacing()))
    fee = int(safe_call(pool_c.functions.fee()))
    slot0 = safe_call(pool_c.functions.slot0())
    pool_liquidity = int(safe_call(pool_c.functions.liquidity()))

    sqrt_price_x96 = int(slot0[0])
    current_tick = int(slot0[1])

    t0 = w3.eth.contract(address=token0, abi=ERC20_ABI)
    t1 = w3.eth.contract(address=token1, abi=ERC20_ABI)

    dec0 = int(safe_call(t0.functions.decimals()))
    dec1 = int(safe_call(t1.functions.decimals()))
    sym0 = safe_call(t0.functions.symbol())
    sym1 = safe_call(t1.functions.symbol())

    return {
        "token0": token0,
        "token1": token1,
        "dec0": dec0,
        "dec1": dec1,
        "sym0": sym0,
        "sym1": sym1,
        "tick_spacing": tick_spacing,
        "fee": fee,
        "sqrt_price_x96": sqrt_price_x96,
        "current_tick": current_tick,
        "current_price_token1_per_token0": sqrt_price_x96_to_price_token1_per_token0(
            sqrt_price_x96, dec0, dec1
        ),
        "pool_liquidity_raw": pool_liquidity,
    }


def get_scan_range(state: Dict[str, Any], latest_block_safe: int) -> Tuple[int, int]:
    last_scanned_block = state["sync"]["last_scanned_block"]

    if last_scanned_block is None:
        return FROM_BLOCK, latest_block_safe

    start_block = max(FROM_BLOCK, int(last_scanned_block) - SAFETY_WINDOW_BLOCKS)
    return start_block, latest_block_safe


def process_logs_into_state(state: Dict[str, Any], start_block: int, end_block: int):
    positions = state["positions"]

    total_logs = 0
    total_new_mint = 0
    total_new_burn = 0
    total_init = 0

    # локальный дедуп этого запуска, потому что из-за safety window мы перечитываем overlap
    seen_event_ids_this_run = set()

    for chunk_from, chunk_to in chunked_range(start_block, end_block, BACKFILL_CHUNK):
        logs = get_logs_adaptive(
            from_block=chunk_from,
            to_block=chunk_to,
            topics=[MINT_TOPIC0, BURN_TOPIC0, INIT_TOPIC0],
            address=str(POOL),
        )

        mint_c = 0
        burn_c = 0
        init_c = 0

        for lg in logs:
            tx_hash = lg["transactionHash"].hex().lower()
            log_index = int(lg["logIndex"])
            block_number = int(lg["blockNumber"])
            event_id = f"{tx_hash}:{log_index}"

            if event_id in seen_event_ids_this_run:
                continue
            seen_event_ids_this_run.add(event_id)

            topic0 = lg["topics"][0].hex()

            if topic0.lower() == INIT_TOPIC0.lower():
                init_c += 1
                continue

            if len(lg["topics"]) < 4:
                continue

            owner = topic_to_address(lg["topics"][1]).lower()
            tick_lower = decode_int24_topic(lg["topics"][2])
            tick_upper = decode_int24_topic(lg["topics"][3])

            key = position_key(owner, tick_lower, tick_upper)
            if key not in positions:
                positions[key] = {
                    "owner": owner,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "liquidity_raw": 0,
                    "minted_amount0_raw": 0,
                    "minted_amount1_raw": 0,
                    "burned_amount0_raw": 0,
                    "burned_amount1_raw": 0,
                    "first_seen_block": block_number,
                    "last_seen_block": block_number,
                    "mint_count": 0,
                    "burn_count": 0,
                }

            row = positions[key]

            # Защита от overlap: если мы перечитали старый хвост, не хотим еще раз суммировать те же самые события.
            # Поэтому при overlap мы сначала "обнуляем" состояние только если стартуем не с первого полного запуска.
            # Но это сложно и ненадежно на row-level, поэтому делаем проще:
            # если chunk начинается раньше или равен last_scanned_block, то rebuild корректнее делать с нуля.
            # Однако чтобы не усложнять, используем state только для forward-режима:
            # overlap допустим, потому что дедуп идет в рамках запуска, а chunk внутри одного запуска не дублируется.

            row["last_seen_block"] = block_number
            if row["first_seen_block"] is None:
                row["first_seen_block"] = block_number

            liq_delta, amount0, amount1 = decode_mint_or_burn_data(lg["data"])

            if topic0.lower() == MINT_TOPIC0.lower():
                row["liquidity_raw"] += liq_delta
                row["minted_amount0_raw"] += amount0
                row["minted_amount1_raw"] += amount1
                row["mint_count"] += 1
                mint_c += 1

            elif topic0.lower() == BURN_TOPIC0.lower():
                row["liquidity_raw"] -= liq_delta
                row["burned_amount0_raw"] += amount0
                row["burned_amount1_raw"] += amount1
                row["burn_count"] += 1
                burn_c += 1

            positions[key] = row

        state["sync"]["last_scanned_block"] = chunk_to

        total_logs += len(logs)
        total_new_mint += mint_c
        total_new_burn += burn_c
        total_init += init_c

        print(
            f"scanned {chunk_from}-{chunk_to} | "
            f"logs={len(logs)} mint={mint_c} burn={burn_c} init={init_c}"
        )

        time.sleep(SLEEP_BETWEEN_CHUNKS)

    return {
        "logs_scanned_this_run": total_logs,
        "mint_events_this_run": total_new_mint,
        "burn_events_this_run": total_new_burn,
        "initialize_events_this_run": total_init,
    }


def build_exports(state: Dict[str, Any], pool_info: Dict[str, Any]):
    positions_rows = []
    owner_aggr = defaultdict(lambda: {
        "owner_label": "external",
        "owner_type": "external",
        "total_liquidity_raw": 0,
        "positions_count": 0,
        "active_positions_count": 0,
        "current_amount0": Decimal(0),
        "current_amount1": Decimal(0),
    })

    bucket_map = defaultdict(lambda: {
        "total_liquidity_raw": 0,
        "our_liquidity_raw": 0,
        "external_liquidity_raw": 0,
        "positions_count": 0,
        "our_positions_count": 0,
        "external_positions_count": 0,
        "owners": set(),
    })

    our_total_liquidity_raw = 0
    external_total_liquidity_raw = 0
    our_current_amount0 = Decimal(0)
    our_current_amount1 = Decimal(0)
    external_current_amount0 = Decimal(0)
    external_current_amount1 = Decimal(0)

    current_tick = pool_info["current_tick"]
    sqrt_price_x96 = pool_info["sqrt_price_x96"]
    dec0 = pool_info["dec0"]
    dec1 = pool_info["dec1"]
    sym0 = pool_info["sym0"]
    sym1 = pool_info["sym1"]

    for _, row in state["positions"].items():
        liquidity_raw = int(row["liquidity_raw"])
        if liquidity_raw <= 0:
            continue

        owner = row["owner"].lower()
        owner_label = OUR_WALLETS.get(owner, "external")
        owner_type = "ours" if owner in OUR_WALLETS else "external"
        tick_lower = int(row["tick_lower"])
        tick_upper = int(row["tick_upper"])
        in_range = tick_lower <= current_tick < tick_upper

        amt0_raw_dec, amt1_raw_dec = current_amounts_from_liquidity(
            liquidity_raw, tick_lower, tick_upper, sqrt_price_x96
        )
        current_amount0 = human_amount(amt0_raw_dec, dec0)
        current_amount1 = human_amount(amt1_raw_dec, dec1)

        if owner_type == "ours":
            our_total_liquidity_raw += liquidity_raw
            our_current_amount0 += current_amount0
            our_current_amount1 += current_amount1
        else:
            external_total_liquidity_raw += liquidity_raw
            external_current_amount0 += current_amount0
            external_current_amount1 += current_amount1

        positions_rows.append({
            "owner": owner,
            "owner_label": owner_label,
            "owner_type": owner_type,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "width_ticks": tick_upper - tick_lower,
            "liquidity_raw": liquidity_raw,
            "in_range": in_range,
            f"current_{sym0.lower()}": format_dec(current_amount0, 8),
            f"current_{sym1.lower()}": format_dec(current_amount1, 8),
            f"minted_{sym0.lower()}": format_dec(human_amount(int(row["minted_amount0_raw"]), dec0), 8),
            f"minted_{sym1.lower()}": format_dec(human_amount(int(row["minted_amount1_raw"]), dec1), 8),
            f"burned_{sym0.lower()}": format_dec(human_amount(int(row["burned_amount0_raw"]), dec0), 8),
            f"burned_{sym1.lower()}": format_dec(human_amount(int(row["burned_amount1_raw"]), dec1), 8),
            "first_seen_block": row["first_seen_block"],
            "last_seen_block": row["last_seen_block"],
            "mint_count": row["mint_count"],
            "burn_count": row["burn_count"],
        })

        ag = owner_aggr[owner]
        ag["owner_label"] = owner_label
        ag["owner_type"] = owner_type
        ag["total_liquidity_raw"] += liquidity_raw
        ag["positions_count"] += 1
        ag["active_positions_count"] += 1 if in_range else 0
        ag["current_amount0"] += current_amount0
        ag["current_amount1"] += current_amount1

        start_bucket = tick_to_bucket_floor(tick_lower, BUCKET_SIZE)
        end_bucket = tick_to_bucket_floor(tick_upper - 1, BUCKET_SIZE)

        cur = start_bucket
        while cur <= end_bucket:
            bucket_map[cur]["total_liquidity_raw"] += liquidity_raw
            bucket_map[cur]["positions_count"] += 1
            bucket_map[cur]["owners"].add(owner)

            if owner_type == "ours":
                bucket_map[cur]["our_liquidity_raw"] += liquidity_raw
                bucket_map[cur]["our_positions_count"] += 1
            else:
                bucket_map[cur]["external_liquidity_raw"] += liquidity_raw
                bucket_map[cur]["external_positions_count"] += 1

            cur += BUCKET_SIZE

    positions_rows.sort(
        key=lambda r: (
            r["owner_type"] != "ours",
            -int(r["liquidity_raw"]),
            r["owner"],
            r["tick_lower"],
            r["tick_upper"],
        )
    )

    top_lp_rows = []
    for owner, ag in owner_aggr.items():
        top_lp_rows.append({
            "owner": owner,
            "owner_label": ag["owner_label"],
            "owner_type": ag["owner_type"],
            "total_liquidity_raw": ag["total_liquidity_raw"],
            "positions_count": ag["positions_count"],
            "active_positions_count": ag["active_positions_count"],
            f"current_{sym0.lower()}": format_dec(ag["current_amount0"], 8),
            f"current_{sym1.lower()}": format_dec(ag["current_amount1"], 8),
        })

    top_lp_rows.sort(
        key=lambda r: (
            r["owner_type"] != "ours",
            -int(r["total_liquidity_raw"]),
            r["owner"],
        )
    )

    bucket_rows = []
    for bucket_low in sorted(bucket_map.keys()):
        bucket_rows.append({
            "bucket_tick_lower": bucket_low,
            "bucket_tick_upper": bucket_low + BUCKET_SIZE,
            "total_liquidity_raw": bucket_map[bucket_low]["total_liquidity_raw"],
            "our_liquidity_raw": bucket_map[bucket_low]["our_liquidity_raw"],
            "external_liquidity_raw": bucket_map[bucket_low]["external_liquidity_raw"],
            "positions_count": bucket_map[bucket_low]["positions_count"],
            "our_positions_count": bucket_map[bucket_low]["our_positions_count"],
            "external_positions_count": bucket_map[bucket_low]["external_positions_count"],
            "owners_count": len(bucket_map[bucket_low]["owners"]),
            "contains_current_tick": bucket_low <= current_tick < bucket_low + BUCKET_SIZE,
        })

    unique_owners = len(owner_aggr)
    open_positions_count = len(positions_rows)
    our_open_positions_count = sum(1 for r in positions_rows if r["owner_type"] == "ours")
    external_open_positions_count = sum(1 for r in positions_rows if r["owner_type"] == "external")
    positions_in_range_count = sum(1 for r in positions_rows if r["in_range"])

    summary_rows = [
        {"metric": "chain_id", "value": CHAIN_ID},
        {"metric": "pool", "value": str(POOL)},
        {"metric": "from_block", "value": FROM_BLOCK},
        {"metric": "last_scanned_block", "value": state["sync"]["last_scanned_block"]},
        {"metric": "token0_symbol", "value": sym0},
        {"metric": "token1_symbol", "value": sym1},
        {"metric": "token0_address", "value": pool_info["token0"]},
        {"metric": "token1_address", "value": pool_info["token1"]},
        {"metric": "token0_decimals", "value": dec0},
        {"metric": "token1_decimals", "value": dec1},
        {"metric": "tick_spacing", "value": pool_info["tick_spacing"]},
        {"metric": "fee", "value": pool_info["fee"]},
        {"metric": "current_tick", "value": current_tick},
        {"metric": f"price_{sym1}_per_1_{sym0}", "value": str(pool_info["current_price_token1_per_token0"])},
        {"metric": "pool_liquidity_raw", "value": pool_info["pool_liquidity_raw"]},
        {"metric": "open_positions_count", "value": open_positions_count},
        {"metric": "unique_owners_count", "value": unique_owners},
        {"metric": "positions_in_range_count", "value": positions_in_range_count},
        {"metric": "our_open_positions_count", "value": our_open_positions_count},
        {"metric": "external_open_positions_count", "value": external_open_positions_count},
        {"metric": "our_total_liquidity_raw", "value": our_total_liquidity_raw},
        {"metric": "external_total_liquidity_raw", "value": external_total_liquidity_raw},
        {"metric": f"our_current_{sym0.lower()}", "value": format_dec(our_current_amount0, 8)},
        {"metric": f"our_current_{sym1.lower()}", "value": format_dec(our_current_amount1, 8)},
        {"metric": f"external_current_{sym0.lower()}", "value": format_dec(external_current_amount0, 8)},
        {"metric": f"external_current_{sym1.lower()}", "value": format_dec(external_current_amount1, 8)},
    ]

    return positions_rows, top_lp_rows, bucket_rows, summary_rows


def write_csv(path: str, rows: list, fieldnames: list | None = None):
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []

    with open(path, "w", newline="", encoding="utf-8") as f:
        if fieldnames:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            if rows:
                w.writerows(rows)


def print_summary(summary_rows):
    print("\n=== SUMMARY ===")
    for row in summary_rows:
        print(f"{row['metric']}: {row['value']}")


def main():
    ensure_dirs()

    latest_block = int(w3.eth.block_number)
    latest_block_safe = latest_block - CONFIRMATIONS_BUFFER
    if latest_block_safe < FROM_BLOCK:
        raise RuntimeError("latest_block_safe < FROM_BLOCK")

    state = load_state()
    pool_info = load_pool_info()

    print("=== POOL INFO ===")
    print(f"Pool:               {POOL}")
    print(f"Latest block:       {latest_block}")
    print(f"Latest safe block:  {latest_block_safe}")
    print(f"From block:         {FROM_BLOCK}")
    print(f"Token0:             {pool_info['sym0']} ({pool_info['token0']}) decimals={pool_info['dec0']}")
    print(f"Token1:             {pool_info['sym1']} ({pool_info['token1']}) decimals={pool_info['dec1']}")
    print(f"Tick spacing:       {pool_info['tick_spacing']}")
    print(f"Fee:                {pool_info['fee']}")
    print(f"Current tick:       {pool_info['current_tick']}")
    print(f"Pool liquidity raw: {pool_info['pool_liquidity_raw']}")
    print(f"Price {pool_info['sym1']} per 1 {pool_info['sym0']}: {pool_info['current_price_token1_per_token0']}")

    if pool_info["token0"].lower() != EXPECTED_TOKEN0 or pool_info["token1"].lower() != EXPECTED_TOKEN1:
        print("\nWARNING: token0/token1 отличаются от ожидаемых USDC/LMTS.")
        print("Скрипт все равно отработает, но проверь пул.\n")

    # ВАЖНО:
    # если уже есть state и ты хочешь корректный incremental через overlap,
    # текущая простая логика безопасна только если старый overlap не был уже посчитан повторно.
    # Поэтому для первой корректной версии делаем так:
    # - первый запуск с нуля
    # - следующие запуски либо без overlap, либо потом допилим rebuild-from-events.
    #
    # Чтобы сейчас не портить state повторным overlap, временно отключаем overlap для persisted-state режима.
    if state["sync"]["last_scanned_block"] is None:
        start_block, end_block = FROM_BLOCK, latest_block_safe
    else:
        start_block = int(state["sync"]["last_scanned_block"]) + 1
        end_block = latest_block_safe

    if start_block > end_block:
        print("\nNothing new to scan. Rebuilding exports only...")
    else:
        print("\n=== INDEXING ===")
        print(f"Scan range: {start_block} -> {end_block}")
        run_stats = process_logs_into_state(state, start_block, end_block)

        print("\n=== RUN STATS ===")
        for k, v in run_stats.items():
            print(f"{k}: {v}")

        save_state(state)

    state["meta"]["token0"] = pool_info["token0"]
    state["meta"]["token1"] = pool_info["token1"]
    state["meta"]["token0_symbol"] = pool_info["sym0"]
    state["meta"]["token1_symbol"] = pool_info["sym1"]
    state["meta"]["token0_decimals"] = pool_info["dec0"]
    state["meta"]["token1_decimals"] = pool_info["dec1"]
    state["meta"]["tick_spacing"] = pool_info["tick_spacing"]
    state["meta"]["fee"] = pool_info["fee"]
    state["meta"]["pool_liquidity_raw"] = pool_info["pool_liquidity_raw"]
    save_state(state)

    positions_rows, top_lp_rows, bucket_rows, summary_rows = build_exports(state, pool_info)

    write_csv(os.path.join(OUT_DIR, "open_positions.csv"), positions_rows)
    write_csv(os.path.join(OUT_DIR, "top_lp.csv"), top_lp_rows)
    write_csv(os.path.join(OUT_DIR, "buckets.csv"), bucket_rows)
    write_csv(os.path.join(OUT_DIR, "summary.csv"), summary_rows, fieldnames=["metric", "value"])

    print_summary(summary_rows)

    print("\nSaved:")
    print(" - state/state.json")
    print(" - out/open_positions.csv")
    print(" - out/top_lp.csv")
    print(" - out/buckets.csv")
    print(" - out/summary.csv")


if __name__ == "__main__":
    main()
