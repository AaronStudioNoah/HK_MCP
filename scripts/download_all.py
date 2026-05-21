"""Full dump of Noah HK CRM MCP data to JSON files.

Stages:
  1. customer-list   : paginated dump of hk_search_customers (region=HK)
  2. details         : per-customer 6 endpoints (KYC, CN/HK/SG holdings, service, live)
  3. meetings        : my meetings + per-meeting attendees
  4. stats           : RM stats aggregations by various dimensions

Resume-safe: each output file is written atomically; existing non-empty files are skipped.
Run:
  python3 scripts/download_all.py [--stage list|details|meetings|stats|all] [--concurrency N]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mcp_client import call_tool, initialize_session, load_token  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
LIST_DIR = DATA / "customers" / "list"
DETAILS_DIR = DATA / "customers" / "details"
MEETINGS_DIR = DATA / "meetings"
MEETING_ATTENDEES_DIR = MEETINGS_DIR / "attendees"
STATS_DIR = DATA / "stats"
LOG_FILE = DATA / "download.log"

DETAIL_ENDPOINTS = [
    ("kyc", "hk_get_customer_kyc", "group_no"),
    ("domestic_holdings", "hk_get_domestic_holdings", "group_no"),
    ("hk_holdings", "hk_get_hk_holdings", "group_no"),
    ("sg_holdings", "hk_get_sg_holdings", "group_no"),
    ("service_records", "hk_get_service_records", "group_no"),
    ("live_information", "hk_get_live_information", "group_no"),
]

PAGE_SIZE = 500  # records per page (~1MB each at this size)


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a") as f:
        f.write(line + "\n")


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def is_complete(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 2  # bigger than "{}"


async def fetch_customer_list_page(
    client: httpx.AsyncClient, token: str, page: int
) -> dict[str, Any]:
    return await call_tool(
        client,
        token,
        "hk_search_customers",
        {"pageNum": page, "pageSize": PAGE_SIZE, "orderBy": "latest_aum_all desc"},
        request_id=page,
    )


async def stage_customer_list(client: httpx.AsyncClient, token: str, concurrency: int) -> list[str]:
    LIST_DIR.mkdir(parents=True, exist_ok=True)
    # First page to get total
    first = await fetch_customer_list_page(client, token, 1)
    total = first["total"]
    page_count = (total + PAGE_SIZE - 1) // PAGE_SIZE
    log(f"customer list: total={total} pages={page_count} pageSize={PAGE_SIZE}")
    write_json_atomic(LIST_DIR / f"page_{1:04d}.json", first)

    sem = asyncio.Semaphore(concurrency)

    async def one(page: int) -> None:
        out = LIST_DIR / f"page_{page:04d}.json"
        if is_complete(out):
            return
        async with sem:
            data = await fetch_customer_list_page(client, token, page)
            write_json_atomic(out, data)
            if page % 10 == 0:
                log(f"  list page {page}/{page_count} done")

    await asyncio.gather(*(one(p) for p in range(2, page_count + 1)))

    # Build consolidated index of group_no -> record
    all_records: dict[str, dict[str, Any]] = {}
    for p in range(1, page_count + 1):
        page_data = json.loads((LIST_DIR / f"page_{p:04d}.json").read_text())
        for rec in page_data.get("records", []):
            gno = rec.get("groupNo")
            if gno:
                all_records[gno] = rec
    write_json_atomic(LIST_DIR / "_index.json", all_records)
    log(f"customer list done: {len(all_records)} unique group_no")
    return sorted(all_records.keys())


async def fetch_detail(
    client: httpx.AsyncClient,
    token: str,
    sem: asyncio.Semaphore,
    group_no: str,
    key: str,
    tool_name: str,
    arg_name: str,
    counters: dict[str, int],
) -> None:
    out = DETAILS_DIR / group_no / f"{key}.json"
    if is_complete(out):
        counters["skipped"] += 1
        return
    async with sem:
        try:
            data = await call_tool(client, token, tool_name, {arg_name: group_no})
            write_json_atomic(out, data)
            counters["ok"] += 1
        except Exception as e:
            err_out = DETAILS_DIR / group_no / f"{key}.error.json"
            write_json_atomic(err_out, {"error": str(e)})
            counters["err"] += 1


async def stage_details(
    client: httpx.AsyncClient, token: str, group_nos: list[str], concurrency: int
) -> None:
    DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)
    counters = {"ok": 0, "err": 0, "skipped": 0}
    total_tasks = len(group_nos) * len(DETAIL_ENDPOINTS)
    log(f"details: {len(group_nos)} customers x {len(DETAIL_ENDPOINTS)} endpoints = {total_tasks} calls")

    tasks: list[asyncio.Task] = []
    start = time.time()
    last_report = start

    async def reporter() -> None:
        nonlocal last_report
        while not stop_event.is_set():
            await asyncio.sleep(20)
            done = counters["ok"] + counters["err"] + counters["skipped"]
            elapsed = time.time() - start
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total_tasks - done) / rate if rate > 0 else 0
            log(
                f"  progress: {done}/{total_tasks} "
                f"(ok={counters['ok']} err={counters['err']} skip={counters['skipped']}) "
                f"{rate:.1f} req/s eta={eta/60:.1f}min"
            )

    stop_event = asyncio.Event()
    rep_task = asyncio.create_task(reporter())

    for gno in group_nos:
        for key, tool_name, arg_name in DETAIL_ENDPOINTS:
            tasks.append(
                asyncio.create_task(
                    fetch_detail(client, token, sem, gno, key, tool_name, arg_name, counters)
                )
            )

    await asyncio.gather(*tasks)
    stop_event.set()
    rep_task.cancel()
    try:
        await rep_task
    except asyncio.CancelledError:
        pass

    log(
        f"details done: ok={counters['ok']} err={counters['err']} "
        f"skipped={counters['skipped']} in {(time.time()-start)/60:.1f}min"
    )


async def stage_meetings(client: httpx.AsyncClient, token: str, concurrency: int) -> None:
    MEETINGS_DIR.mkdir(parents=True, exist_ok=True)
    MEETING_ATTENDEES_DIR.mkdir(parents=True, exist_ok=True)
    meetings = await call_tool(client, token, "hk_get_my_meetings", {"includeAll": True})
    write_json_atomic(MEETINGS_DIR / "meetings_all.json", meetings)
    data = meetings.get("data", []) if isinstance(meetings, dict) else []
    log(f"meetings: total={len(data)}")
    sem = asyncio.Semaphore(concurrency)

    async def one(m: dict[str, Any]) -> None:
        mid = m.get("meetingId")
        if mid is None:
            return
        out = MEETING_ATTENDEES_DIR / f"{mid}.json"
        if is_complete(out):
            return
        async with sem:
            try:
                attendees = await call_tool(client, token, "hk_get_meeting_customers", {"meetingId": int(mid)})
                write_json_atomic(out, attendees)
            except Exception as e:
                write_json_atomic(MEETING_ATTENDEES_DIR / f"{mid}.error.json", {"error": str(e)})

    await asyncio.gather(*(one(m) for m in data))
    log("meetings done")


async def stage_stats(client: httpx.AsyncClient, token: str) -> None:
    STATS_DIR.mkdir(parents=True, exist_ok=True)
    variants = [
        ("rm_stats_total", {}),
        ("rm_stats_hk_opened", {"hongkongServiceFlag": "是"}),
        ("rm_stats_hk_not_opened", {"hongkongServiceFlag": "否"}),
        ("rm_stats_self_developed", {"selfDevelopedFlag": "是"}),
        ("rm_stats_referral", {"selfDevelopedFlag": "否"}),
    ]
    # Common customer levels seen in the data; if a level returns count=0, that's fine
    levels = ["家办", "黑卡", "钻石", "白金", "金", "银", "铜", "Platinum", "Gold"]
    for lv in levels:
        variants.append((f"rm_stats_by_level_{lv}", {"customerLevel": [lv]}))
    # By star level 0-10
    for s in range(0, 11):
        variants.append((f"rm_stats_by_star_{s}", {"starLevel": [s]}))

    for name, args in variants:
        out = STATS_DIR / f"{name}.json"
        if is_complete(out):
            continue
        try:
            data = await call_tool(client, token, "hk_get_rm_stats_total", args)
            write_json_atomic(out, {"args": args, "result": data})
        except Exception as e:
            write_json_atomic(STATS_DIR / f"{name}.error.json", {"args": args, "error": str(e)})
    log(f"stats done: {len(variants)} variants")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["list", "details", "meetings", "stats", "all"], default="all")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--limit", type=int, default=0, help="limit number of group_nos for testing")
    args = parser.parse_args()

    DATA.mkdir(parents=True, exist_ok=True)
    token = load_token()
    limits = httpx.Limits(max_connections=args.concurrency * 2, max_keepalive_connections=args.concurrency * 2)
    async with httpx.AsyncClient(http2=False, limits=limits) as client:
        await initialize_session(client, token)

        if args.stage in ("list", "all", "details"):
            index_path = LIST_DIR / "_index.json"
            if not is_complete(index_path):
                group_nos = await stage_customer_list(client, token, args.concurrency)
            else:
                log("customer list already complete; loading index")
                group_nos = sorted(json.loads(index_path.read_text()).keys())
                log(f"loaded {len(group_nos)} group_nos from index")
        else:
            group_nos = []

        if args.stage in ("stats", "all"):
            await stage_stats(client, token)

        if args.stage in ("meetings", "all"):
            await stage_meetings(client, token, min(args.concurrency, 8))

        if args.stage in ("details", "all"):
            if args.limit > 0:
                group_nos = group_nos[: args.limit]
            await stage_details(client, token, group_nos, args.concurrency)

    log("ALL DONE")


if __name__ == "__main__":
    asyncio.run(main())
