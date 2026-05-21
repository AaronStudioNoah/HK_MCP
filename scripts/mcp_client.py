"""Async client for the Noah HK CRM MCP server via Anthropic proxy."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx

TOKEN_FILE = "/home/claude/.claude/remote/.session_ingress_token"
SESSION_UUID = "cse_01NWGserL9Kh4owCjYgpHTtR"
MCP_SERVER_ID = "c73b1af0-e297-49a6-bf96-ebf0b6c820c2"
TOOLBOX_MCP_SERVER_ID = "c73b1af0-e297-49a6-bf96-ebf0b6c820c2"
UPSTREAM_MCP_SERVER_ID = "e9469c3d-88c5-5b9e-8a4f-76feeeabf7de"
UPSTREAM_URL = "https://ai-crm.noahgroup.com/mcp/crm-customers-hk/mcp"

PROXY_URL = (
    f"https://api.anthropic.com/v2/ccr-sessions/{SESSION_UUID}/mcp"
    f"?mcp_url={httpx.QueryParams({'x': UPSTREAM_URL})['x']}"
    f"&mcp_server_id={UPSTREAM_MCP_SERVER_ID}"
    f"&toolbox_mcp_server_id={TOOLBOX_MCP_SERVER_ID}"
)
# httpx auto-encoded the url. Use raw construction instead to match curl exactly.
import urllib.parse
PROXY_URL = (
    f"https://api.anthropic.com/v2/ccr-sessions/{SESSION_UUID}/mcp"
    f"?mcp_url={urllib.parse.quote(UPSTREAM_URL, safe='')}"
    f"&mcp_server_id={UPSTREAM_MCP_SERVER_ID}"
    f"&toolbox_mcp_server_id={TOOLBOX_MCP_SERVER_ID}"
)


def load_token() -> str:
    return Path(TOKEN_FILE).read_text().strip()


def headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-MCP-Server-ID": MCP_SERVER_ID,
        "X-Session-UUID": SESSION_UUID,
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }


def parse_sse(body: str) -> dict[str, Any]:
    for line in body.splitlines():
        if line.startswith("data:"):
            payload = line[5:].strip()
            if payload:
                return json.loads(payload)
    raise RuntimeError(f"No SSE data in response: {body[:300]}")


async def call_tool(
    client: httpx.AsyncClient,
    token: str,
    tool_name: str,
    arguments: dict[str, Any],
    request_id: int = 1,
    timeout: float = 90.0,
    retries: int = 4,
) -> Any:
    """Call a single MCP tool. Returns parsed tool result (the inner JSON string parsed)."""
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            resp = await client.post(
                PROXY_URL,
                headers=headers(token),
                content=json.dumps(payload),
                timeout=timeout,
            )
            resp.raise_for_status()
            envelope = parse_sse(resp.text)
            if "error" in envelope:
                raise RuntimeError(f"MCP error: {envelope['error']}")
            result = envelope["result"]
            if result.get("isError"):
                raise RuntimeError(f"Tool error: {result}")
            # Prefer structuredContent.result (string of JSON) if present
            sc = result.get("structuredContent")
            if sc and "result" in sc:
                inner = sc["result"]
                if isinstance(inner, str):
                    try:
                        return json.loads(inner)
                    except json.JSONDecodeError:
                        return inner
                return inner
            # Fallback: content[0].text
            content = result.get("content", [])
            if content and content[0].get("type") == "text":
                text = content[0]["text"]
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return text
            return result
        except (httpx.HTTPError, RuntimeError) as e:
            last_err = e
            wait = min(2 ** attempt, 16)
            await asyncio.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


async def initialize_session(client: httpx.AsyncClient, token: str) -> None:
    """Send initialize + notifications/initialized handshake. Idempotent for stateless proxy."""
    init_payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "hk-crm-downloader", "version": "1.0"},
        },
    }
    r = await client.post(PROXY_URL, headers=headers(token), content=json.dumps(init_payload), timeout=30)
    r.raise_for_status()
    notify_payload = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    await client.post(PROXY_URL, headers=headers(token), content=json.dumps(notify_payload), timeout=30)
