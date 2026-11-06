#!/usr/bin/env python3
"""Deploy the NEW ``alice-code-bridge`` Worker through the Cloudflare REST API.

Authorisation is taken from the already granted Wrangler OAuth credential via
``zhi_guang/pc/alice_cloudflare.access_token()``. That helper is imported for
its refresh-only token accessor; ``prepare()`` / ``deploy_api()`` are never
called, so the existing lamp Worker, its bindings and its local config stay
untouched.

Creating a Worker requires a missing script (HTTP 404). Updating an existing
one requires ``--update`` and is refused when the deployed bindings differ from
the expected set, so no binding is ever dropped silently. Secret values, tokens
and the account id are never printed.

Exit codes: 0 deployed, 3 local secrets missing (deployment left to the
parent), 4 Worker already exists, 5 Cloudflare or helper failure.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOCAL = HERE.parent
SECRETS = LOCAL / "secrets.json"
DEPLOYMENT = HERE / "deployment.json"
MODULE = HERE / "worker.js"

NAME = "alice-code-bridge"
CLASS = "AliceCodeBridge"
EXPECTED_BINDINGS = {"BRIDGE", "AGENT_KEY", "WEBHOOK_SECRET"}
COMPATIBILITY_DATE = "2026-10-01"
API = "https://api.cloudflare.com/client/v4"
HELPER_DIR = Path(os.environ.get("ALICE_HELPER_DIR", str(Path("H:/workspace/zhi_guang/pc"))))
LAMP_CONFIG = HELPER_DIR / "alice" / "config.json"


def read_secrets() -> dict | None:
    if not SECRETS.exists():
        return None
    try:
        data = json.loads(SECRETS.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not data.get("agent_key") or not data.get("webhook_secret"):
        return None
    return data


def access_token() -> str:
    """Refresh-only helper import; never triggers a deployment side effect."""
    if not HELPER_DIR.is_dir():
        raise RuntimeError(f"helper project not found: {HELPER_DIR}")
    sys.path.insert(0, str(HELPER_DIR))
    try:
        from alice_cloudflare import access_token as helper_token
    except Exception as error:  # pragma: no cover - environment dependent
        raise RuntimeError(f"cannot import the Cloudflare helper: {type(error).__name__}") from None
    return helper_token()


def lamp_account_id() -> str:
    """Read the account id the lamp Worker already deploys to, without echoing it."""
    try:
        data = json.loads(LAMP_CONFIG.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    return str(data.get("cloudflare_account_id") or "")


class Api:
    def __init__(self, token: str, redactions: tuple[str, ...]):
        import requests

        self._requests = requests
        self._redactions = tuple(value for value in redactions if value)
        self.session = requests.Session()
        self.session.headers["Authorization"] = "Bearer " + token
        self.session.headers["Connection"] = "close"

    def protect(self, value: str) -> None:
        if value:
            self._redactions = self._redactions + (value,)

    def scrub(self, text: str) -> str:
        for value in self._redactions:
            text = text.replace(value, "[hidden]")
        return text

    def call(self, method: str, path: str, missing_ok: bool = False, **kwargs):
        response = self.session.request(method, API + path, timeout=(10, 60), **kwargs)
        if missing_ok and response.status_code == 404:
            return None
        try:
            data = response.json()
        except ValueError:
            raise RuntimeError(f"Cloudflare API: HTTP {response.status_code} without a JSON body") from None
        if not response.ok or not data.get("success"):
            message = "; ".join(f"{e.get('code')}: {e.get('message')}" for e in data.get("errors", []))
            raise RuntimeError(self.scrub(f"Cloudflare API HTTP {response.status_code}: {message[:600]}"))
        return data.get("result")

    def raw(self, path: str, missing_ok: bool = False) -> bytes:
        response = self.session.request("GET", API + path, timeout=(10, 60))
        if missing_ok and response.status_code == 404:
            return b""
        if not response.ok:
            raise RuntimeError(self.scrub(f"Cloudflare API HTTP {response.status_code} reading script source"))
        return response.content

    def close(self) -> None:
        self.session.close()


def account_id(api: Api) -> str:
    preferred = lamp_account_id()
    accounts = api.call("GET", "/accounts") or []
    if preferred:
        for account in accounts:
            if account.get("id") == preferred:
                return preferred
        raise RuntimeError("the lamp account id is not visible to this Cloudflare grant")
    if len(accounts) == 1:
        return accounts[0]["id"]
    raise RuntimeError("several Cloudflare accounts are visible; cannot choose one")


def metadata(agent_key: str, webhook_secret: str, fresh: bool) -> dict:
    body = {
        "main_module": "worker.js",
        "compatibility_date": COMPATIBILITY_DATE,
        "observability": {"enabled": False},
        "bindings": [
            {"name": "BRIDGE", "type": "durable_object_namespace", "class_name": CLASS},
            {"name": "AGENT_KEY", "type": "secret_text", "text": agent_key},
            {"name": "WEBHOOK_SECRET", "type": "secret_text", "text": webhook_secret},
        ],
    }
    if fresh:
        body["migrations"] = {"new_tag": "v1", "new_sqlite_classes": [CLASS]}
    return body


def binding_summary(settings: dict) -> list[str]:
    lines = []
    for binding in settings.get("bindings") or []:
        kind = binding.get("type")
        if kind == "durable_object_namespace":
            lines.append(f"{binding.get('name')}: {kind} class={binding.get('class_name')}")
        elif kind == "secret_text":
            lines.append(f"{binding.get('name')}: {kind} (value hidden)")
        else:
            lines.append(f"{binding.get('name')}: {kind}")
    return lines


def probe(url: str, attempts: int = 1, expected: int | None = None) -> tuple[int, str]:
    """GET a public URL, retrying while the new Worker propagates."""
    import time

    import requests

    status, body = 0, ""
    for attempt in range(attempts):
        try:
            response = requests.get(url, timeout=(10, 25))
            status, body = response.status_code, response.text[:200]
        except Exception as error:  # network hiccup during propagation
            status, body = 0, type(error).__name__
        if expected is None or status == expected:
            break
        if attempt + 1 < attempts:
            time.sleep(2)
    return status, body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deploy the alice-code-bridge relay Worker")
    parser.add_argument("--check", action="store_true", help="verify preconditions without writing anything")
    parser.add_argument("--update", action="store_true", help="update the existing script, keeping its bindings")
    parser.add_argument("--verify", action="store_true", help="read back an existing deployment without writing")
    args = parser.parse_args(argv)

    secrets = read_secrets()
    if secrets is None:
        print("local-deploy/secrets.json with agent_key and webhook_secret is required; nothing deployed")
        return 3
    agent_key, webhook_secret = secrets["agent_key"], secrets["webhook_secret"]

    if not MODULE.is_file():
        print(f"missing module: {MODULE.name}")
        return 5

    try:
        token = access_token()
    except Exception as error:
        print(f"Cloudflare authorisation unavailable: {error}")
        return 5

    api = Api(token, (token, agent_key, webhook_secret, lamp_account_id()))
    try:
        account = account_id(api)
        api.protect(account)
        prefix = f"/accounts/{account}/workers"
        domain = api.call("GET", prefix + "/subdomain", missing_ok=True) or {}
        subdomain = domain.get("subdomain")
        if not subdomain:
            print("the account has no workers.dev subdomain; nothing deployed")
            return 5

        script = f"{prefix}/scripts/{NAME}"
        existing = api.call("GET", script + "/settings", missing_ok=True)
        if args.verify and existing is None:
            print(f"Worker {NAME} does not exist; nothing to verify")
            return 5
        fresh = existing is None
        if not args.verify and not fresh and not args.update:
            print(f"Worker {NAME} already exists; pass --update to change it")
            return 4
        if not fresh:
            deployed = {binding.get("name") for binding in existing.get("bindings") or []}
            if deployed != EXPECTED_BINDINGS:
                print(f"deployed bindings {sorted(deployed)} differ from {sorted(EXPECTED_BINDINGS)}; refusing to overwrite")
                return 4

        print(f"zone {subdomain}")
        if not args.verify:
            verb = "create" if fresh else "update"
            print(f"plan: {verb} module {NAME} with class {CLASS}, then enable {NAME}.{subdomain}.workers.dev")
            if args.check:
                print("check only: preconditions satisfied, nothing written")
                return 0
            api.call(
                "PUT",
                script,
                files={
                    "metadata": (None, json.dumps(metadata(agent_key, webhook_secret, fresh)), "application/json"),
                    "worker.js": ("worker.js", MODULE.read_bytes(), "application/javascript+module"),
                },
            )
            api.call("POST", script + "/subdomain", json={"enabled": True, "previews_enabled": False})
        else:
            print("verify only: reading back the existing script")

        settings = api.call("GET", script + "/settings") or {}
        source = api.raw(script + "/content/v2", missing_ok=True)
        names = re.findall(rb'filename="([^"]+)"', source)
        encoded = f"export class {CLASS}".encode()
        main_module = names[0].decode() if names and encoded in source else None
        bindings = binding_summary(settings)
        public_url = f"https://{NAME}.{subdomain}.workers.dev"
        print("settings keys:", ", ".join(sorted(settings)))
        print("compatibility_date:", settings.get("compatibility_date"))
        print("main_module:", main_module)
        for line in bindings:
            print("binding:", line)

        ok_health, body_health = probe(public_url + "/health", attempts=6, expected=200)
        ok_unknown, _ = probe(public_url + "/definitely-not-a-route", attempts=3, expected=404)
        print("health:", ok_health, body_health)
        print("unknown route:", ok_unknown)
        print("public_url:", public_url)

        verified = (
            main_module == "worker.js"
            and settings.get("compatibility_date") == COMPATIBILITY_DATE
            and any("durable_object_namespace" in line and CLASS in line for line in bindings)
            and sum("secret_text" in line for line in bindings) == 2
            and ok_health == 200
            and ok_unknown == 404
        )

        DEPLOYMENT.write_text(
            json.dumps(
                {
                    "worker_name": NAME,
                    "public_url": public_url,
                    "account_id": account,
                    "durable_object_class": CLASS,
                    "main_module": main_module,
                    "bindings": bindings,
                    "health_status": ok_health,
                    "unknown_route_status": ok_unknown,
                    "verified": verified,
                    "deployed_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print("verified:", verified)
        return 0 if verified else 5
    except RuntimeError as error:
        print(f"deployment failed: {api.scrub(str(error))}")
        return 5
    finally:
        api.close()


if __name__ == "__main__":
    raise SystemExit(main())
