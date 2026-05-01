"""
Automatic GitHub Issue reporter for daemon errors.

When the daemon hits a transcode/scan/upload failure, this module opens
(or appends to) a GitHub Issue on the configured repo so the operator
doesn't have to copy logs by hand. Every report includes the context
needed to diagnose: stage, error message, log tail, encoder, ffmpeg
version, OS, and the daemon version.

Throttling: identical errors within `throttle_sec` are coalesced into
comments on the same open issue, keyed by a short fingerprint computed
from kind + summary. Beyond `throttle_sec`, a new comment is posted.
After the issue is closed, the next occurrence opens a fresh one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
INCIDENT_LABEL = "auto-incident"


@dataclass
class _ThrottleEntry:
    issue_number: int
    last_reported_at: float
    occurrences: int = 1


class IncidentReporter:
    """Posts errors to GitHub Issues via REST API."""

    def __init__(
        self,
        repo: str,
        token: str,
        throttle_sec: int = 600,
        version: str = "",
    ) -> None:
        self.repo = repo  # "owner/name"
        self.token = token
        self.throttle_sec = throttle_sec
        self.version = version
        self._lock = threading.Lock()
        self._throttle: dict[str, _ThrottleEntry] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.repo and self.token)

    def report(
        self,
        kind: str,
        summary: str,
        log_tail: str = "",
        context: dict | None = None,
    ) -> str | None:
        """Open or update an incident issue. Returns the issue URL on success."""
        if not self.enabled:
            return None

        fingerprint = self._fingerprint(kind, summary)
        body = self._compose_body(kind, summary, log_tail, context or {})

        try:
            with self._lock:
                entry = self._throttle.get(fingerprint)
                now = time.time()

                if entry and (now - entry.last_reported_at) < self.throttle_sec:
                    entry.occurrences += 1
                    return self._issue_url(entry.issue_number)

                # Either no entry or throttle window elapsed; verify the issue
                # is still open before re-reporting against it.
                if entry:
                    if self._issue_open(entry.issue_number):
                        self._add_comment(entry.issue_number, body)
                        entry.last_reported_at = now
                        entry.occurrences += 1
                        return self._issue_url(entry.issue_number)
                    # Closed: drop the entry so we open a fresh one.
                    self._throttle.pop(fingerprint, None)

                # Look for an existing open issue tagged with this fingerprint
                existing = self._find_open_issue(fingerprint)
                if existing is not None:
                    self._add_comment(existing, body)
                    self._throttle[fingerprint] = _ThrottleEntry(
                        issue_number=existing,
                        last_reported_at=now,
                    )
                    return self._issue_url(existing)

                # Create a new issue
                title = self._compose_title(kind, summary)
                number = self._create_issue(title, body, fingerprint, kind)
                if number is not None:
                    self._throttle[fingerprint] = _ThrottleEntry(
                        issue_number=number,
                        last_reported_at=now,
                    )
                    return self._issue_url(number)
                return None

        except Exception as e:
            logger.warning(f"incident report failed: {e}")
            return None

    # ------------------------------------------------------------ formatting

    def _fingerprint(self, kind: str, summary: str) -> str:
        h = hashlib.sha1(f"{kind}|{summary}".encode("utf-8")).hexdigest()[:10]
        return f"fp:{h}"

    def _compose_title(self, kind: str, summary: str) -> str:
        # Cap length so it fits GitHub's 256-char limit comfortably.
        s = summary.strip().splitlines()[0] if summary else "(no summary)"
        if len(s) > 140:
            s = s[:137] + "..."
        return f"[{kind}] {s}"

    def _compose_body(
        self,
        kind: str,
        summary: str,
        log_tail: str,
        context: dict,
    ) -> str:
        ts = datetime.now(timezone.utc).isoformat()
        ctx_lines = [f"- **{k}**: {v}" for k, v in context.items()]
        env = [
            f"- daemon version: `{self.version or 'unknown'}`",
            f"- OS: `{platform.platform()}`",
            f"- python: `{platform.python_version()}`",
        ]
        body = [
            f"**Kind:** {kind}  ",
            f"**Reported at:** `{ts}`  ",
            "",
            f"**Summary:**  ",
            f"```",
            summary.strip(),
            f"```",
        ]
        if ctx_lines:
            body.extend(["", "**Job context:**"] + ctx_lines)
        body.extend(["", "**Daemon environment:**"] + env)
        if log_tail.strip():
            body.extend([
                "",
                "**Log tail:**",
                "```",
                log_tail.strip(),
                "```",
            ])
        return "\n".join(body)

    # --------------------------------------------------------------- HTTP

    def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
    ) -> tuple[int, dict | list | None]:
        url = f"{GITHUB_API}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "heavydrops-transcoder",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            try:
                err_body = json.loads(e.read().decode("utf-8"))
            except Exception:
                err_body = None
            logger.warning(f"github API {method} {path} → {e.code} {err_body}")
            return e.code, err_body

    def _create_issue(
        self,
        title: str,
        body: str,
        fingerprint: str,
        kind: str,
    ) -> int | None:
        # Embed the fingerprint at the end of the body so we can find it later
        # without relying on labels (which need to exist on the repo).
        marker = f"\n\n<!-- {fingerprint} -->"
        status, data = self._request(
            "POST",
            f"/repos/{self.repo}/issues",
            {
                "title": title,
                "body": body + marker,
                "labels": [INCIDENT_LABEL, f"kind:{kind}"],
            },
        )
        if status in (200, 201) and isinstance(data, dict):
            return int(data.get("number", 0)) or None
        # Retry without labels in case the repo doesn't have them; GitHub
        # auto-creates labels for the authenticated user, but only if the
        # token has the right scopes.
        if status in (403, 404, 422):
            status2, data2 = self._request(
                "POST",
                f"/repos/{self.repo}/issues",
                {"title": title, "body": body + marker},
            )
            if status2 in (200, 201) and isinstance(data2, dict):
                return int(data2.get("number", 0)) or None
        return None

    def _add_comment(self, issue_number: int, body: str) -> bool:
        status, _ = self._request(
            "POST",
            f"/repos/{self.repo}/issues/{issue_number}/comments",
            {"body": body},
        )
        return status in (200, 201)

    def _issue_open(self, issue_number: int) -> bool:
        status, data = self._request(
            "GET",
            f"/repos/{self.repo}/issues/{issue_number}",
        )
        if status == 200 and isinstance(data, dict):
            return data.get("state") == "open"
        return False

    def _find_open_issue(self, fingerprint: str) -> int | None:
        # Search for an open issue containing the fingerprint marker. GitHub's
        # search API is rate-limited but this only fires on first occurrence
        # within a throttle window.
        owner_repo = self.repo
        q = urllib.parse.quote(f"repo:{owner_repo} is:issue is:open in:body {fingerprint}")
        status, data = self._request("GET", f"/search/issues?q={q}")
        if status == 200 and isinstance(data, dict):
            items = data.get("items") or []
            if items:
                return int(items[0].get("number", 0)) or None
        return None

    def _issue_url(self, number: int) -> str:
        return f"https://github.com/{self.repo}/issues/{number}"
