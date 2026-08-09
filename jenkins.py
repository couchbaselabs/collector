"""
Jenkins HTTP client.

One requests.Session per hostname → HTTP keep-alive connection reuse across
calls to the same server (vs. the old code which opened a new TCP socket for
every single API call).

Thread-safety: each worker process gets its own JenkinsClient instance
(created in the pool initializer), so no locking is needed.
"""
from __future__ import annotations

import configparser
import json
import logging
import time
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class JenkinsClient:

    def __init__(
        self,
        credentials: Dict[str, Tuple[str, str]],
        timeout: int = 15,
        console_timeout: int = 5,
        max_retries: int = 5,
    ) -> None:
        self._credentials = credentials          # {base_url: (user, pass)}
        self._timeout = timeout
        self._console_timeout = console_timeout
        self._max_retries = max_retries
        self._sessions: Dict[str, requests.Session] = {}  # keyed by netloc

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_ini(
        cls,
        path: str = "credentials.ini",
        timeout: int = 15,
        console_timeout: int = 5,
    ) -> "JenkinsClient":
        cfg = configparser.ConfigParser()
        cfg.read(path)
        creds: Dict[str, Tuple[str, str]] = {}
        for section in cfg.sections():
            try:
                creds[section] = (cfg.get(section, "username"),
                                  cfg.get(section, "password"))
            except configparser.NoOptionError:
                pass
        return cls(creds, timeout=timeout, console_timeout=console_timeout)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _session(self, url: str) -> requests.Session:
        netloc = urlparse(url).netloc
        if netloc not in self._sessions:
            session = requests.Session()
            auth = self._auth_for(url)
            if auth:
                session.auth = auth
            self._sessions[netloc] = session
        return self._sessions[netloc]

    def _auth_for(self, url: str) -> Optional[HTTPBasicAuth]:
        for base, (user, pwd) in self._credentials.items():
            if url.startswith(base):
                return HTTPBasicAuth(user, pwd)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_json_status(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        append_api: bool = True,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Fetch JSON and report the *outcome*, so callers can distinguish a build
        that is genuinely gone from one that merely couldn't be read this time:

            ("ok", data)      — HTTP 200, parsed JSON
            ("missing", None) — HTTP 404, the resource truly does not exist
            ("error", None)   — non-404 HTTP error, timeout, or exception (transient)

        This distinction matters for the executor build walk: only a confirmed 404
        means "this build was wiped and every older one is too" (safe to stop). A
        timeout or a still-running build must NOT be mistaken for the retention
        boundary, or the walk aborts early and drops thousands of live builds.
        """
        target = f"{url.rstrip('/')}/api/json" if append_api else url
        session = self._session(url)
        backoff = 1
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = session.get(target, params=params, timeout=self._timeout)
                if resp.status_code == 404:
                    logger.debug("404 %s", target)
                    return "missing", None
                if resp.status_code != 200:
                    logger.warning("HTTP %d for %s", resp.status_code, target)
                    return "error", None
                return "ok", resp.json()
            except requests.Timeout:
                logger.warning("Timeout attempt %d/%d: %s", attempt, self._max_retries, target)
            except Exception as exc:
                logger.warning("Request error attempt %d/%d %s: %s",
                               attempt, self._max_retries, target, exc)
            if attempt < self._max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        return "error", None

    def get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        append_api: bool = True,
    ) -> Optional[Dict[str, Any]]:
        _, data = self.get_json_status(url, params, append_api)
        return data

    def stream_console_lines(self, job_url: str) -> Iterator[str]:
        """Yield decoded lines from /consoleText with a hard wall-clock timeout."""
        target = f"{job_url.rstrip('/')}/consoleText"
        session = self._session(job_url)
        deadline = time.monotonic() + self._console_timeout
        try:
            with session.get(target, stream=True,
                             timeout=self._console_timeout) as resp:
                if resp.status_code != 200:
                    return
                for line in resp.iter_lines(decode_unicode=True):
                    if time.monotonic() > deadline:
                        break
                    yield line
        except Exception as exc:
            logger.debug("Console stream error %s: %s", job_url, exc)

    def get_test_report(self, job_url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse /testReport/api/json with streaming + timeout."""
        target = f"{job_url.rstrip('/')}/testReport/api/json"
        session = self._session(job_url)
        deadline = time.monotonic() + self._console_timeout
        buf = ""
        try:
            with session.get(target, stream=True,
                             timeout=self._console_timeout) as resp:
                if resp.status_code != 200:
                    return None
                for chunk in resp.iter_content(decode_unicode=True):
                    if time.monotonic() > deadline:
                        logger.debug("Test report timeout %s", job_url)
                        return None
                    buf += chunk
            return json.loads(buf)
        except Exception as exc:
            logger.debug("Test report error %s: %s", job_url, exc)
            return None
