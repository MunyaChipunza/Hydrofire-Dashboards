from __future__ import annotations

import json
import os
import shutil
import base64
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

from live_dashboard_server import build_dashboard_payload, workbook_snapshot


ROOT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_JSON = ROOT_DIR / "dashboard_data.json"


def workbook_source() -> str:
    source = os.environ.get("WORKBOOK_URL", "").strip()
    if not source:
        raise SystemExit("WORKBOOK_URL secret is empty. Add it in the GitHub repository secrets.")
    return source


def with_download_hint(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    keys = {key.lower() for key, _ in query}
    if "download" not in keys:
        query.append(("download", "1"))
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment))


def candidate_urls(url: str) -> list[str]:
    candidates = [url]
    hinted = with_download_hint(url)
    if hinted != url:
        candidates.append(hinted)
    return candidates


def share_token(url: str) -> str:
    raw = base64.b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")
    return "u!" + raw.replace("/", "_").replace("+", "-")


def request_json(url: str, headers: dict[str, str] | None = None, data: bytes | None = None) -> dict:
    request = urllib.request.Request(url, headers=headers or {}, data=data)
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))


def request_bytes(url: str, headers: dict[str, str] | None = None) -> bytes:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request) as response:
        return response.read()


def find_download_url(payload):
    if isinstance(payload, dict):
        for key, value in payload.items():
            if "downloadurl" in key.lower() and isinstance(value, str) and value.startswith("http"):
                return value
            found = find_download_url(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_download_url(item)
            if found:
                return found
    return None


def onedrive_badger_headers() -> dict[str, str]:
    token_payload = request_json(
        "https://api-badgerp.svc.ms/v1.0/token",
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        data=json.dumps({"appId": "5cbed6ac-a083-4e14-b191-b4ba07653de2"}).encode("utf-8"),
    )
    token = token_payload.get("token")
    if not token:
        raise RuntimeError("Could not get OneDrive public access token.")
    return {
        "Authorization": f"Badger {token}",
        "Prefer": "autoredeem",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, */*",
    }


def download_onedrive_share(source: str, target_dir: Path) -> Path:
    token = share_token(source)
    headers = onedrive_badger_headers()
    metadata = request_json(f"https://api.onedrive.com/v1.0/shares/{token}/driveItem", headers=headers)

    download_url = find_download_url(metadata)
    if download_url:
        data = request_bytes(download_url, headers={"User-Agent": "Mozilla/5.0"})
        filename = metadata.get("name") or "downloaded_workbook.xlsm"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            filename = f"{Path(filename).stem or 'downloaded_workbook'}.xlsm"
        target = target_dir / filename
        target.write_bytes(data)
        ensure_excel_file(target)
        return target

    for content_url in (
        f"https://api.onedrive.com/v1.0/shares/{token}/driveItem/content",
        f"https://api.onedrive.com/v1.0/shares/{token}/root/content",
    ):
        try:
            data = request_bytes(content_url, headers=headers)
            filename = metadata.get("name") or "downloaded_workbook.xlsm"
            if not filename.lower().endswith((".xlsx", ".xlsm")):
                filename = f"{Path(filename).stem or 'downloaded_workbook'}.xlsm"
            target = target_dir / filename
            target.write_bytes(data)
            ensure_excel_file(target)
            return target
        except Exception:
            continue

    raise RuntimeError("OneDrive share metadata loaded, but no downloadable workbook URL was available.")


def local_copy(source: str, target_dir: Path) -> Path | None:
    local_path = None
    if source.startswith("file:///"):
        local_path = Path(urllib.request.url2pathname(urllib.parse.urlsplit(source).path))
    else:
        raw_path = Path(source)
        if raw_path.exists():
            local_path = raw_path

    if local_path is None:
        return None

    snapshot = workbook_snapshot(local_path)
    copied = target_dir / local_path.name
    shutil.copy2(snapshot, copied)
    snapshot.unlink(missing_ok=True)
    return copied


def guessed_name(url: str, headers) -> str:
    content_disposition = headers.get("Content-Disposition", "")
    if "filename=" in content_disposition:
        filename = content_disposition.split("filename=", 1)[1].strip().strip('"')
        if filename:
            return filename

    parsed = urllib.parse.urlsplit(url)
    filename = Path(parsed.path).name
    if filename:
        return filename
    return "downloaded_workbook.xlsm"


def ensure_excel_file(path: Path) -> None:
    if zipfile.is_zipfile(path):
        return
    raise ValueError(f"Downloaded file is not a valid Excel workbook: {path.name}")


def download_workbook(source: str, target_dir: Path) -> Path:
    local = local_copy(source, target_dir)
    if local is not None:
        ensure_excel_file(local)
        return local

    last_error: Exception | None = None
    for url in candidate_urls(source):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request) as response:
                data = response.read()
                final_url = response.geturl()
                filename = guessed_name(final_url, response.headers)
                if not filename.lower().endswith((".xlsx", ".xlsm")):
                    filename = f"{Path(filename).stem or 'downloaded_workbook'}.xlsm"

                target = target_dir / filename
                target.write_bytes(data)
                ensure_excel_file(target)
                return target
        except Exception as exc:  # pragma: no cover - exercised in GitHub Actions
            last_error = exc

    source_lower = source.lower()
    if "1drv.ms" in source_lower or "onedrive.live.com" in source_lower:
        try:
            return download_onedrive_share(source, target_dir)
        except Exception as exc:  # pragma: no cover - exercised in GitHub Actions
            last_error = exc

    raise last_error or RuntimeError("Could not download workbook.")


def main() -> None:
    source = workbook_source()
    tmp_dir = ROOT_DIR / ".tmp_workbook"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    try:
        workbook = download_workbook(source, tmp_dir)
        payload = build_dashboard_payload(tmp_dir, workbook.name)
        payload.setdefault("meta", {})
        payload["meta"]["publishedFrom"] = source
        OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {OUTPUT_JSON}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
