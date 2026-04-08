from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS = {"a": MAIN_NS, "r": REL_NS}
SAST = ZoneInfo("Africa/Johannesburg")

MONTH_INDEX = {
    "jan": 0,
    "january": 0,
    "feb": 1,
    "february": 1,
    "mar": 2,
    "march": 2,
    "apr": 3,
    "april": 3,
    "may": 4,
    "jun": 5,
    "june": 5,
    "jul": 6,
    "july": 6,
    "aug": 7,
    "august": 7,
    "sep": 8,
    "sept": 8,
    "september": 8,
    "oct": 9,
    "october": 9,
    "nov": 10,
    "november": 10,
    "dec": 11,
    "december": 11,
}

CANONICAL_KPI_KEYS = {
    "stocktransfercompliance": "stockTransfer",
    "stocktransfer": "stockTransfer",
    "scanningcompliance": "scanning",
    "scanning": "scanning",
    "fillingoutsheets": "sheets",
    "sheetscompliance": "sheets",
    "dispatchaccuracy": "dispatch",
    "truckloadingaccuracy": "truckLoading",
    "truckloading": "truckLoading",
    "stockcountaccuracy": "stockCount",
    "stockcount": "stockCount",
    "localpartsavailability": "localParts",
    "localparts": "localParts",
    "liseoassembledreadytobuy": "liseoRTB",
    "liseortb": "liseoRTB",
    "coverplatesnodelays": "coverPlates",
    "coverplates": "coverPlates",
    "liseoissuesimproveyoy": "liseoIssues",
    "liseoissues": "liseoIssues",
    "takealotandodoperformance": "tkOdo",
    "takealotodoperformance": "tkOdo",
    "tkandodogrowth": "tkOdo",
    "tkodogrowth": "tkOdo",
    "monthlystockcounts": "monthlySC",
    "monthlysc": "monthlySC",
}


def normalize_label(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", text)


def slugify(value: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return base or "row"


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "n/a", "#n/a"}:
        return None

    text = text.replace("%", "").replace(" ", "")
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def as_percent(value: Any) -> float | None:
    number = safe_float(value)
    if number is None:
        return None
    return round(number * 100 if abs(number) <= 10 else number, 2)


def as_raw_number(value: Any) -> float | None:
    number = safe_float(value)
    if number is None:
        return None
    return round(number, 2)


def extract_prior_year_value(comment: Any, canonical: str | None) -> float | None:
    if comment is None:
        return None

    text = str(comment).strip()
    if not text:
        return None

    if canonical == "liseoIssues":
        match = re.search(r"2025\s+(?:had|was)\s+([0-9]+(?:[.,][0-9]+)?)", text, re.IGNORECASE)
        return as_raw_number(match.group(1)) if match else None

    match = re.search(r"2025[^0-9]{0,24}([0-9]+(?:[.,][0-9]+)?)\s*%?", text, re.IGNORECASE)
    return as_percent(match.group(1)) if match else None


def non_empty(value: Any) -> bool:
    return value is not None and str(value).strip() != "" and str(value).strip().lower() != "none"


def truthy_cell(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if not text or text in {"none", "null", "false", "no"}:
        return False

    number = safe_float(value)
    if number is None:
        return True
    return abs(number) > 0


def excel_serial_to_label(number: float) -> str:
    if not 20000 <= number <= 70000:
        return str(number)
    dt = datetime(1899, 12, 30) + timedelta(days=number)
    return dt.strftime("%d %b").lstrip("0")


def header_label(value: Any) -> str:
    if value is None:
        return ""
    number = safe_float(value)
    if number is not None and 20000 <= number <= 70000:
        return excel_serial_to_label(number)
    text = str(value).strip()
    return text.replace("â€”", "-")


def month_slot(value: Any) -> int | None:
    key = normalize_label(value)
    return MONTH_INDEX.get(key)


def percent_series() -> list[float | None]:
    return [None] * 12


def raw_series() -> list[float | None]:
    return [None] * 12


def workbook_snapshot(workbook_path: Path, attempts: int = 6, delay: float = 0.25) -> Path:
    last_error: Exception | None = None
    for _ in range(attempts):
        handle, temp_name = tempfile.mkstemp(suffix=workbook_path.suffix)
        os.close(handle)
        temp_path = Path(temp_name)
        try:
            shutil.copy2(workbook_path, temp_path)
            return temp_path
        except Exception as exc:  # pragma: no cover - Windows file locking is environment-specific
            last_error = exc
            temp_path.unlink(missing_ok=True)
            time.sleep(delay)
    raise last_error or PermissionError(f"Could not read workbook: {workbook_path}")


class WorkbookReader:
    def __init__(self, workbook_path: Path) -> None:
        self.workbook_path = workbook_path
        self.archive = zipfile.ZipFile(workbook_path)
        self.shared_strings = self._load_shared_strings()
        self.sheet_targets = self._load_sheet_targets()

    def close(self) -> None:
        self.archive.close()

    def _load_shared_strings(self) -> list[str]:
        name = "xl/sharedStrings.xml"
        if name not in self.archive.namelist():
            return []

        root = ET.fromstring(self.archive.read(name))
        strings: list[str] = []
        for item in root.findall("a:si", NS):
            parts = [node.text or "" for node in item.iter(f"{{{MAIN_NS}}}t")]
            strings.append("".join(parts))
        return strings

    def _load_sheet_targets(self) -> dict[str, str]:
        workbook = ET.fromstring(self.archive.read("xl/workbook.xml"))
        rels = ET.fromstring(self.archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}

        targets: dict[str, str] = {}
        for sheet in workbook.find("a:sheets", NS) or []:
            name = sheet.attrib["name"]
            rid = sheet.attrib[f"{{{REL_NS}}}id"]
            target = rel_map[rid]
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            targets[name] = target
        return targets

    def sheet(self, name: str) -> dict[int, dict[int, Any]]:
        if name not in self.sheet_targets:
            raise KeyError(f"Sheet not found: {name}")

        root = ET.fromstring(self.archive.read(self.sheet_targets[name]))
        rows: dict[int, dict[int, Any]] = {}

        for row in root.findall(".//a:sheetData/a:row", NS):
            row_index = int(row.attrib["r"])
            current: dict[int, Any] = {}
            for cell in row.findall("a:c", NS):
                ref = cell.attrib.get("r", "")
                match = re.match(r"([A-Z]+)(\d+)", ref)
                if not match:
                    continue
                col_index = self._col_to_index(match.group(1))
                current[col_index] = self._cell_value(cell)
            rows[row_index] = current

        return rows

    def _cell_value(self, cell: ET.Element) -> Any:
        cell_type = cell.attrib.get("t")
        value = cell.find("a:v", NS)
        inline = cell.find("a:is", NS)

        if cell_type == "s" and value is not None and value.text is not None:
            index = int(value.text)
            return self.shared_strings[index] if index < len(self.shared_strings) else None

        if cell_type == "inlineStr" and inline is not None:
            pieces = [node.text or "" for node in inline.iter(f"{{{MAIN_NS}}}t")]
            return "".join(pieces)

        if cell_type == "b" and value is not None and value.text is not None:
            return value.text == "1"

        if value is None or value.text is None:
            return None

        text = value.text
        number = safe_float(text)
        if number is not None:
            return number
        return text

    @staticmethod
    def _col_to_index(label: str) -> int:
        index = 0
        for char in label:
            index = index * 26 + ord(char) - 64
        return index


def row_value(row: dict[int, Any], col: int) -> Any:
    return row.get(col)


def find_header_row(
    rows: dict[int, dict[int, Any]],
    required_headers: set[str],
    scan_to: int = 12,
) -> tuple[int, dict[str, int]]:
    for row_index in range(1, scan_to + 1):
        row = rows.get(row_index, {})
        header_map: dict[str, int] = {}
        for col_index, value in row.items():
            normalized = normalize_label(value)
            if normalized:
                header_map[normalized] = col_index
        if required_headers.issubset(set(header_map)):
            return row_index, header_map
    raise ValueError(f"Could not find headers: {sorted(required_headers)}")


def build_month_lookup(rows: dict[int, dict[int, Any]], month_col: int) -> dict[int, dict[int, Any]]:
    lookup: dict[int, dict[int, Any]] = {}
    for row in rows.values():
        slot = month_slot(row_value(row, month_col))
        if slot is not None:
            lookup[slot] = row
    return lookup


def extract_monthly_percent_series(reader: WorkbookReader, sheet_name: str) -> list[float | None]:
    rows = reader.sheet(sheet_name)
    header_row, header_map = find_header_row(rows, {"month"})

    percent_col = None
    for label, col_index in header_map.items():
        if label in {"", "month"}:
            continue
        if "%" in str(row_value(rows[header_row], col_index)) or label == "":
            percent_col = col_index
            break
    if percent_col is None:
        percent_col = header_map.get("") or header_map.get("percent") or 4

    lookup = build_month_lookup(rows, header_map["month"])
    series = percent_series()
    for slot, row in lookup.items():
        series[slot] = as_percent(row_value(row, percent_col))
    return series


def extract_truck_loads(reader: WorkbookReader) -> tuple[list[float | None], list[float | None]]:
    rows = reader.sheet("Truck Loads")
    jhb: list[float | None] = []
    george: list[float | None] = []

    row_index = 4
    while row_index in rows:
        row = rows[row_index]
        if not any(non_empty(value) for value in row.values()):
            row_index += 1
            continue
        if row_index > 40:
            break
        jhb.append(as_percent(row_value(row, 2)))
        george.append(as_percent(row_value(row, 6)))
        row_index += 1

    while jhb and jhb[-1] is None:
        jhb.pop()
    while george and george[-1] is None:
        george.pop()

    return jhb, george


def extract_stock_count(reader: WorkbookReader) -> tuple[list[float | None], list[float | None]]:
    rows = reader.sheet("Stock Count")
    _, header_map = find_header_row(rows, {"week", "1stcount", "final"})

    week_col = header_map["week"]
    first_col = header_map["1stcount"]
    final_col = header_map["final"]

    first_series: list[float | None] = []
    final_series: list[float | None] = []

    for row_index in sorted(rows):
        row = rows[row_index]
        if month_slot(row_value(row, week_col)) is not None:
            continue
        if safe_float(row_value(row, week_col)) is None:
            continue
        first_series.append(as_percent(row_value(row, first_col)))
        final_series.append(as_percent(row_value(row, final_col)))

    while first_series and first_series[-1] is None and final_series and final_series[-1] is None:
        first_series.pop()
        final_series.pop()

    return first_series, final_series


def extract_liseo_issues(reader: WorkbookReader) -> tuple[list[int | None], float | None]:
    rows = reader.sheet("Liseo Issues")
    _, header_map = find_header_row(rows, {"month"})
    month_col = header_map["month"]
    count_col = next((col for label, col in header_map.items() if label != "month"), 2)

    lookup = build_month_lookup(rows, month_col)
    series: list[int | None] = [None] * 12
    for slot, row in lookup.items():
        number = safe_float(row_value(row, count_col))
        series[slot] = None if number is None else int(round(number))

    score = None
    for row in rows.values():
        if normalize_label(row_value(row, 1)) == "score":
            score = as_percent(row_value(row, count_col))
            break

    return series, score


def extract_tk_odo(reader: WorkbookReader) -> tuple[list[float | None], list[float | None], list[float | None]]:
    rows = reader.sheet("TK & ODO")
    _, header_map = find_header_row(rows, {"month"})
    month_col = header_map["month"]

    tk2025_col = next((col for label, col in header_map.items() if "tk2025" in label), 2)
    tk2026_col = next((col for label, col in header_map.items() if "tk2026" in label), 3)
    odo2026_col = next((col for label, col in header_map.items() if "odo2026" in label), 4)

    lookup = build_month_lookup(rows, month_col)
    tk2025 = raw_series()
    tk2026 = raw_series()
    odo2026 = raw_series()

    for slot, row in lookup.items():
        tk2025[slot] = as_raw_number(row_value(row, tk2025_col))
        tk2026[slot] = as_raw_number(row_value(row, tk2026_col))
        odo2026[slot] = as_raw_number(row_value(row, odo2026_col))

    return tk2025, tk2026, odo2026


def extract_monthly_sc(reader: WorkbookReader) -> list[dict[str, bool]]:
    rows = reader.sheet("Monthly SC")
    _, header_map = find_header_row(rows, {"month", "liseo", "87auckland"})
    month_col = header_map["month"]
    liseo_col = header_map["liseo"]
    a87_col = header_map["87auckland"]

    lookup = build_month_lookup(rows, month_col)
    output = [{"liseo": False, "a87": False} for _ in range(12)]
    for slot, row in lookup.items():
        output[slot] = {
            "liseo": truthy_cell(row_value(row, liseo_col)),
            "a87": truthy_cell(row_value(row, a87_col)),
        }
    return output


def extract_scorecard(reader: WorkbookReader) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = reader.sheet("Scorecard")
    header_row, header_map = find_header_row(rows, {"kpi", "target", "aveytd", "current"})

    kpi_col = header_map["kpi"]
    target_col = header_map["target"]
    ave_col = header_map["aveytd"]
    current_col = header_map["current"]
    comments_col = next((col for label, col in header_map.items() if label == "comments"), current_col + 1)
    number_col = 1

    header_cells = rows[header_row]
    biweekly_cols = [
        col
        for col in sorted(header_cells)
        if col > comments_col and non_empty(row_value(header_cells, col))
    ]

    history_headers = [header_label(row_value(header_cells, col)) for col in biweekly_cols]

    scorecard_rows: list[dict[str, Any]] = []
    kpis: dict[str, Any] = {}

    for row_index in sorted(rows):
        if row_index <= header_row:
            continue
        row = rows[row_index]
        kpi_name = row_value(row, kpi_col)
        if not non_empty(kpi_name):
            continue
        if safe_float(row_value(row, number_col)) is None:
            continue

        name = str(kpi_name).strip().replace("â€”", "-")
        target = as_percent(row_value(row, target_col))
        ave = as_percent(row_value(row, ave_col))
        current = as_percent(row_value(row, current_col))
        history = [as_percent(row_value(row, col)) for col in biweekly_cols]
        comments = row_value(row, comments_col)

        canonical = CANONICAL_KPI_KEYS.get(normalize_label(name))
        prior_year = extract_prior_year_value(comments, canonical)
        row_payload = {
            "id": canonical or slugify(name),
            "number": str(int(safe_float(row_value(row, number_col)))) if safe_float(row_value(row, number_col)) is not None else str(row_value(row, number_col)),
            "name": name,
            "target": target,
            "ave": ave,
            "current": current,
            "history": history,
            "priorYear": prior_year,
            "comments": "" if comments is None else str(comments).strip(),
            "key": canonical,
        }
        scorecard_rows.append(row_payload)

        if canonical:
            kpis[canonical] = {
                "name": name,
                "target": target if target is not None else 0,
                "ave": ave if ave is not None else 0,
                "current": current if current is not None else 0,
                "priorYear": prior_year,
            }

    return {
        "headers": history_headers,
        "rows": scorecard_rows,
    }, kpis


def select_workbook(workdir: Path, explicit: str | None) -> Path:
    if explicit:
        candidate = (workdir / explicit).resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"Workbook not found: {candidate}")
        return candidate

    candidates = sorted(
        (
            path
            for path in workdir.iterdir()
            if path.is_file()
            and path.suffix.lower() in {".xlsm", ".xlsx"}
            and not path.name.startswith("~$")
            and not path.name.startswith(".~lock")
        ),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No .xlsx or .xlsm workbook found in this folder.")
    return candidates[0]


def build_dashboard_payload(workdir: Path, workbook_name: str | None) -> dict[str, Any]:
    workbook_path = select_workbook(workdir, workbook_name)
    snapshot_path = workbook_snapshot(workbook_path)
    reader = WorkbookReader(snapshot_path)
    try:
        scorecard, kpi = extract_scorecard(reader)
        truck_jhb, truck_george = extract_truck_loads(reader)
        stock_count_first, stock_count_final = extract_stock_count(reader)
        liseo_issues, liseo_issue_score = extract_liseo_issues(reader)
        tk2025, tk2026, odo2026 = extract_tk_odo(reader)
        monthly_sc = extract_monthly_sc(reader)

        payload = {
            "meta": {
                "workbookName": workbook_path.name,
                "workbookPath": str(workbook_path),
                "workbookUpdatedAt": datetime.fromtimestamp(workbook_path.stat().st_mtime, SAST).isoformat(),
                "generatedAt": datetime.now(SAST).isoformat(),
            },
            "kpi": kpi,
            "scorecard": scorecard,
            "trCompliance": extract_monthly_percent_series(reader, "TR Compliance"),
            "scanning": extract_monthly_percent_series(reader, "Scanning"),
            "sheetsCompliance": extract_monthly_percent_series(reader, "Sheets Compliance"),
            "dispatchAccuracy": extract_monthly_percent_series(reader, "Dispatch Accuracy"),
            "truckJHB": truck_jhb,
            "truckGeorge": truck_george,
            "stockCount1st": stock_count_first,
            "stockCountFinal": stock_count_final,
            "localParts": extract_monthly_percent_series(reader, "Local Parts"),
            "liseoRTB": extract_monthly_percent_series(reader, "Liseo RTB"),
            "coverplates": extract_monthly_percent_series(reader, "Coverplates"),
            "liseoIssues": liseo_issues,
            "liseoIssuesScore": liseo_issue_score if liseo_issue_score is not None else 100,
            "tk2025": tk2025,
            "tk2026": tk2026,
            "odo2026": odo2026,
            "monthlySC": monthly_sc,
        }
        return payload
    finally:
        reader.close()
        snapshot_path.unlink(missing_ok=True)


class DashboardServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_class: type[SimpleHTTPRequestHandler], workdir: Path, assets_dir: Path, workbook_name: str | None, html_name: str) -> None:
        super().__init__(server_address, handler_class)
        self.workdir = workdir
        self.assets_dir = assets_dir
        self.workbook_name = workbook_name
        self.html_name = html_name
        self.cache_file = assets_dir / "dashboard_cache.json"
        self.cached_payload: dict[str, Any] | None = None
        if self.cache_file.exists():
            self.cached_payload = json.loads(self.cache_file.read_text(encoding="utf-8"))


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    server: DashboardServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/dashboard-data":
            self._serve_api()
            return
        if parsed.path == "/api/health":
            self._serve_health()
            return
        if parsed.path == "/":
            self.path = f"/{self.server.html_name}"
        super().do_GET()

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        super().end_headers()

    def translate_path(self, path: str) -> str:
        parsed = urlparse(path)
        target = parsed.path.lstrip("/") or self.server.html_name
        bases = [self.server.workdir.resolve(), self.server.assets_dir.resolve()]
        names = [Path(target), Path(target).name]
        for base in bases:
            for name in names:
                candidate = (base / name).resolve()
                try:
                    candidate.relative_to(base)
                except ValueError:
                    continue
                if candidate.exists():
                    return str(candidate)
        return str((self.server.workdir / target).resolve())

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    def _serve_api(self) -> None:
        try:
            payload = build_dashboard_payload(self.server.workdir, self.server.workbook_name)
            self.server.cached_payload = payload
            self.server.cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            self._write_json(200, payload)
        except Exception as exc:
            cached_payload = self.server.cached_payload
            if cached_payload is None and self.server.cache_file.exists():
                cached_payload = json.loads(self.server.cache_file.read_text(encoding="utf-8"))

            if cached_payload is not None:
                payload = json.loads(json.dumps(cached_payload))
                payload.setdefault("meta", {})
                payload["meta"]["generatedAt"] = datetime.now(SAST).isoformat()
                payload["meta"]["stale"] = True
                payload["meta"]["warning"] = str(exc)
                self._write_json(200, payload)
            else:
                self._write_json(500, {"error": str(exc)})

    def _serve_health(self) -> None:
        try:
            workbook_path = select_workbook(self.server.workdir, self.server.workbook_name)
            self._write_json(200, {"status": "ok", "workbook": workbook_path.name})
        except Exception as exc:
            self._write_json(500, {"status": "error", "error": str(exc)})

    def _write_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the Munya/Kristo dashboard with live workbook data.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8768)
    parser.add_argument("--workbook", help="Workbook filename in the current folder. Defaults to the newest .xlsm/.xlsx file.")
    parser.add_argument("--html", default="code/dashboard_live.html", help="Dashboard HTML filename to serve at '/'.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workdir = Path.cwd()
    assets_dir = Path(__file__).resolve().parent
    server = DashboardServer((args.host, args.port), DashboardRequestHandler, workdir, assets_dir, args.workbook, args.html)
    workbook_path = select_workbook(workdir, args.workbook)
    try:
        payload = build_dashboard_payload(workdir, args.workbook)
        server.cached_payload = payload
        server.cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        print("Startup cache ready.")
    except Exception as exc:
        print(f"Startup cache skipped: {exc}")
    print(f"Serving dashboard from {workdir}")
    print(f"Workbook source: {workbook_path.name}")
    print(f"Open http://{args.host}:{args.port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
