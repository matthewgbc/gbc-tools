#!/usr/bin/env python3
"""
GBC Partner Portal Data Updater
================================
Reads Square CSV exports or Square API and updates data/partners.json,
then injects the updated data back into partner-portal-v2.html.

Usage:
  python scripts/update_portal_data.py              # reads CSVs from INBOX_DIR
  python scripts/update_portal_data.py --api-only   # Square API only
  python scripts/update_portal_data.py --dry-run    # print changes, don't write

Square API activation: set SQUARE_ACCESS_TOKEN env var.
"""

import os
import sys
import json
import csv
import glob
import re
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Tuple

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_FILE = REPO_ROOT / "data" / "partners.json"
HTML_FILE = REPO_ROOT / "partner-portal-v2.html"
INBOX_DIR = Path(os.environ.get("INBOX_DIR", Path.home() / "Documents" / "GBC_OS" / "_INBOX"))

SQUARE_ACCESS_TOKEN = os.environ.get("SQUARE_ACCESS_TOKEN", "")
SQUARE_LOCATION = os.environ.get("SQUARE_LOCATION", "LRFDSMWDWDW98")
SQUARE_BASE_URL = "https://connect.squareup.com/v2"

# Partner email domain → partner ID mapping
EMAIL_DOMAIN_MAP = {
    "q39kc.com": "q39",
    "culinaryvirtue.com": "meatmitch",
    "meatmitch.com": "meatmitch",
    "boothcreekwagyu.com": "boothcreek",
    "eatpbj.com": "burntend",
    "dallasmavs.com": "dallasmavs",
}

# Customer name substring → partner ID mapping (fallback)
NAME_MAP = {
    "q39": "q39",
    "burnt end": "burntend",
    "pbj": "burntend",
    "meat mitch": "meatmitch",
    "booth creek": "boothcreek",
    "dallas mavericks": "dallasmavs",
    "claire gerhardt": "dallasmavs",
    "colin shipley": "gates",
    "gates bbq": "gates",
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def log(msg):
    print(f"  {msg}")


def warn(msg):
    print(f"  ⚠️  {msg}")


def match_partner_by_email(email: str) -> Optional[str]:
    """Return partner ID from an email address, or None."""
    if not email:
        return None
    domain = email.split("@")[-1].lower().strip()
    return EMAIL_DOMAIN_MAP.get(domain)


def match_partner_by_name(name: str) -> Optional[str]:
    """Return partner ID from a customer name string, or None."""
    if not name:
        return None
    lower = name.lower().strip()
    for fragment, pid in NAME_MAP.items():
        if fragment in lower:
            return pid
    return None


def find_partner(email: str, name: str) -> Optional[str]:
    return match_partner_by_email(email) or match_partner_by_name(name)


def parse_amount(val: str) -> Optional[float]:
    """Parse dollar strings like '$1,234.56' or '1234.56' into float."""
    if not val:
        return None
    cleaned = re.sub(r"[^\d\.]", "", str(val))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def amount_to_int_cents(amount: float) -> int:
    """Convert dollar float to integer cents for Square API."""
    return int(round(amount * 100))


def cents_to_dollars(cents: int) -> float:
    """Convert Square API integer cents to dollars."""
    return round(cents / 100, 2)


# ─────────────────────────────────────────────
# CSV PARSING
# ─────────────────────────────────────────────
def find_csv(pattern: str) -> list[Path]:
    """Find CSV files matching a glob pattern in INBOX_DIR."""
    matches = list(INBOX_DIR.glob(pattern))
    return sorted(matches)


def parse_invoices_csv(filepath: Path) -> List[dict]:
    """
    Parse Square invoices export CSV.
    Expected columns (flexible): Invoice ID, Customer Name, Customer Email,
    Amount, Status, Date / Created At / Invoice Date
    """
    invoices = []
    try:
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = [h.lower().strip() for h in (reader.fieldnames or [])]

            # Map flexible column names
            col_id = next((h for h in reader.fieldnames or [] if "invoice" in h.lower() and "id" in h.lower()), None)
            col_name = next((h for h in reader.fieldnames or [] if "customer" in h.lower() and "name" in h.lower()), None)
            col_email = next((h for h in reader.fieldnames or [] if "email" in h.lower()), None)
            col_amount = next((h for h in reader.fieldnames or [] if "amount" in h.lower() or "total" in h.lower()), None)
            col_status = next((h for h in reader.fieldnames or [] if "status" in h.lower()), None)
            col_date = next((h for h in reader.fieldnames or [] if "date" in h.lower() or "created" in h.lower()), None)

            for row in reader:
                inv = {
                    "id": row.get(col_id, "").strip() if col_id else "",
                    "customer_name": row.get(col_name, "").strip() if col_name else "",
                    "email": row.get(col_email, "").strip() if col_email else "",
                    "amount": parse_amount(row.get(col_amount, "")) if col_amount else None,
                    "status": row.get(col_status, "").strip().lower() if col_status else "",
                    "date": row.get(col_date, "").strip() if col_date else "",
                }
                # Normalize status
                raw_status = inv["status"]
                if "paid" in raw_status or "complete" in raw_status:
                    inv["status_normalized"] = "paid"
                elif "overdue" in raw_status or "past" in raw_status:
                    inv["status_normalized"] = "overdue"
                elif "draft" in raw_status or "unpaid" in raw_status:
                    inv["status_normalized"] = "draft"
                else:
                    inv["status_normalized"] = raw_status or "unknown"

                invoices.append(inv)
    except FileNotFoundError:
        warn(f"Invoice CSV not found: {filepath}")
    except Exception as e:
        warn(f"Error reading {filepath.name}: {e}")

    return invoices


def parse_customers_csv(filepath: Path) -> List[dict]:
    """Parse Square customer download CSV."""
    customers = []
    try:
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            col_id = next((h for h in reader.fieldnames or [] if "id" in h.lower() and "customer" in h.lower()), None)
            col_first = next((h for h in reader.fieldnames or [] if "first" in h.lower()), None)
            col_last = next((h for h in reader.fieldnames or [] if "last" in h.lower()), None)
            col_email = next((h for h in reader.fieldnames or [] if "email" in h.lower()), None)
            col_company = next((h for h in reader.fieldnames or [] if "company" in h.lower() or "business" in h.lower()), None)

            for row in reader:
                first = row.get(col_first, "").strip() if col_first else ""
                last = row.get(col_last, "").strip() if col_last else ""
                company = row.get(col_company, "").strip() if col_company else ""
                customers.append({
                    "id": row.get(col_id, "").strip() if col_id else "",
                    "name": f"{first} {last}".strip() or company,
                    "email": row.get(col_email, "").strip() if col_email else "",
                    "company": company,
                })
    except FileNotFoundError:
        warn(f"Customer CSV not found: {filepath}")
    except Exception as e:
        warn(f"Error reading {filepath.name}: {e}")
    return customers


def parse_items_csv(filepath: Path) -> List[dict]:
    """Parse Square items/transactions CSV for line-item detail."""
    items = []
    try:
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            col_name = next((h for h in reader.fieldnames or [] if "customer" in h.lower() and "name" in h.lower()), None)
            col_email = next((h for h in reader.fieldnames or [] if "email" in h.lower()), None)
            col_item = next((h for h in reader.fieldnames or [] if "item" in h.lower() or "description" in h.lower()), None)
            col_amount = next((h for h in reader.fieldnames or [] if "amount" in h.lower() or "gross" in h.lower() or "net" in h.lower()), None)
            col_date = next((h for h in reader.fieldnames or [] if "date" in h.lower()), None)

            for row in reader:
                items.append({
                    "customer_name": row.get(col_name, "").strip() if col_name else "",
                    "email": row.get(col_email, "").strip() if col_email else "",
                    "item": row.get(col_item, "").strip() if col_item else "",
                    "amount": parse_amount(row.get(col_amount, "")) if col_amount else None,
                    "date": row.get(col_date, "").strip() if col_date else "",
                })
    except FileNotFoundError:
        warn(f"Items CSV not found: {filepath}")
    except Exception as e:
        warn(f"Error reading {filepath.name}: {e}")
    return items


# ─────────────────────────────────────────────
# SQUARE API
# ─────────────────────────────────────────────
def square_get(endpoint: str) -> dict:
    """Make authenticated Square API GET request."""
    if not HAS_REQUESTS:
        raise RuntimeError("requests library not installed. Run: pip install requests")
    headers = {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Square-Version": "2024-01-17",
    }
    url = f"{SQUARE_BASE_URL}{endpoint}"
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 401:
        raise RuntimeError("Square API: Unauthorized. Check SQUARE_ACCESS_TOKEN.")
    if resp.status_code != 200:
        raise RuntimeError(f"Square API error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def square_post(endpoint: str, payload: dict) -> dict:
    """Make authenticated Square API POST request (for paginated list calls)."""
    if not HAS_REQUESTS:
        raise RuntimeError("requests library not installed.")
    headers = {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Square-Version": "2024-01-17",
    }
    url = f"{SQUARE_BASE_URL}{endpoint}"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Square API error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def fetch_square_invoices() -> List[dict]:
    """Fetch all invoices from Square API (handles pagination)."""
    invoices = []
    cursor = None
    while True:
        params = {"location_id": SQUARE_LOCATION, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        query = "&".join(f"{k}={v}" for k, v in params.items())
        data = square_get(f"/invoices?{query}")
        batch = data.get("invoices", [])
        invoices.extend(batch)
        cursor = data.get("cursor")
        if not cursor:
            break

    normalized = []
    for inv in invoices:
        amount_money = inv.get("payment_requests", [{}])[0].get("total_completed_amount_money", {}) if inv.get("payment_requests") else {}
        amount = cents_to_dollars(amount_money.get("amount", 0)) if amount_money else None
        status_raw = inv.get("status", "").lower()
        if "paid" in status_raw or "complete" in status_raw:
            status = "paid"
        elif "overdue" in status_raw or "unpaid" in status_raw:
            status = "overdue"
        else:
            status = status_raw

        # Try to get customer email from primary_recipient
        recipient = inv.get("primary_recipient", {})
        email = recipient.get("email_address", "")
        name = recipient.get("given_name", "") + " " + recipient.get("family_name", "")
        name = name.strip() or recipient.get("company_name", "")

        normalized.append({
            "id": inv.get("invoice_number") or inv.get("id", ""),
            "square_id": inv.get("id", ""),
            "customer_name": name,
            "email": email,
            "amount": amount,
            "status": status,
            "status_normalized": status,
            "date": inv.get("created_at", "")[:10] if inv.get("created_at") else "",
            "items": inv.get("title", "") or inv.get("description", ""),
        })
    return normalized


def fetch_square_customers() -> List[dict]:
    """Fetch all customers from Square API."""
    customers = []
    cursor = None
    while True:
        payload = {"limit": 100}
        if cursor:
            payload["cursor"] = cursor
        data = square_post("/customers/search", payload)
        batch = data.get("customers", [])
        customers.extend(batch)
        cursor = data.get("cursor")
        if not cursor:
            break

    return [
        {
            "id": c.get("id", ""),
            "name": f"{c.get('given_name','')} {c.get('family_name','')}".strip() or c.get("company_name", ""),
            "email": c.get("email_address", ""),
            "company": c.get("company_name", ""),
        }
        for c in customers
    ]


# ─────────────────────────────────────────────
# MATCH + MERGE
# ─────────────────────────────────────────────
def build_partner_invoice_map(invoices: List[dict]) -> Dict[str, List[dict]]:
    """Map partner IDs to their matched invoices."""
    mapping: Dict[str, List[dict]] = {}
    unmatched = 0
    for inv in invoices:
        pid = find_partner(inv.get("email", ""), inv.get("customer_name", ""))
        if pid:
            mapping.setdefault(pid, []).append(inv)
        else:
            unmatched += 1
    if unmatched:
        log(f"  {unmatched} invoices could not be matched to a partner (no email/name match)")
    return mapping


def update_partner_from_invoices(partner: dict, matched_invoices: List[dict]) -> Tuple[dict, bool]:
    """
    Update a partner's financial fields from matched invoices.
    Returns (updated_partner, was_changed).
    """
    if not matched_invoices:
        return partner, False

    changed = False

    # Build set of existing invoice IDs to avoid duplicates
    existing_ids = {inv["id"] for inv in partner.get("invoices", [])}

    new_invoices = []
    for inv in matched_invoices:
        inv_id = inv.get("id", "") or inv.get("square_id", "")
        if inv_id and inv_id not in existing_ids:
            new_invoices.append({
                "id": inv_id,
                "date": inv.get("date", ""),
                "amount": inv.get("amount"),
                "status": inv.get("status_normalized", "unknown"),
                "items": inv.get("items", "Square invoice — auto-imported"),
            })
            changed = True

    if new_invoices:
        partner["invoices"] = partner.get("invoices", []) + new_invoices

    # Recompute outstanding balance from overdue invoices
    overdue_total = sum(
        inv["amount"] for inv in partner.get("invoices", [])
        if inv.get("status") == "overdue" and inv.get("amount") is not None
    )
    if partner.get("outstandingBalance") != overdue_total and new_invoices:
        partner["outstandingBalance"] = round(overdue_total, 2)
        changed = True

    # Update lastOrder from most recent paid invoice
    paid_dates = [
        inv["date"] for inv in partner.get("invoices", [])
        if inv.get("status") == "paid" and inv.get("date")
    ]
    if paid_dates and new_invoices:
        # Try to find the most recent by sorting date strings
        try:
            most_recent = sorted(paid_dates)[-1]
            partner["lastOrder"] = most_recent
            changed = True
        except Exception:
            pass

    # Recompute lifetime revenue
    paid_total = sum(
        inv["amount"] for inv in partner.get("invoices", [])
        if inv.get("status") == "paid" and inv.get("amount") is not None
    )
    if new_invoices:
        partner["lifetimeRevenue"] = round(paid_total, 2)
        changed = True

    return partner, changed


# ─────────────────────────────────────────────
# HTML INJECTION
# ─────────────────────────────────────────────
BEGIN_MARKER = "// BEGIN:PARTNERS_DATA"
END_MARKER = "// END:PARTNERS_DATA"


def generate_js_block(data: dict) -> str:
    """Generate the JavaScript data block from partners.json."""
    partners = data["partners"]
    episodes = data["episodes"]
    vendors = data["vendors"]
    lifecycle = data["lifecycle"]

    # Serialize each section as JavaScript
    partners_js = json.dumps(partners, indent=2, ensure_ascii=False)
    episodes_js = json.dumps(episodes, indent=2, ensure_ascii=False)
    vendors_js = json.dumps(vendors, indent=2, ensure_ascii=False)
    lifecycle_js = json.dumps(lifecycle, indent=2, ensure_ascii=False)

    return f"""{BEGIN_MARKER}
const PARTNERS = {partners_js};

const VENDORS = {vendors_js};

const EPISODES = {episodes_js};

const LIFECYCLE = {lifecycle_js};
{END_MARKER}"""


def inject_into_html(html_path: Path, data: dict, dry_run: bool = False) -> bool:
    """
    Replace the data block between BEGIN/END markers in the HTML file.
    Returns True if the file was (or would be) changed.
    """
    try:
        content = html_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        warn(f"HTML file not found: {html_path}")
        return False

    begin_idx = content.find(BEGIN_MARKER)
    end_idx = content.find(END_MARKER)

    if begin_idx == -1 or end_idx == -1:
        warn("BEGIN:PARTNERS_DATA / END:PARTNERS_DATA markers not found in HTML.")
        warn("Cannot inject data. Add markers manually first.")
        return False

    # Extract current block
    current_block = content[begin_idx:end_idx + len(END_MARKER)]
    new_block = generate_js_block(data)

    if current_block == new_block:
        log("HTML data block is already up to date — no changes needed.")
        return False

    if dry_run:
        log(f"[DRY RUN] Would update JS data block in {html_path.name}")
        return True

    new_content = content[:begin_idx] + new_block + content[end_idx + len(END_MARKER):]
    html_path.write_text(new_content, encoding="utf-8")
    log(f"Updated JS data block in {html_path.name}")
    return True


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GBC Partner Portal Data Updater")
    parser.add_argument("--api-only", action="store_true", help="Use Square API instead of CSVs")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing files")
    args = parser.parse_args()

    print("\n── GBC Portal Data Updater ──────────────────────────")

    # Load current data
    if not DATA_FILE.exists():
        print(f"ERROR: {DATA_FILE} not found. Run from repo root.")
        sys.exit(1)

    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    partners = data["partners"]
    all_invoices: List[dict] = []

    # ── Phase: Fetch invoice data ─────────────────────────
    use_api = args.api_only or bool(SQUARE_ACCESS_TOKEN)

    if use_api:
        if not SQUARE_ACCESS_TOKEN:
            print("ERROR: --api-only requires SQUARE_ACCESS_TOKEN env var.")
            sys.exit(1)
        if not HAS_REQUESTS:
            print("ERROR: requests library not installed. Run: pip install requests")
            sys.exit(1)
        print(f"\n[Square API] Fetching invoices...")
        try:
            all_invoices = fetch_square_invoices()
            print(f"  Fetched {len(all_invoices)} invoices from Square API")
        except Exception as e:
            print(f"  ERROR fetching from Square API: {e}")
            sys.exit(1)
    else:
        print(f"\n[CSV Mode] Looking in: {INBOX_DIR}")

        # Invoices
        invoice_files = (
            find_csv("invoices-export-*.csv") +
            find_csv("invoices_export_*.csv") +
            find_csv("invoices*.csv")
        )
        if invoice_files:
            for f in invoice_files:
                log(f"Reading {f.name}")
                batch = parse_invoices_csv(f)
                all_invoices.extend(batch)
                log(f"  → {len(batch)} invoices")
        else:
            warn("No invoice CSV files found matching 'invoices-export-*.csv'")

        # Items CSV (for additional line-item context)
        item_files = (
            find_csv("items-*.csv") +
            find_csv("items_*.csv")
        )
        for f in item_files:
            log(f"Reading items: {f.name}")
            items = parse_items_csv(f)
            # Items become supplementary invoices with lower priority
            for item in items:
                if item.get("amount") and item.get("date"):
                    all_invoices.append({
                        "id": f"ITEM-{hash(item['date']+item['item']) & 0xFFFF:04X}",
                        "customer_name": item["customer_name"],
                        "email": item["email"],
                        "amount": item["amount"],
                        "status": "paid",
                        "status_normalized": "paid",
                        "date": item["date"],
                        "items": item["item"],
                    })

        print(f"  Total invoices loaded: {len(all_invoices)}")

    # ── Match invoices to partners ────────────────────────
    print("\n[Matching] Mapping invoices to partners...")
    partner_invoice_map = build_partner_invoice_map(all_invoices)
    for pid, invs in partner_invoice_map.items():
        log(f"{pid}: {len(invs)} invoices matched")

    # ── Update partner records ────────────────────────────
    print("\n[Update] Merging invoice data into partners...")
    partners_updated = 0
    invoices_added = 0

    for pid, partner in partners.items():
        matched = partner_invoice_map.get(pid, [])
        pre_count = len(partner.get("invoices", []))
        updated_partner, changed = update_partner_from_invoices(partner, matched)
        if changed:
            data["partners"][pid] = updated_partner
            added = len(updated_partner.get("invoices", [])) - pre_count
            invoices_added += added
            partners_updated += 1
            log(f"  {pid}: +{added} invoice(s), balance={updated_partner.get('outstandingBalance')}")

    # ── Update meta ───────────────────────────────────────
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data["meta"]["lastUpdated"] = now_str
    if use_api or all_invoices:
        data["meta"]["lastSquareSync"] = now_str

    # Recompute total lifetime revenue
    data["meta"]["totalLifetimeRevenue"] = round(
        sum(p.get("lifetimeRevenue", 0) for p in data["partners"].values()), 2
    )

    # ── Write JSON ────────────────────────────────────────
    if args.dry_run:
        print(f"\n[DRY RUN] Would write updated data/partners.json")
    else:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"Wrote {DATA_FILE.name}")

    # ── Inject into HTML ──────────────────────────────────
    print("\n[HTML] Injecting updated data into partner-portal-v2.html...")
    html_changed = inject_into_html(HTML_FILE, data, dry_run=args.dry_run)

    # ── Summary ───────────────────────────────────────────
    total_outstanding = sum(
        p.get("outstandingBalance", 0) for p in data["partners"].values()
    )
    print("\n── Summary ─────────────────────────────────────────")
    print(f"  Partners updated:  {partners_updated}")
    print(f"  Invoices added:    {invoices_added}")
    print(f"  Total invoices:    {len(all_invoices)}")
    print(f"  Total outstanding: ${total_outstanding:,.2f}")
    print(f"  JSON updated:      {not args.dry_run}")
    print(f"  HTML updated:      {html_changed and not args.dry_run}")
    print(f"  Sync date:         {now_str}")
    if args.dry_run:
        print("\n  [DRY RUN — no files were written]")
    print("─────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
