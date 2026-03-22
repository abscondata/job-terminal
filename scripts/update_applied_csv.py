"""Update applied_jobs_cleaned.csv with dates and new rows."""
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path

CSV_PATH = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"
TODAY = datetime(2026, 3, 22)


def parse_date(status_raw):
    s = (status_raw or "").strip()
    m = re.search(r"(\d+)\s*(h|d|w|mo)\s*ago", s, re.I)
    if not m:
        return TODAY.strftime("%Y-%m-%d")
    val, unit = int(m.group(1)), m.group(2).lower()
    if unit == "h":
        return TODAY.strftime("%Y-%m-%d") if val < 24 else (TODAY - timedelta(days=1)).strftime("%Y-%m-%d")
    if unit == "d":
        return (TODAY - timedelta(days=val)).strftime("%Y-%m-%d")
    if unit == "w":
        return (TODAY - timedelta(weeks=val)).strftime("%Y-%m-%d")
    if unit == "mo":
        return (TODAY - timedelta(days=val * 30)).strftime("%Y-%m-%d")
    return TODAY.strftime("%Y-%m-%d")


with open(CSV_PATH, "r", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    existing_fields = list(reader.fieldnames)
    rows = list(reader)

print(f"Existing rows: {len(rows)}")

for r in rows:
    r["applied_date"] = parse_date(r.get("status_raw", ""))
    r["status"] = "applied"

new_rows = [
    {"company_display":"Ascendo Resources","title_raw":"Compliance Associate , Verified","company_raw":"Ascendo Resources","location_raw":"Miami, FL (Hybrid)","status_raw":"Application viewed 4h ago","kind":"applied","title":"Compliance Associate","company":"Ascendo Resources","location":"Miami, FL (Hybrid)","city":"Miami/South FL","mode":"Hybrid","world":"Compliance/Risk","function":"Compliance/Risk","applied_date":"2026-03-22","status":"applied"},
    {"company_display":"Voluntae","title_raw":"Consultant Contr\u00f4le interne & Data","company_raw":"Voluntae","location_raw":"Paris (On-site)","status_raw":"Applied 4h ago","kind":"applied","title":"Consultant Contr\u00f4le interne & Data","company":"Voluntae","location":"Paris (On-site)","city":"Paris","mode":"On-site","world":"Other","function":"Other","applied_date":"2026-03-22","status":"applied"},
    {"company_display":"Coda Search\u2502Staffing","title_raw":"Investor Relations Operations Associate , Verified","company_raw":"Coda Search\u2502Staffing","location_raw":"New York, NY (On-site)","status_raw":"Applied 22h ago","kind":"applied","title":"Investor Relations Operations Associate","company":"Coda Search\u2502Staffing","location":"New York, NY (On-site)","city":"NYC","mode":"On-site","world":"Operations","function":"Operations","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"LevelUP HCS","title_raw":"Entry Level Finance Role , Verified","company_raw":"LevelUP HCS","location_raw":"New York, NY (On-site)","status_raw":"Applied 22h ago","kind":"applied","title":"Entry Level Finance Role","company":"LevelUP HCS","location":"New York, NY (On-site)","city":"NYC","mode":"On-site","world":"Other","function":"Other","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"Social Capital Resources","title_raw":"KYC Analyst","company_raw":"Social Capital Resources","location_raw":"New York, NY (On-site)","status_raw":"Applied 22h ago","kind":"applied","title":"KYC Analyst","company":"Social Capital Resources","location":"New York, NY (On-site)","city":"NYC","mode":"On-site","world":"Compliance/Risk","function":"Compliance/Risk","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"Birnam Oak Advisors, LP","title_raw":"Operations Associate","company_raw":"Birnam Oak Advisors, LP","location_raw":"New York, NY (On-site)","status_raw":"Applied 22h ago","kind":"applied","title":"Operations Associate","company":"Birnam Oak Advisors, LP","location":"New York, NY (On-site)","city":"NYC","mode":"On-site","world":"Operations","function":"Operations","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"CHANEL","title_raw":"Stage \u2013 Assistant(e) outils de communication Presse (H/F/X) , Verified","company_raw":"CHANEL","location_raw":"Paris (On-site)","status_raw":"Applied on Company Website 22h ago","kind":"applied","title":"Stage \u2013 Assistant(e) outils de communication Presse (H/F/X)","company":"CHANEL","location":"Paris (On-site)","city":"Paris","mode":"On-site","world":"Luxury/Fashion/Beauty","function":"Content/Brand/Creative","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"Herm\u00e8s","title_raw":"ALTERNANCE - Fondation d'Entreprise Herm\u00e8s - Assistant(e) Arts Visuels et Artisanat H/F , Verified","company_raw":"Herm\u00e8s","location_raw":"Paris","status_raw":"Applied on Company Website 22h ago","kind":"applied","title":"ALTERNANCE - Fondation d'Entreprise Herm\u00e8s - Assistant(e) Arts Visuels et Artisanat H/F","company":"Herm\u00e8s","location":"Paris","city":"Paris","mode":"Unknown","world":"Luxury/Fashion/Beauty","function":"Other","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"Christian Dior Couture","title_raw":"Assistant(e) Communication et Coordination Internationale VM F/H - Alternance , Verified","company_raw":"Christian Dior Couture","location_raw":"Paris (On-site)","status_raw":"Applied on Company Website 22h ago","kind":"applied","title":"Assistant(e) Communication et Coordination Internationale VM F/H - Alternance","company":"Christian Dior Couture","location":"Paris (On-site)","city":"Paris","mode":"On-site","world":"Luxury/Fashion/Beauty","function":"Project/Coordination","applied_date":"2026-03-21","status":"withdrawn"},
    {"company_display":"Van Cleef & Arpels","title_raw":"Alternance - Assistant(e) chef de projet media et budget de communication (H/F) , Verified","company_raw":"Van Cleef & Arpels","location_raw":"Paris (On-site)","status_raw":"Applied on Company Website 22h ago","kind":"applied","title":"Alternance - Assistant(e) chef de projet media et budget de communication (H/F)","company":"Van Cleef & Arpels","location":"Paris (On-site)","city":"Paris","mode":"On-site","world":"Luxury/Fashion/Beauty","function":"Project/Coordination","applied_date":"2026-03-21","status":"applied"},
    {"company_display":"JW Michaels & Co.","title_raw":"Compliance Analyst , Verified","company_raw":"JW Michaels & Co.","location_raw":"New York City Metropolitan Area (Hybrid)","status_raw":"Applied 1d ago","kind":"applied","title":"Compliance Analyst","company":"JW Michaels & Co.","location":"New York City Metropolitan Area (Hybrid)","city":"NYC","mode":"Hybrid","world":"Compliance/Risk","function":"Compliance/Risk","applied_date":"2026-03-21","status":"applied"},
]

all_rows = rows + new_rows
fields = existing_fields + ["applied_date", "status"]

with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for r in all_rows:
        writer.writerow({k: r.get(k, "") for k in fields})

print(f"Updated CSV: {len(all_rows)} rows ({len(rows)} existing + {len(new_rows)} new)")
for r in all_rows[-3:]:
    print(f"  {r.get('company','')} | {r.get('title','')[:40]} | {r.get('applied_date','')} | {r.get('status','')}")
