#!/bin/bash
set -euo pipefail

# === Usage ==============================================================
# bash avgTimeInMeetings.sh [START_DATE] [END_DATE]
# Dates optional; format YYYY-MM-DD. If omitted, defaults to last 56 days.
# -----------------------------------------------------------------------

# --- dates ---
if [[ "${1-}" == "" || "${2-}" == "" ]]; then
  END_DATE=$(date +"%Y-%m-%d")
  # macOS: date -v-56d ; Linux: date -d "-56 days"
  START_DATE=$(date -v-56d +"%Y-%m-%d" 2>/dev/null || date -d "-56 days" +"%Y-%m-%d")
else
  START_DATE="$1"
  END_DATE="$2"
fi

# Build RFC3339 offset like -04:00
RAW_OFF="$(date +%z)"; TZ_HOUR="${RAW_OFF:0:3}"; TZ_MIN="${RAW_OFF:3:2}"
TZ_OFFSET="${TZ_HOUR}:${TZ_MIN}"
START_TS="${START_DATE}T00:00:00${TZ_OFFSET}"
END_TS="${END_DATE}T23:59:59${TZ_OFFSET}"

# --- files ---
RAW_CSV="calendar_events_raw.csv"
CLEAN_CSV="meetings_clean.csv"
WEEKLY_USER_CSV="weekly_summary_by_user.csv"
WEEKLY_ORG_CSV="weekly_org_average.csv"
SCOPE_FILE="scope_users.txt"   # OPTIONAL: one email per line to limit scope

# --- locate gam ---
: "${GAM_BIN:=gam}"  # override with: GAM_BIN=/full/path/to/gam bash avgTimeInMeetings.sh ...
if ! command -v "$GAM_BIN" >/dev/null 2>&1; then
  echo "ERROR: 'gam' not found. Set GAM_BIN or add to PATH."
  exit 1
fi

echo ">>> Exporting events with GAM from ${START_TS} to ${END_TS}"

# --- choose scope: all users vs list in scope_users.txt ---
TARGET_ARGS=()
if [[ -s "$SCOPE_FILE" ]]; then
  echo "Using scope file: $SCOPE_FILE"
  TARGET_ARGS=(multiprocess users file "$SCOPE_FILE")
else
  echo "No scope_users.txt found; using ALL USERS (may include suspended/archived)."
  TARGET_ARGS=(multiprocess all users)
fi

# --- export ---
# Notes:
# - 'print events primary' is the correct grammar.
# - eventtypes default,fromGmail excludes focus time / OOO / working location.
# - matchfield transparency ^opaque$ keeps only busy events.
# - singleevents expands recurring instances.
set -x
"$GAM_BIN" redirect csv "./${RAW_CSV}" \
  "${TARGET_ARGS[@]}" \
  print events primary singleevents \
  starttime "${START_TS}" endtime "${END_TS}" \
  eventtypes default,fromGmail \
  matchfield transparency ^opaque$ \
  fields "id,summary,description,location,starttime,endtime,transparency,eventType,attendees.email,attendees.responseStatus,attendees.self,attendees.organizer,attendees.resource"
set +x

# --- quick check for empty result (only headers or zero bytes) ---
if [[ ! -s "$RAW_CSV" ]] || [[ $(wc -l < "$RAW_CSV") -le 1 ]]; then
  echo "WARNING: ${RAW_CSV} appears empty (no matching events)."
  echo "Troubleshooting tips:"
  echo "  1) Try widening the date range."
  echo "  2) Try removing filters: drop 'eventtypes …' and 'matchfield transparency …' to confirm visibility."
  echo "  3) Sanity check one user you KNOW has meetings:"
  echo "       $GAM_BIN user YOUR_EMAIL print events primary starttime ${START_TS} endtime ${END_TS} fields \"id,summary,starttime,endtime,transparency,eventType\""
  exit 0
fi

# --- post-process in Python ---
/usr/bin/env python3 << 'PYCODE'
import csv
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dateutil import parser as dtp

RAW_CSV = "calendar_events_raw.csv"
CLEAN_CSV = "meetings_clean.csv"
WEEKLY_USER_CSV = "weekly_summary_by_user.csv"
WEEKLY_ORG_CSV = "weekly_org_average.csv"

def merge_intervals(intervals):
    if not intervals: return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = [intervals[0]]
    for s,e in intervals[1:]:
        ls, le = merged[-1]
        if s <= le:
            merged[-1] = (ls, max(le, e))
        else:
            merged.append((s,e))
    return merged

# Load
with open(RAW_CSV, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Figure out which column holds the user (GAM includes one when iterating users)
user_col = None
for cand in ("User", "user", "primaryEmail", "calendarId", "Owner"):
    if rows and cand in rows[0]:
        user_col = cand
        break
if user_col is None:
    # Best-effort: add a placeholder so downstream doesn't crash
    user_col = "User"
    for r in rows:
        r[user_col] = ""

# Normalize missing columns
for r in rows:
    for col in ("id","starttime","endtime","transparency","eventType","eventtype",
                "attendees.email","attendees.responseStatus","attendees.self","attendees.organizer","attendees.resource","summary","description","location"):
        r.setdefault(col, "")

# Group rows by (user, event id)
by_event = defaultdict(list)
for r in rows:
    # Use eventType if present, else eventtype (GAM casing varies)
    et = r.get("eventType") or r.get("eventtype") or ""
    if r.get("transparency","").lower() != "opaque":
        continue
    if et and et.lower() not in ("default","fromgmail"):
        continue
    by_event[(r[user_col], r["id"])].append(r)

kept = []
for (user, eid), grp in by_event.items():
    s_raw = grp[0].get("starttime","")
    e_raw = grp[0].get("endtime","")
    try:
        s = dtp.parse(s_raw); e = dtp.parse(e_raw)
    except Exception:
        continue
    if e <= s: continue

    owner_accepted = False
    has_other_human = False
    for g in grp:
        # self accepted?
        if g.get("attendees.self","").lower()=="true" and g.get("attendees.responseStatus","").lower()=="accepted":
            owner_accepted = True
        # someone else, not a room
        if g.get("attendees.self","").lower()!="true" and g.get("attendees.resource","").lower()!="true":
            if g.get("attendees.email","").strip():
                has_other_human = True

    if owner_accepted and has_other_human:
        kept.append({
            "User": user,
            "id": eid,
            "summary": grp[0].get("summary",""),
            "start": s,
            "end": e
        })

# De-overlap per user per day
by_user_day = defaultdict(lambda: defaultdict(list))
for ev in kept:
    user = ev["User"]
    s, e = ev["start"], ev["end"]
    # split by day
    cur = s
    while cur.date() < e.date():
        end_of_day = datetime(cur.year, cur.month, cur.day, 23, 59, 59, tzinfo=cur.tzinfo)
        by_user_day[user][cur.date()].append((cur, end_of_day))
        cur = end_of_day + timedelta(seconds=1)
    by_user_day[user][e.date()].append((max(s, datetime(e.year,e.month,e.day,0,0,0,tzinfo=e.tzinfo)), e))

# Totals per ISO week
user_week_minutes = defaultdict(lambda: defaultdict(float))
for user, daymap in by_user_day.items():
    for d, intervals in daymap.items():
        merged = merge_intervals(intervals)
        mins = sum((e - s).total_seconds()/60.0 for s,e in merged)
        y,wk,_ = d.isocalendar()
        user_week_minutes[user][(y,wk)] += mins

# Write event-level file
with open(CLEAN_CSV, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f); w.writerow(["User","EventID","Summary","Start","End","DurationMinutes"])
    for ev in kept:
        mins = (ev["end"] - ev["start"]).total_seconds()/60.0
        w.writerow([ev["User"], ev["id"], ev["summary"], ev["start"].isoformat(), ev["end"].isoformat(), f"{mins:.2f}"])

# User-week file
with open(WEEKLY_USER_CSV, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f); w.writerow(["User","ISOYear","ISOWeek","Minutes","Hours"])
    for user, wkmap in sorted(user_week_minutes.items()):
        for (y,wk), mins in sorted(wkmap.items()):
            w.writerow([user, y, wk, f"{mins:.2f}", f"{mins/60.0:.2f}"])

# Org average across users with >0 minutes that week
week_to_vals = defaultdict(list)
for user, wkmap in user_week_minutes.items():
    for wk, mins in wkmap.items():
        if mins > 0: week_to_vals[wk].append(mins)

with open(WEEKLY_ORG_CSV, "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f); w.writerow(["ISOYear","ISOWeek","AvgMinutesPerPerson","AvgHoursPerPerson","ActiveUsers"])
    for (y,wk), arr in sorted(week_to_vals.items()):
        avg_m = sum(arr)/len(arr) if arr else 0.0
        w.writerow([y, wk, f"{avg_m:.2f}", f"{avg_m/60.0:.2f}", len(arr)])

print(f">>> Wrote {CLEAN_CSV}, {WEEKLY_USER_CSV}, {WEEKLY_ORG_CSV}")
PYCODE

echo "Done."
echo "Outputs:"
echo "  - ${RAW_CSV}"
echo "  - ${CLEAN_CSV}"
echo "  - ${WEEKLY_USER_CSV}"
echo "  - ${WEEKLY_ORG_CSV}"
