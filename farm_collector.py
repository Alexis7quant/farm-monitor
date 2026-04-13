import requests
import json
import os
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
# Reads from environment variables (set as GitHub Secrets in Actions)
GIST_ID   = os.environ.get("GIST_ID", "4f1267eb3b0735b455c4ae81ccd3429c")
GH_TOKEN  = os.environ.get("GH_TOKEN", "")
GIST_FILE = "farm_data.json"

VAR_URL   = "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
EXT_URL   = "https://api.starknet.extended.exchange/api/v1/info/markets"

HEADERS_GH = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# ── Epoch helpers ─────────────────────────────────────────────────────────────
def get_epoch_info(now_utc, start_weekday):
    days_since_start = (now_utc.weekday() - start_weekday) % 7
    epoch_start = (now_utc - timedelta(days=days_since_start)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    epoch_end = epoch_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    days_remaining = max(0, (epoch_end.date() - now_utc.date()).days)
    fmt = "%a %d %b"
    label = f"{epoch_start.strftime(fmt)} -> {epoch_end.strftime(fmt)}"
    epoch_id = epoch_start.strftime("%Y-%m-%d")
    return epoch_start, epoch_end, days_remaining, label, epoch_id

def get_var_epoch(now_utc):
    return get_epoch_info(now_utc, start_weekday=3)  # Thursday

def get_ext_epoch(now_utc):
    return get_epoch_info(now_utc, start_weekday=1)  # Tuesday

# ── Gist helpers ──────────────────────────────────────────────────────────────
def load_gist():
    url = f"https://api.github.com/gists/{GIST_ID}"
    r = requests.get(url, headers=HEADERS_GH, timeout=10)
    r.raise_for_status()
    content = r.json()["files"][GIST_FILE]["content"]
    try:
        data = json.loads(content)
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}
    for key in ("var_accumulator", "ext_accumulator", "history"):
        if key not in data:
            data[key] = {} if key != "history" else []
    return data

def save_gist(data):
    url = f"https://api.github.com/gists/{GIST_ID}"
    payload = {"files": {GIST_FILE: {"content": json.dumps(data, indent=2)}}}
    r = requests.patch(url, headers=HEADERS_GH, json=payload, timeout=10)
    r.raise_for_status()

# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_variational():
    r = requests.get(VAR_URL, timeout=10)
    r.raise_for_status()
    d = r.json()
    vol = float(d.get("total_volume_24h", 0))
    oi  = float(d.get("open_interest", 0))
    print(f"  Variational -> 24h vol: ${vol/1e6:.2f}M  OI: ${oi/1e6:.2f}M")
    return vol, oi

def fetch_extended():
    r = requests.get(EXT_URL, timeout=10)
    r.raise_for_status()
    d = r.json()
    markets = d.get("data") or []
    if not isinstance(markets, list):
        markets = []
    total_vol = 0.0
    total_oi  = 0.0
    for m in markets:
        stats = m.get("marketStats") or {}
        if m.get("status") not in ("ACTIVE", "REDUCE_ONLY"):
            continue
        total_vol += float(stats.get("dailyVolume") or 0)
        total_oi  += float(stats.get("openInterest") or 0)
    print(f"  Extended    -> 24h vol: ${total_vol/1e6:.2f}M  OI: ${total_oi/1e6:.2f}M")
    return total_vol, total_oi

# ── Accumulate epoch data ─────────────────────────────────────────────────────
def accumulate(acc, epoch_id, epoch_label, days_remaining, today_str, vol, oi):
    if epoch_id not in acc:
        acc[epoch_id] = {"epoch_label": epoch_label, "days": {}, "latest_oi": None}
    acc[epoch_id]["epoch_label"] = epoch_label
    if vol is not None:
        acc[epoch_id]["days"][today_str] = {"vol": vol}
        acc[epoch_id]["latest_oi"] = oi
    days_data      = acc[epoch_id]["days"]
    weekly_vol     = sum(d["vol"] for d in days_data.values() if d.get("vol"))
    days_collected = len(days_data)
    latest_oi      = acc[epoch_id].get("latest_oi")
    return {
        "epoch_id":       epoch_id,
        "epoch_label":    epoch_label,
        "days_remaining": days_remaining,
        "days_collected": days_collected,
        "weekly_vol":     weekly_vol or None,
        "latest_oi":      latest_oi,
        "daily_log":      dict(sorted(days_data.items(), reverse=True)),
    }

def update_history(history, platform, epoch_id, epoch_label, weekly_vol, latest_oi, days_collected):
    history[:] = [h for h in history if not (h.get("platform") == platform and h.get("epoch_id") == epoch_id)]
    history.append({
        "platform":       platform,
        "epoch_id":       epoch_id,
        "epoch_label":    epoch_label,
        "weekly_vol":     weekly_vol,
        "latest_oi":      latest_oi,
        "days_collected": days_collected,
    })
    history[:] = sorted(history, key=lambda h: h.get("epoch_id", ""))[-40:]

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=== Farm Monitor Collector ===")
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    ts        = now_utc.isoformat()
    print(f"UTC time: {now_utc.strftime('%Y-%m-%d %H:%M')}")
    print()

    errors = []

    print("Fetching Variational...")
    try:
        var_vol, var_oi = fetch_variational()
    except Exception as e:
        print(f"  ERROR: {e}")
        var_vol, var_oi = None, None
        errors.append("Variational")

    print("Fetching Extended...")
    try:
        ext_vol, ext_oi = fetch_extended()
    except Exception as e:
        print(f"  ERROR: {e}")
        ext_vol, ext_oi = None, None
        errors.append("Extended")

    print()
    print("Loading Gist...")
    data = load_gist()

    _, _, var_days_rem, var_label, var_epoch_id = get_var_epoch(now_utc)
    _, _, ext_days_rem, ext_label, ext_epoch_id = get_ext_epoch(now_utc)

    print(f"  VAR epoch: {var_label}  ({var_days_rem} days remaining)")
    print(f"  EXT epoch: {ext_label}  ({ext_days_rem} days remaining)")
    print()

    var_snap = accumulate(data["var_accumulator"], var_epoch_id, var_label, var_days_rem, today_str, var_vol, var_oi)
    ext_snap = accumulate(data["ext_accumulator"], ext_epoch_id, ext_label, ext_days_rem, today_str, ext_vol, ext_oi)

    print(f"  VAR weekly vol: ${(var_snap['weekly_vol'] or 0)/1e6:.2f}M  ({var_snap['days_collected']} days)")
    print(f"  EXT weekly vol: ${(ext_snap['weekly_vol'] or 0)/1e6:.2f}M  ({ext_snap['days_collected']} days)")

    update_history(data["history"], "var", var_epoch_id, var_label, var_snap["weekly_vol"], var_snap["latest_oi"], var_snap["days_collected"])
    update_history(data["history"], "ext", ext_epoch_id, ext_label, ext_snap["weekly_vol"], ext_snap["latest_oi"], ext_snap["days_collected"])

    data["current"]      = {"timestamp": ts, "var": var_snap, "ext": ext_snap}
    data["last_updated"] = ts

    for acc_key in ("var_accumulator", "ext_accumulator"):
        for old_key in sorted(data[acc_key].keys())[:-4]:
            del data[acc_key][old_key]

    print()
    print("Saving to Gist...")
    save_gist(data)
    print()

    if errors:
        print(f"Done with errors on: {', '.join(errors)}")
        exit(1)
    else:
        print("Done! All data saved successfully.")

if __name__ == "__main__":
    main()
