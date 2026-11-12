"""Bundle the DAX exports (web/data/*.json) into one compact web/app/data.json, and download
the free-licensed aircraft photos into web/app/photos/<TYPECODE>.jpg.
Run after dax_export.ps1 and powerbi_build/fetch_aircraft_photos.py:   py build_data.py"""
import io
import json
import os
import urllib.request
from collections import defaultdict

from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
APP = os.path.join(HERE, "..", "app")
PHOTOS_JSON = os.path.join(HERE, "..", "..", "powerbi_build", "aircraft_photos.json")
UA = {"User-Agent": "SkySeaDashboard/1.0 (portfolio data project; python urllib)"}
os.makedirs(os.path.join(APP, "photos"), exist_ok=True)


def load(n):
    return json.load(open(os.path.join(DATA, n + ".json"), encoding="utf-8-sig"))


def r(v, nd=1):
    return None if v is None else round(v, nd)


period = load("period")[0]
out = {"period": {"first": period["first"][:10], "last": period["last"][:10], "days": period["days"],
                  "allMedian": r(period["all_median"], 2)}}

# Pulse
hourly = defaultdict(lambda: {"air": [None] * 24, "sea": [None] * 24, "ships": [None] * 24})
for h in load("hourly"):
    d = hourly[h["date"][:10]]
    for k in ("air", "sea", "ships"):
        d[k][h["hour_of_day"]] = r(h[k], 0)
out["hourly"] = dict(sorted(hourly.items()))
out["daily"] = sorted(({"date": d["date"][:10], "weekend": d["is_weekend"], "aircraft": d["aircraft"], "ships": d["ships"]}
                       for d in load("daily") if d["aircraft"] or d["ships"]), key=lambda x: x["date"])
out["quality"] = sorted(({"date": q["date"][:10], "ds": q["dataset"], "bronze": q["bronze"], "silver": q["silver"],
                          "dropped": q["dropped"], "stale": q["stale"], "speed": q["speed"]} for q in load("quality") if q["bronze"]),
                        key=lambda x: (x["date"], x["ds"]))

# Aircraft
photos = {}
if os.path.exists(PHOTOS_JSON):
    for p in json.load(open(PHOTOS_JSON, encoding="utf-8")):
        if p.get("image") and p.get("license"):
            photos[p["typecode"]] = p
types = sorted(load("types"), key=lambda t: -(t["seen"] or 0))
ops = defaultdict(list)
for o in load("operators"):
    ops[o["typecode"]].append([o["operator"], o["n"]])
out["types"] = []
got = 0
for t in types:
    tc = t["typecode"]
    rec = [tc, t["model"], t["maker"], t["cls"], t["eng"], t["engines"], t["wtc"], r(t["span"]), r(t["len"]), r(t["height"]),
           r(t["mtow"], 0), t["seen"], r(t["kmh"], 0), r(t["alt"], 0), None, None]
    p = photos.get(tc)
    if p and t["span"]:
        fn = os.path.join(APP, "photos", tc + ".jpg")
        if not os.path.exists(fn):
            try:
                req = urllib.request.Request(p["image"], headers=UA)
                im = Image.open(io.BytesIO(urllib.request.urlopen(req, timeout=30).read())).convert("RGB")
                im.thumbnail((480, 300))
                im.save(fn, "JPEG", quality=80, optimize=True)
            except Exception as e:  # noqa: BLE001
                print("photo failed", tc, e)
        if os.path.exists(fn):
            got += 1
            artist = (p.get("artist") or "unknown").replace("Unknown author", "unknown")
            rec[14] = f"photos/{tc}.jpg"
            rec[15] = [artist, p["license"], p.get("page")]
    out["types"].append(rec)
out["typeCols"] = ["code", "model", "maker", "cls", "eng", "engines", "wtc", "span", "len", "height", "mtow", "seen", "kmh", "alt", "photo", "credit"]
out["operators"] = {k: sorted(v, key=lambda x: -x[1])[:6] for k, v in ops.items()}

# Ports
ports = [p for p in load("ports") if p["calls"]]
out["ports"] = [[p["locode"], p["name"], p["country"], r(p["lat"], 3), r(p["lon"], 3), p["calls"], p["ships"], r(p["median"], 2), p["long"] or 0]
                for p in sorted(ports, key=lambda p: -p["calls"])]
out["portCols"] = ["code", "name", "country", "lat", "lon", "calls", "ships", "median", "long"]
pg = defaultdict(dict)
for x in load("port_groups"):
    if x["calls"]:
        pg[x["locode"]][x["ship_group"]] = [x["calls"], x["ships"], r(x["median"], 2), x["long"] or 0]
out["portGroups"] = pg
BANDS = ["0–12 h", "12–24 h", "24–48 h", "48 h +"]
bands = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0]))
for x in load("port_bands"):
    if x["calls"]:
        bands[x["locode"]][x["ship_group"]][BANDS.index(x["dwell_band"])] = x["calls"]
out["bands"] = {k: dict(v) for k, v in bands.items()}
out["bandNames"] = BANDS
stays = defaultdict(lambda: defaultdict(list))
for x in load("stays"):
    stays[x["locode"]][x["ship_group"]].append([x["ship_label"], r(x["h"], 1)])
out["stays"] = {k: {g: sorted(v, key=lambda s: -s[1])[:10] for g, v in gg.items()} for k, gg in stays.items()}

# World land (TopoJSON -> rings of [lon, lat], 1 decimal)
topo = json.load(open(os.path.join(HERE, "ref", "land-110m.json"), encoding="utf-8"))
sc, tr = topo["transform"]["scale"], topo["transform"]["translate"]
arcs = []
for a in topo["arcs"]:
    x = y = 0
    pts = []
    for dx, dy in a:
        x += dx
        y += dy
        pts.append([round(x * sc[0] + tr[0], 1), round(y * sc[1] + tr[1], 1)])
    arcs.append(pts)


def ring(idx):
    pts = []
    for i in idx:
        seg = arcs[i] if i >= 0 else arcs[~i][::-1]
        pts.extend(seg if not pts else seg[1:])
    return pts


land = []
for g in topo["objects"]["land"]["geometries"]:
    polys = g["arcs"] if g["type"] == "MultiPolygon" else [g["arcs"]]
    for poly in polys:
        land.append([ring(rg) for rg in poly])
out["land"] = land

js = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
open(os.path.join(APP, "data.json"), "w", encoding="utf-8").write(js)
print("data.json", round(len(js) / 1e6, 2), "MB | types", len(out["types"]), "photos", got, "| ports", len(out["ports"]))
