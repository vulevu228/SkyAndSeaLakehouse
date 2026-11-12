"""Find a Wikipedia lead photo for every aircraft type that has FAA dimensions.
Output: aircraft_photos.json  [{typecode, model, page, image, file, artist, license}]
1) exact page title = model name (redirects followed), 50 titles per request
2) otherwise a search, accepted only if the page title carries the model's number tokens (A320 must not become A321)
Images are Wikimedia Commons files; the report shows artist + licence under each photo."""
import html
import json
import re
import sys
import time
import urllib.parse
import urllib.request

OUT = __file__.replace("\\", "/").rsplit("/", 1)[0] + "/aircraft_photos.json"
UA = {"User-Agent": "SkySeaDashboard/1.0 (portfolio data project; python urllib)"}
WP = "https://en.wikipedia.org/w/api.php?"
COMMONS = "https://commons.wikimedia.org/w/api.php?"
AIRCRAFT = re.compile(r"aircraft|airliner|airplane|aeroplane|helicopter|jet|monoplane|biplane|turboprop|trainer|transport|glider|flying boat|bomber|fighter|tanker|seaplane|amphibi", re.I)


def get(base, **p):
    p.update(format="json", formatversion="2")
    req = urllib.request.Request(base + urllib.parse.urlencode(p), headers=UA)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception:
            time.sleep(2 + attempt * 3)
    return {}


def strip(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", s or ""))).strip()


def first_num(s):
    m = re.search(r"\d{2,}", s)
    return m.group(0) if m else None


def title_ok(model, title):
    n = first_num(model)
    nums = re.findall(r"\d+", title)
    return not n or not nums or n in nums


def pages_info(titles):
    """title -> (final title, thumb, file, description) for up to 50 titles."""
    r = get(WP, action="query", prop="pageimages|description", titles="|".join(titles), piprop="thumbnail|name",
            pithumbsize=640, redirects=1)
    q = r.get("query", {})
    alias = {}
    for k in ("normalized", "redirects"):
        for x in q.get(k, []):
            alias[x["from"]] = x["to"]
    info = {p["title"]: p for p in q.get("pages", []) if not p.get("missing")}
    out = {}
    for t in titles:
        f = t
        while f in alias:
            f = alias[f]
        p = info.get(f)
        if p and "thumbnail" in p and AIRCRAFT.search(p.get("description", "") + " " + f):
            out[t] = (f, p["thumbnail"]["source"], p.get("pageimage"), p.get("description", ""))
    return out


types = [t for t in json.load(open(sys.argv[1], encoding="utf-8")) if t["span"]]
types.sort(key=lambda t: -(t["seen"] or 0))
res = {t["typecode"]: {"typecode": t["typecode"], "model": t["model"], "page": None, "image": None, "file": None,
                       "artist": None, "license": None, "how": None} for t in types}
try:  # keep matches from an earlier run
    for old in json.load(open(OUT, encoding="utf-8")):
        if old.get("page") and old["typecode"] in res:
            res[old["typecode"]].update(old)
except (FileNotFoundError, ValueError):
    pass

# 1) exact titles: the FAA model name and its first part before "/"
cands = {}
for t in types:
    m = t["model"]
    cands[t["typecode"]] = list(dict.fromkeys([m, m.split("/")[0].strip()]))
allt = sorted({c for v in cands.values() for c in v})
found = {}
for i in range(0, len(allt), 50):
    found.update(pages_info(allt[i:i + 50]))
    time.sleep(0.3)
for tc, cs in cands.items():
    if res[tc]["page"]:
        continue
    for c in cs:
        if c in found:
            f, img, file, _ = found[c]
            res[tc].update(page=f, image=img, file=file, how="title")
            break
print("exact titles:", sum(1 for r in res.values() if r["page"]), "of", len(res), flush=True)

# 2) search for the rest, with a number-token guard
for t in types:
    r = res[t["typecode"]]
    if r["page"]:
        continue
    model = t["model"].split("/")[0].strip()
    s = get(WP, action="query", list="search", srsearch=f"{model} aircraft", srlimit=6)
    titles = [h["title"] for h in s.get("query", {}).get("search", [])
              if not h["title"].startswith("List of") and not re.search(r"accident|crash|Flight \d|airport|airline", h["title"], re.I)]
    ok = [x for x in titles if title_ok(model, x)]
    if ok:
        pi = pages_info(ok[:5])
        for x in ok[:5]:
            if x in pi:
                f, img, file, _ = pi[x]
                r.update(page=f, image=img, file=file, how="search")
                break
    print(t["typecode"], t["model"], "->", r["page"], flush=True)
    time.sleep(0.2)

# 3) licence + artist from Commons, 50 files per request
meta = {}
for r in res.values():
    if not r["file"] or r.get("license"):
        continue
    f = r["file"]
    if f not in meta:
        c = get(COMMONS, action="query", prop="imageinfo", titles="File:" + f, iiprop="extmetadata")
        pg = c.get("query", {}).get("pages", [])
        meta[f] = (None, None)
        if pg and "imageinfo" in pg[0]:
            md = pg[0]["imageinfo"][0]["extmetadata"]
            meta[f] = (strip(md.get("Artist", {}).get("value"))[:60], strip(md.get("LicenseShortName", {}).get("value")))
        time.sleep(0.15)
    r["artist"], r["license"] = meta[f]
json.dump(list(res.values()), open(OUT, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print("saved", OUT, "with photo:", sum(1 for r in res.values() if r["image"]), "licence known:", sum(1 for r in res.values() if r["license"]))
