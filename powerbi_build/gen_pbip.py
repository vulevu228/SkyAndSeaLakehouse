"""Generates the "Sky & Sea" Power BI project on top of the Databricks gold tables (workspace.skysea).

Model: 11 M queries against the SQL Warehouse (Databricks.Catalogs, Import) + _Measures. No DAX calculated tables.
Report: 4 pages (Pulse / Aircraft in focus / Port dwell / Data quality), 1280 x 720, light "product UI" design:
designed page backgrounds (white rounded cards, soft shadows, icon rail) rendered by make_backgrounds(),
transparent visuals on top, hero numbers, pill slicers, SVG measures (aircraft silhouette to scale, spec bars).

Built on Documents/Power BI - Query/PowerQuery-SkySea-Hamburg/_build/gen_pbip.py: only visual types and property
shapes that already worked on this machine (see Desktop/PowerBI-PBIP-Errors-and-Fixes.txt). New here, verify in
Desktop: tableEx used for SVG images (measure dataCategory ImageUrl) and for left-aligned text lines.
Re-running overwrites model + pages: snapshot + semantic diff first if the user saved in Desktop.
Run with the global Python (needs Pillow):  py gen_pbip.py
"""
import hashlib, json, math, os, re, shutil, uuid

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = os.environ.get("GEN_ROOT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "powerbi").replace(os.sep, "/") + "/")
NAME = "SkySeaDatabricks"
REPORT = ROOT + NAME + ".Report/"
SM = ROOT + NAME + ".SemanticModel/"
MODEL = SM + "definition/"
TEMPLATE = os.environ.get("GEN_TEMPLATE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "HamburgTrainDelays", "powerbi", "hamburg_delays"))  # any PBIP used as a file template
HOST = "dbc-745b4485-763f.cloud.databricks.com"          # not secret; the token stays in Desktop's credential store
HTTP_PATH = "/sql/1.0/warehouses/c52db76098574ec9"

# ============================================================== design tokens (light)
WALL, CANVAS, CARD, LINE = "#E4E8EC", "#EEF1F4", "#FFFFFF", "#E3E7EC"
INK1, INK2, INK3 = "#111418", "#4F5865", "#8A94A1"
AIR, SEA, ALERT = "#2A78D6", "#1BAF7A", "#EB6834"   # validated: categorical slots 1, 3, 2 (light)
DARK = "#16191D"                                    # active pill / plane fill
FONT_H, FONT_B, FONT_L = "Segoe UI Semibold", "Segoe UI", "Segoe UI Light"
W, H = 1280, 720
PAGE_H = {"aircraft": 860}                           # taller page (scrolls, FitToWidth) instead of a 5th page
MEAS = "_Measures"
SCALE = 2                                            # background PNG at 2x


def guid(seed):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "skysea-databricks/" + seed))


def hid(seed):
    return hashlib.md5(("skysea-databricks/" + seed).encode()).hexdigest()[:20]


def tq(n):
    return f"'{n}'" if re.search(r"[^A-Za-z0-9_]", n) else n


# ============================================================== M queries (Databricks SQL Warehouse)
def m_table(src, cols, extra_steps=""):
    """cols: [(name, m_type)]. Timestamps may arrive as datetimezone: switched to UTC and the zone removed,
    so nothing is converted to the PC's local time."""
    names = ", ".join(f'"{c}"' for c, _ in cols)
    types = ", ".join(f'{{"{c}", {t}}}' for c, t in cols if t != "datetime")
    ts_cols = [c for c, t in cols if t == "datetime"]
    ts_step = ""
    last = "Typed"
    if ts_cols:
        fx = ", ".join(f'{{"{c}", each if _ is datetimezone then DateTimeZone.RemoveZone(DateTimeZone.SwitchZone(_, 0)) else _, type datetime}}'
                       for c in ts_cols)
        ts_step = f',\n    #"UTC datetime" = Table.TransformColumns(Typed, {{{fx}}})'
        last = '#"UTC datetime"'
    steps = (f'let\n'
             f'    Source = Databricks.Catalogs(DatabricksHost, DatabricksHttpPath, [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = null]),\n'
             f'    Catalog = Source{{[Name = "workspace"]}}[Data],\n'
             f'    Schema = Catalog{{[Name = "skysea"]}}[Data],\n'
             f'    Data = Schema{{[Name = "{src}"]}}[Data],\n'
             f'    Kept = Table.SelectColumns(Data, {{{names}}}),\n'
             f'    Typed = Table.TransformColumnTypes(Kept, {{{types}}}){ts_step}')
    if extra_steps:
        steps += ",\n" + extra_steps.replace("{prev}", last)
        last = extra_steps.strip().split("\n")[-1].split("=")[0].strip()
    return steps + f"\nin\n    {last}"


M, COLS = {}, {}


def table(name, src, cols, extra="", added=()):
    """cols: (name, m_type, tmdl_type, format[, sortBy[, hidden]]); added = extra M columns (same tuple)."""
    M[name] = m_table(src, [(c[0], c[1]) for c in cols], extra)
    COLS[name] = [(c[0], c[2], c[3]) + tuple(c[4:]) for c in list(cols) + list(added)]


I64, NUM, TXT, BOOL, DT, DATE = "Int64.Type", "type number", "type text", "type logical", "datetime", "type date"

table("DimDate", "dim_date",
      [("date_key", I64, "int64", "0", None, True), ("date", DATE, "date", "dd mmm yyyy"),
       ("weekday", TXT, "string", None), ("is_weekend", BOOL, "boolean", None)],
      extra='    #"Added day_label" = Table.AddColumn({prev}, "day_label", each Date.ToText([date], "ddd d MMM", "en-GB"), type text)',
      added=[("day_label", None, "string", None, "date_key")])

table("DimAircraft", "dim_aircraft",
      [("icao24", TXT, "string", None), ("registration", TXT, "string", None), ("typecode", TXT, "string", None),
       ("operator", TXT, "string", None), ("origin_country", TXT, "string", None), ("built_year", I64, "int64", "0"),
       ("is_test_transponder", BOOL, "boolean", None)])

CLASS_ORDER = ["Heavy jet", "Medium jet", "Light jet", "Turboprop", "Piston", "Helicopter", "Other", "Unknown"]
table("DimAircraftType", "dim_aircraft_type",
      [("typecode", TXT, "string", None), ("model_name", TXT, "string", None), ("manufacturer", TXT, "string", None),
       ("aircraft_class", TXT, "string", None, "class_order"), ("engine_type", TXT, "string", None),
       ("engines", I64, "int64", "0"), ("wtc", TXT, "string", None), ("wingspan_m", NUM, "double", "0.0"),
       ("length_m", NUM, "double", "0.0"), ("tail_height_m", NUM, "double", "0.0"), ("mtow_kg", NUM, "double", "#,##0"),
       ("approach_speed_kt", NUM, "double", "0"), ("has_dimensions", BOOL, "boolean", None),
       ("aircraft_seen", I64, "int64", "#,##0")],
      extra='    #"Short class" = Table.ReplaceValue({prev}, "Unknown type", "Unknown", Replacer.ReplaceValue, {"aircraft_class"}),\n'
            '    #"Added class_order" = Table.AddColumn(#"Short class", "class_order", each List.PositionOf({'
            + ", ".join(f'"{c}"' for c in CLASS_ORDER) + '}, [aircraft_class]), Int64.Type)',
      added=[("class_order", None, "int64", "0", None, True)])

SHIP_GROUPS = [("Cargo", ["cargo"]), ("Tanker", ["tanker"]), ("Passenger", ["passenger"]), ("Fishing", ["fishing"]),
               ("Tug & service", ["tug", "towing", "pilot", "port tender", "search and rescue", "law enforcement",
                                  "anti-pollution", "medical transport", "dredging / underwater ops", "diving ops"]),
               ("Leisure", ["sailing", "pleasure craft"])]
grp_m = " else ".join("if List.Contains({" + ", ".join(f'"{v}"' for v in vals) + f'}}, [ship_category]) then "{g}"'
                      for g, vals in SHIP_GROUPS) + ' else "Other"'
table("DimShip", "dim_ship",
      [("mmsi", TXT, "string", None), ("ship_name", TXT, "string", None), ("ship_category", TXT, "string", None),
       ("is_seagoing", BOOL, "boolean", None), ("length_m", NUM, "double", "0"), ("mmsi_kind", TXT, "string", None)],
      extra=f'    #"Added ship_group" = Table.AddColumn({{prev}}, "ship_group", each {grp_m}, type text),\n'
            f'    #"Added group_order" = Table.AddColumn(#"Added ship_group", "group_order", each List.PositionOf({{'
            + ", ".join(f'"{g}"' for g, _ in SHIP_GROUPS) + ', "Other"}, [ship_group]), Int64.Type),\n'
            '    #"Added ship_label" = Table.AddColumn(#"Added group_order", "ship_label", each if [ship_name] = null or [ship_name] = "" then "MMSI " & [mmsi] else [ship_name], type text)',
      added=[("ship_group", None, "string", None, "group_order"), ("group_order", None, "int64", "0", None, True),
             ("ship_label", None, "string", None)])

PORT_LABEL = '    #"Added port_label" = Table.AddColumn({prev}, "port_label", each [port_name] & " · " & [locode], type text)'
table("DimPort", "dim_port",
      [("locode", TXT, "string", None), ("port_name", TXT, "string", None), ("country", TXT, "string", None),
       ("port_lat", NUM, "double", "0.0000"), ("port_lon", NUM, "double", "0.0000"), ("dest_ships", I64, "int64", "#,##0")],
      extra=PORT_LABEL, added=[("port_label", None, "string", None)])
# Disconnected copy for the "port in focus" slicer: it must not filter the port ranking chart.
table("PortPick", "dim_port",
      [("locode", TXT, "string", None), ("port_name", TXT, "string", None), ("country", TXT, "string", None)],
      extra=PORT_LABEL.replace("port_label", "port_pick") + ',\n    #"Sorted ports" = Table.Sort(#"Added port_pick", {{"port_pick", Order.Ascending}})',
      added=[("port_pick", None, "string", None)])

table("FactAircraftDaily", "fact_aircraft_daily",
      [("date_key", I64, "int64", "0"), ("icao24", TXT, "string", None), ("snapshots", I64, "int64", "#,##0"),
       ("airborne_snapshots", I64, "int64", "#,##0"), ("max_baro_altitude_m", NUM, "double", "#,##0"),
       ("avg_airborne_speed_kmh", NUM, "double", "#,##0"), ("emergency_snapshots", I64, "int64", "0")])

table("FactShipDaily", "fact_ship_daily",
      [("date_key", I64, "int64", "0"), ("mmsi", TXT, "string", None), ("position_windows", I64, "int64", "#,##0"),
       ("avg_sog_knots", NUM, "double", "0.0"), ("stationary_windows", I64, "int64", "#,##0")])

table("FactTrafficHourly", "fact_traffic_hourly",
      [("date_key", I64, "int64", "0"), ("hour_utc", DT, "dateTime", "dd mmm yyyy hh:nn"), ("hour_of_day", I64, "int64", "00"),
       ("aircraft", I64, "int64", "#,##0"), ("aircraft_airborne", I64, "int64", "#,##0"), ("ships", I64, "int64", "#,##0"),
       ("ships_moving", I64, "int64", "#,##0")])

BANDS = [("0–12 h", 0, 12), ("12–24 h", 12, 24), ("24–48 h", 24, 48), ("48 h +", 48, 10 ** 6)]
band_m = " else ".join(f'if [duration_h] < {hi} then "{lab}"' for lab, lo, hi in BANDS[:-1]) + f' else "{BANDS[-1][0]}"'
table("FactPortCalls", "fact_port_calls",
      [("call_id", TXT, "string", None), ("mmsi", TXT, "string", None), ("locode", TXT, "string", None),
       ("date_key", I64, "int64", "0"), ("arrival", DT, "dateTime", "dd mmm hh:nn"), ("departure", DT, "dateTime", "dd mmm hh:nn"),
       ("duration_h", NUM, "double", "0.0"), ("is_truncated", BOOL, "boolean", None), ("is_seagoing", BOOL, "boolean", None)],
      extra=f'    #"Added dwell_band" = Table.AddColumn({{prev}}, "dwell_band", each {band_m}, type text),\n'
            f'    #"Added band_order" = Table.AddColumn(#"Added dwell_band", "band_order", each List.PositionOf({{'
            + ", ".join(f'"{b[0]}"' for b in BANDS) + '}, [dwell_band]), Int64.Type)',
      added=[("dwell_band", None, "string", None, "band_order"), ("band_order", None, "int64", "0", None, True)])

table("FactQuality", "fact_quality_daily",
      [("dataset", TXT, "string", None), ("date_key", I64, "int64", "0"), ("bronze_rows", I64, "int64", "#,##0"),
       ("silver_rows", I64, "int64", "#,##0"), ("dropped_rows", I64, "int64", "#,##0"), ("flag_stale", I64, "int64", "#,##0"),
       ("flag_speed", I64, "int64", "#,##0"), ("flag_altitude", I64, "int64", "#,##0")])

def photo_table():
    """Wikipedia/Wikimedia Commons lead photo per type code (fetch_aircraft_photos.py). Only free-licensed Commons files."""
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aircraft_photos.json")
    rows = []
    if os.path.exists(src):
        for r in json.load(open(src, encoding="utf-8")):
            if r.get("image") and r.get("license"):
                artist = (r.get("artist") or "unknown").replace("Unknown author", "unknown")
                credit = f"Photo: {artist} · {r['license']} · Wikimedia Commons"
                rows.append((r["typecode"], r["image"], credit))
    esc = lambda v: v.replace('"', '""').replace("#(", "# (")  # noqa: E731
    lit_rows = ", ".join("{" + ", ".join(f'"{esc(v)}"' for v in row) + "}" for row in rows)
    M["AircraftPhoto"] = ("let\n    Source = #table(type table [typecode = text, image = text, credit = text], {" + lit_rows + "})\nin\n    Source")
    COLS["AircraftPhoto"] = [("typecode", "string", None), ("image", "string", None), ("credit", "string", None)]
    return len(rows)


PHOTOS = photo_table()
ORDER = ["DimDate", "DimAircraft", "DimAircraftType", "DimShip", "DimPort", "PortPick", "FactAircraftDaily",
         "FactShipDaily", "FactTrafficHourly", "FactPortCalls", "FactQuality", "AircraftPhoto", MEAS]
RELS = [("FactAircraftDaily", "date_key", "DimDate", "date_key"), ("FactAircraftDaily", "icao24", "DimAircraft", "icao24"),
        ("DimAircraft", "typecode", "DimAircraftType", "typecode"),
        ("FactShipDaily", "date_key", "DimDate", "date_key"), ("FactShipDaily", "mmsi", "DimShip", "mmsi"),
        ("FactTrafficHourly", "date_key", "DimDate", "date_key"),
        ("FactPortCalls", "date_key", "DimDate", "date_key"), ("FactPortCalls", "mmsi", "DimShip", "mmsi"),
        ("FactPortCalls", "locode", "DimPort", "locode"), ("FactQuality", "date_key", "DimDate", "date_key")]


def col_block(table, spec):
    name, dt, fmt = spec[0], spec[1], spec[2]
    sort = spec[3] if len(spec) > 3 else None
    hidden = spec[4] if len(spec) > 4 else False
    o = [f"\tcolumn {tq(name)}", f"\t\tdataType: {'dateTime' if dt == 'date' else dt}"]
    if hidden:
        o.append("\t\tisHidden")
    if fmt:
        o.append(f"\t\tformatString: {fmt}")
    o.append(f"\t\tlineageTag: {guid(f'c/{table}/{name}')}")
    o.append("\t\tsummarizeBy: none")
    o.append(f"\t\tsourceColumn: {name}")
    if sort:
        o.append(f"\t\tsortByColumn: {tq(sort)}")
    o += ["", "\t\tannotation SummarizationSetBy = Automatic"]
    if dt == "date":
        o += ["", "\t\tannotation UnderlyingDateTimeDataType = Date"]
    o.append("")
    return o


def m_partition(tname, m):
    o = [f"\tpartition {tq(tname)} = m", "\t\tmode: import", "\t\tsource ="]
    o += ["\t\t\t\t" + l for l in m.split("\n")]
    o += ["", "\tannotation PBI_ResultType = Table", ""]
    return o


def tmdl_table(t):
    o = [f"table {t}", f"\tlineageTag: {guid('t/' + t)}", ""]
    for spec in COLS[t]:
        o += col_block(t, spec)
    return "\n".join(o + m_partition(t, M[t]))


# ============================================================== measures
class Measures:
    def __init__(self):
        self.items = []

    def add(self, name, dax, fmt=None, folder="", category=None):
        self.items.append((name, dax, fmt, folder, category))

    def tmdl(self):
        o = [f"table {MEAS}", f"\tlineageTag: {guid('t/_Measures')}", ""]
        for name, dax, fmt, folder, cat in self.items:
            lines = dax.strip("\n").split("\n")
            if len(lines) == 1:
                o.append(f"\tmeasure '{name}' = {lines[0]}")
            else:
                o.append(f"\tmeasure '{name}' =")
                o += ["\t\t\t" + l for l in lines]
            if fmt:
                o.append(f"\t\tformatString: {fmt}")
            if folder:
                o.append(f"\t\tdisplayFolder: {folder}")
            if cat:
                o.append(f"\t\tdataCategory: {cat}")
            o.append(f"\t\tlineageTag: {guid('m/' + name)}")
            o.append("")
        o += ["\tcolumn Value", "\t\tisHidden", "\t\tdataType: int64", "\t\tformatString: 0",
              f"\t\tlineageTag: {guid('c/_Measures.Value')}", "\t\tsummarizeBy: none", "\t\tsourceColumn: Value", "",
              "\t\tannotation SummarizationSetBy = Automatic", "",
              f"\tpartition {MEAS} = m", "\t\tmode: import", "\t\tsource =", "\t\t\t\tlet",
              '\t\t\t\t  Source = #table({"Value"}, {{0}}),',
              '\t\t\t\t  #"Changed Type" = Table.TransformColumnTypes(Source, {{"Value", Int64.Type}})',
              "\t\t\t\tin", '\t\t\t\t  #"Changed Type"', "", "\tannotation PBI_ResultType = Table", ""]
        return "\n".join(o)


def txt(expr, fmt="#,##0"):
    # thin-space thousands separator: cards re-format "7,390" with the viewer's locale, this they leave alone
    return f'SUBSTITUTE(FORMAT({expr}, "{fmt}", "en-GB"), ",", UNICHAR(8239))'


def f2(expr):
    """number -> SVG coordinate text with a dot decimal, whatever the viewer's locale."""
    return f'FORMAT({expr}, "0.00", "en-US")'


def plane_points(S, L, jet, prefix):
    """DAX VAR lines for a top-view airliner silhouette in metres, nose up, centred on (0,0).
    Returns (var_lines, wings_points_expr, tail_points_expr, fus_expr dict)."""
    p = prefix
    v = [f"VAR {p}f = MAX(1.2, {L} * 0.075)",
         f"VAR {p}y0 = -{L} * 0.08",
         f"VAR {p}cr = {L} * 0.2",
         f"VAR {p}sw = IF({jet}, {S} / 2 * 0.5, {S} / 2 * 0.08)",
         f"VAR {p}ct = {p}cr * 0.35",
         f"VAR {p}yt = {L} / 2 - {L} * 0.17",
         f"VAR {p}hs = MAX({S} * 0.17, {L} * 0.14)",
         f"VAR {p}tc = {L} * 0.1",
         f"VAR {p}tsw = IF({jet}, {p}hs * 0.6, {p}hs * 0.1)"]

    def pt(x, y):
        return f'{f2(x)} & "," & {f2(y)}'

    def poly(pts):
        return ' & " " & '.join(pt(x, y) for x, y in pts)

    wing_r = [(f"{p}f/2", f"{p}y0"), (f"{S}/2", f"{p}y0+{p}sw"), (f"{S}/2", f"{p}y0+{p}sw+{p}ct"), (f"{p}f/2", f"{p}y0+{p}cr")]
    wing_l = [(f"-({x})", y) for x, y in reversed(wing_r)]
    tail_r = [(f"{p}f*0.3", f"{p}yt"), (f"{p}hs", f"{p}yt+{p}tsw"), (f"{p}hs", f"{p}yt+{p}tsw+{p}tc*0.45"), (f"{p}f*0.3", f"{p}yt+{p}tc")]
    tail_l = [(f"-({x})", y) for x, y in reversed(tail_r)]
    return v, poly(wing_r + wing_l), poly(tail_r + tail_l)


def svg_text(x, y, size, color, text, anchor="start", weight="400"):
    return (f'"<text x=\'" & {f2(x)} & "\' y=\'" & {f2(y)} & "\' font-family=\'Segoe UI\' font-size=\'" & {f2(size)} & '
            f'"\' font-weight=\'{weight}\' fill=\'{color.replace("#", "%23")}\' text-anchor=\'{anchor}\'>" & {text} & "</text>"')


def silhouette_measure():
    """Top view of the focus type to scale, over a dashed A320 outline (35.8 m span, 37.6 m long) for reference."""
    ft = "[Focus Type]"
    look = lambda c: f"CALCULATE(MAX(DimAircraftType[{c}]), REMOVEFILTERS(DimAircraftType), DimAircraftType[typecode] = ft)"  # noqa: E731
    v = [f"VAR ft = {ft}", f"VAR S = {look('wingspan_m')}", f"VAR L = {look('length_m')}",
         f"VAR Eng = {look('engine_type')}", f"VAR N = {look('engines')}", 'VAR jet = Eng = "Jet"']
    pv, wings, tail = plane_points("S", "L", "jet", "p")
    gv, gwings, gtail = plane_points("35.8", "37.6", "TRUE()", "g")
    ink, ghost, mid = DARK.replace("#", "%23"), "%23B8C0CA", INK3.replace("#", "%23")
    v += pv + gv + ["VAR E = MAX(MAX(S, L), 37.6) * 1.18", "VAR fs = E * 0.058"]
    eng = ('IF(jet || Eng = "Turboprop/Turboshaft", '
           '"<ellipse cx=\'" & ' + f2("S*0.18") + ' & "\' cy=\'" & ' + f2("py0+psw*0.36-L*0.02") + ' & "\' rx=\'" & ' + f2("pf*0.32") +
           ' & "\' ry=\'" & ' + f2("L*0.045") + f' & "\' fill=\'{ink}\'/>" & '
           '"<ellipse cx=\'" & ' + f2("-S*0.18") + ' & "\' cy=\'" & ' + f2("py0+psw*0.36-L*0.02") + ' & "\' rx=\'" & ' + f2("pf*0.32") +
           ' & "\' ry=\'" & ' + f2("L*0.045") + f' & "\' fill=\'{ink}\'/>" & '
           'IF(N >= 4, "<ellipse cx=\'" & ' + f2("S*0.33") + ' & "\' cy=\'" & ' + f2("py0+psw*0.66-L*0.02") + ' & "\' rx=\'" & ' + f2("pf*0.28") +
           ' & "\' ry=\'" & ' + f2("L*0.04") + f' & "\' fill=\'{ink}\'/>" & '
           '"<ellipse cx=\'" & ' + f2("-S*0.33") + ' & "\' cy=\'" & ' + f2("py0+psw*0.66-L*0.02") + ' & "\' rx=\'" & ' + f2("pf*0.28") +
           ' & "\' ry=\'" & ' + f2("L*0.04") + f' & "\' fill=\'{ink}\'/>", ""), "")')
    body = (
        '"<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'" & ' + f2("-E") + ' & " " & ' + f2("-E/2") + ' & " " & ' + f2("2*E") + ' & " " & ' + f2("E") + ' & "\'>" & '
        # the focus type
        f'"<g fill=\'{ink}\'>" & "<polygon points=\'" & ' + wings + ' & "\'/>" & "<polygon points=\'" & ' + tail + ' & "\'/>" & '
        '"<rect x=\'" & ' + f2("-pf/2") + ' & "\' y=\'" & ' + f2("-L/2") + ' & "\' width=\'" & ' + f2("pf") + ' & "\' height=\'" & ' + f2("L") + ' & "\' rx=\'" & ' + f2("pf/2") + ' & "\'/>" & ' + eng + ' & "</g>" & '
        # A320 reference on top, dashed, so it stays visible over big jets
        f'"<g fill=\'none\' stroke=\'{ghost}\' stroke-width=\'" & ' + f2("E*0.004") + ' & "\' stroke-dasharray=\'" & ' + f2("E*0.012") + ' & "\'>" & '
        '"<polygon points=\'" & ' + gwings + ' & "\'/>" & "<polygon points=\'" & ' + gtail + ' & "\'/>" & '
        '"<rect x=\'" & ' + f2("-gf/2") + ' & "\' y=\'" & ' + f2("-37.6/2") + ' & "\' width=\'" & ' + f2("gf") + ' & "\' height=\'37.6\' rx=\'" & ' + f2("gf/2") + ' & "\'/></g>" & '
        # dimension lines: span below, length on the left
        f'"<g stroke=\'{mid}\' stroke-width=\'" & ' + f2("E*0.003") + ' & "\'>" & '
        '"<line x1=\'" & ' + f2("-S/2") + ' & "\' y1=\'" & ' + f2("E*0.44") + ' & "\' x2=\'" & ' + f2("S/2") + ' & "\' y2=\'" & ' + f2("E*0.44") + ' & "\'/>" & '
        '"<line x1=\'" & ' + f2("-E*0.62") + ' & "\' y1=\'" & ' + f2("-L/2") + ' & "\' x2=\'" & ' + f2("-E*0.62") + ' & "\' y2=\'" & ' + f2("L/2") + ' & "\'/></g>" & '
        + svg_text("S/2+E*0.03", "E*0.44+fs*0.35", "fs", INK2, 'FORMAT(S, "0.0", "en-US") & " m span"', "start") + ' & '
        + svg_text("-E*0.65", "0", "fs", INK2, 'FORMAT(L, "0.0", "en-US") & " m long"', "end") + ' & '
        + svg_text("E*0.98", "-E*0.40", "fs*0.9", INK3, '"- - -  A320 for scale"', "end") + ' & '
        '"</svg>"')
    empty = ('"<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 440 220\'>" & '
             + svg_text("220", "110", "14", INK3, '"No FAA dimensions for this type"', "middle") + ' & "</svg>"')
    return "\n".join(v) + f'\nRETURN "data:image/svg+xml;utf8," & IF(ISBLANK(S) || ISBLANK(L), {empty},\n    {body})'


def spec_bars_measure():
    """Wingspan, length and MTOW of the focus type against the largest airliner (A380: 79.8 m, 72.7 m, 575 t)."""
    look = lambda c: f"CALCULATE(MAX(DimAircraftType[{c}]), REMOVEFILTERS(DimAircraftType), DimAircraftType[typecode] = ft)"  # noqa: E731
    v = ["VAR ft = [Focus Type]", f"VAR S = {look('wingspan_m')}", f"VAR L = {look('length_m')}",
         f"VAR T = {look('mtow_kg')} / 1000"]
    rows = [("Wingspan", "S", 79.8, '" m"'), ("Length", "L", 72.7, '" m"'), ("Max take-off weight", "T", 575, '" t"')]
    parts = []
    track, fill = LINE.replace("#", "%23"), DARK.replace("#", "%23")
    for i, (lab, var, mx, unit) in enumerate(rows):
        y = 16 + i * 48
        val = f'IF(ISBLANK({var}), "n/a", FORMAT({var}, "#,##0.0", "en-US") & " / {mx:g}" & {unit})'
        parts.append(svg_text(str(0), str(y), "12", INK3, f'"{lab}"'))
        parts.append(svg_text(str(250), str(y), "12", INK1, val, "end", "600"))
        parts.append(f'"<rect x=\'0\' y=\'{y + 9}\' width=\'250\' height=\'5\' rx=\'2.5\' fill=\'{track}\'/>"')
        parts.append(f'"<rect x=\'0\' y=\'{y + 9}\' width=\'" & ' + f2(f"250 * MIN(1, DIVIDE({var}, {mx}, 0))") +
                     f' & "\' height=\'5\' rx=\'2.5\' fill=\'{fill}\'/>"')
    return "\n".join(v) + ('\nRETURN "data:image/svg+xml;utf8," & "<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 250 150\'>" & '
                           + " & ".join(parts) + ' & "</svg>"')


def build_measures():
    m = Measures()
    FA, FS, FT, FP, FQ = "FactAircraftDaily", "FactShipDaily", "FactTrafficHourly", "FactPortCalls", "FactQuality"
    DD, DT_, DS, DP = "DimDate", "DimAircraftType", "DimShip", "DimPort"

    f = "1 Pulse"
    m.add("Aircraft Seen", f"DISTINCTCOUNT({FA}[icao24])", "#,##0", f)
    m.add("Ships Seen", f'CALCULATE(DISTINCTCOUNT({FS}[mmsi]), {DS}[mmsi_kind] = "ship")', "#,##0", f)
    m.add("Days With Data", f"COUNTROWS(FILTER(VALUES({DD}[date_key]), NOT ISBLANK([Aircraft Seen]) || NOT ISBLANK([Ships Seen])))", "0", f)
    m.add("Aircraft per Day", f"AVERAGEX(VALUES({DD}[date_key]), [Aircraft Seen])", "#,##0", f)
    m.add("Ships per Day", f"AVERAGEX(VALUES({DD}[date_key]), [Ships Seen])", "#,##0", f)
    m.add("Airborne per Hour", f"AVERAGE({FT}[aircraft_airborne])", "#,##0", f)
    m.add("Ships Moving per Hour", f"AVERAGE({FT}[ships_moving])", "#,##0", f)
    m.add("Ships per Hour", f"AVERAGE({FT}[ships])", "#,##0", f)
    m.add("Moving Share", "DIVIDE([Ships Moving per Hour], [Ships per Hour])", "0%", f)
    for nm, base in (("Air", "[Airborne per Hour]"), ("Sea", "[Ships Moving per Hour]")):
        m.add(f"{nm} Peak Hour", f"MAXX(TOPN(1, VALUES({FT}[hour_of_day]), {base}, DESC), {FT}[hour_of_day])", "0", f)
        m.add(f"{nm} Low Hour", f"MAXX(TOPN(1, VALUES({FT}[hour_of_day]), {base}, ASC), {FT}[hour_of_day])", "0", f)
        m.add(f"{nm} Peak Value", f"MAXX(VALUES({FT}[hour_of_day]), {base})", "#,##0", f)
        m.add(f"{nm} Low Value", f"MINX(VALUES({FT}[hour_of_day]), {base})", "#,##0", f)
        m.add(f"{nm} Hour Color",
              f"IF(SELECTEDVALUE({FT}[hour_of_day]) = CALCULATE([{nm} Peak Hour], ALLSELECTED({FT}[hour_of_day])), \"{DARK}\", \"{AIR if nm == 'Air' else SEA}\")",
              None, f)
    m.add("Air Hero Text", txt("[Airborne per Hour]"), None, f)
    m.add("Sea Hero Text", txt("[Ships Moving per Hour]"), None, f)
    m.add("Air Hero Sub",
          '"peak " & ' + txt("[Air Peak Value]") + ' & " at " & FORMAT([Air Peak Hour], "00") & ":00 · low " & '
          + txt("[Air Low Value]") + ' & " at " & FORMAT([Air Low Hour], "00") & ":00 UTC"', None, f)
    m.add("Sea Hero Sub",
          'FORMAT([Moving Share], "0%") & " of " & ' + txt("[Ships per Hour]") + ' & " ships under way · peak " & FORMAT([Sea Peak Hour], "00") & ":00 UTC"',
          None, f)
    m.add("Weekend Air Change",
          f"VAR wd = CALCULATE([Aircraft per Day], {DD}[is_weekend] = FALSE())\nVAR we = CALCULATE([Aircraft per Day], {DD}[is_weekend] = TRUE())\n"
          "RETURN IF(ISBLANK(wd) || ISBLANK(we), BLANK(), DIVIDE(we - wd, wd))", "+0%;-0%;0%", f)
    m.add("Weekend Sea Change",
          f"VAR wd = CALCULATE([Ships per Day], {DD}[is_weekend] = FALSE())\nVAR we = CALCULATE([Ships per Day], {DD}[is_weekend] = TRUE())\n"
          "RETURN IF(ISBLANK(wd) || ISBLANK(we), BLANK(), DIVIDE(we - wd, wd))", "+0%;-0%;0%", f)
    m.add("Weekend Air Text", 'IF(ISBLANK([Weekend Air Change]), "–", FORMAT([Weekend Air Change], "+0%;-0%;0%"))', None, f)
    m.add("Weekend Sea Text", 'IF(ISBLANK([Weekend Sea Change]), "–", FORMAT([Weekend Sea Change], "+0%;-0%;0%"))', None, f)
    m.add("Weekend Air Sub",
          f'{txt(f"CALCULATE([Aircraft per Day], {DD}[is_weekend] = TRUE())")} & " a weekend day vs " & '
          f'{txt(f"CALCULATE([Aircraft per Day], {DD}[is_weekend] = FALSE())")} & " a weekday"', None, f)
    m.add("Weekend Sea Sub",
          f'{txt(f"CALCULATE([Ships per Day], {DD}[is_weekend] = TRUE())")} & " a weekend day vs " & '
          f'{txt(f"CALCULATE([Ships per Day], {DD}[is_weekend] = FALSE())")} & " a weekday"', None, f)
    m.add("Busiest Day Text",
          f'VAR d = MAXX(TOPN(1, VALUES({DD}[date_key]), [Aircraft Seen], DESC), {DD}[date_key])\n'
          f'VAR v = CALCULATE([Aircraft Seen], {DD}[date_key] = d)\n'
          f'RETURN "Busiest sky: " & CALCULATE(MAX({DD}[day_label]), {DD}[date_key] = d) & " · " & {txt("v")} & " aircraft, " & '
          f'FORMAT(DIVIDE(v, [Aircraft per Day]) - 1, "+0%;-0%") & " vs the average day"', None, f)
    m.add("Period Text",
          f'"Raspberry Pi collectors · " & FORMAT(MIN({DD}[date]), "d mmm") & " – " & FORMAT(MAX({DD}[date]), "d mmm yyyy") & '
          f'" · " & [Days With Data] & " days · all times UTC"', None, f)

    f = "2 Aircraft"
    m.add("Type Aircraft", f"DISTINCTCOUNT({FA}[icao24])", "#,##0", f)
    m.add("Focus Type",
          f"VAR sel = SELECTEDVALUE({DT_}[typecode])\n"
          f"VAR best = MAXX(TOPN(1, FILTER(VALUES({DT_}[typecode]), {DT_}[typecode] <> \"UNKN\" && CALCULATE(COUNTROWS({DT_}), {DT_}[has_dimensions] = TRUE()) > 0), [Type Aircraft], DESC), {DT_}[typecode])\n"
          f"VAR any = MAXX(TOPN(1, FILTER(VALUES({DT_}[typecode]), {DT_}[typecode] <> \"UNKN\"), [Type Aircraft], DESC), {DT_}[typecode])\n"
          "RETURN IF(NOT ISBLANK(sel) && sel <> \"UNKN\", sel, IF(ISBLANK(best), any, best))", None, f)
    look = lambda c: f"CALCULATE(MAX({DT_}[{c}]), REMOVEFILTERS({DT_}), {DT_}[typecode] = ft)"  # noqa: E731
    m.add("Focus Model", f"VAR ft = [Focus Type]\nRETURN {look('model_name')}", None, f)
    m.add("Focus Class Chip", f"VAR ft = [Focus Type]\nRETURN UPPER({look('aircraft_class')}) & \"  ·  \" & {look('manufacturer')} & \"  ·  \" & ft", None, f)
    m.add("Focus Dims Text",
          "VAR ft = [Focus Type]\n" + f"VAR S = {look('wingspan_m')}\nVAR L = {look('length_m')}\nVAR Ht = {look('tail_height_m')}\n"
          'RETURN IF(ISBLANK(S), "Dimensions not in the FAA database", "L " & FORMAT(L, "0.0") & "  ·  W " & FORMAT(S, "0.0") & "  ·  H " & FORMAT(Ht, "0.0") & " m")',
          None, f)
    m.add("Focus Sub Text",
          "VAR ft = [Focus Type]\n" + f"VAR T = {look('mtow_kg')}\nVAR N = {look('engines')}\nVAR Eng = {look('engine_type')}\nVAR Wt = {look('wtc')}\n"
          'RETURN IF(ISBLANK(T), "", FORMAT(T / 1000, "#,##0.0") & " t max take-off  ·  ") & N & " × " & Eng & "  ·  wake category " & Wt',
          None, f)
    m.add("Focus Seen",
          f"VAR ft = [Focus Type]\nRETURN CALCULATE([Type Aircraft], REMOVEFILTERS({DT_}), {DT_}[typecode] = ft)", "#,##0", f)
    m.add("Focus Seen Text",
          f'VAR share = DIVIDE([Focus Seen], CALCULATE([Type Aircraft], REMOVEFILTERS({DT_})))\n'
          f'RETURN {txt("[Focus Seen]")} & " seen  ·  1 in " & FORMAT(DIVIDE(1, share), "#,##0") & " aircraft"', None, f)
    m.add("Focus Cruise Text",
          f"VAR ft = [Focus Type]\n"
          f"VAR spd = CALCULATE(AVERAGE({FA}[avg_airborne_speed_kmh]), REMOVEFILTERS({DT_}), {DT_}[typecode] = ft)\n"
          f"VAR alt = CALCULATE(MEDIAN({FA}[max_baro_altitude_m]), REMOVEFILTERS({DT_}), {DT_}[typecode] = ft)\n"
          'RETURN "Flies " & FORMAT(spd, "#,##0") & " km/h  ·  tops out at " & FORMAT(alt, "#,##0") & " m"', None, f)
    m.add("Focus Gate Text",
          "VAR ft = [Focus Type]\n" + f"VAR S = {look('wingspan_m')}\n"
          'VAR code = SWITCH(TRUE(), S < 15, "A", S < 24, "B", S < 36, "C", S < 52, "D", S < 65, "E", S < 80, "F", "–")\n'
          'RETURN IF(ISBLANK(S), "", "Fits ICAO code " & code & " stands  ·  " & SWITCH(code, "A", "under 15 m", "B", "under 24 m", '
          '"C", "under 36 m", "D", "under 52 m", "E", "under 65 m", "F", "under 80 m", "") & " span")', None, f)
    m.add("Focus Silhouette", silhouette_measure(), None, f, "ImageUrl")
    m.add("Focus Photo",
          "VAR ft = [Focus Type]\nVAR u = CALCULATE(MAX(AircraftPhoto[image]), AircraftPhoto[typecode] = ft)\n"
          'RETURN IF(ISBLANK(u), "data:image/svg+xml;utf8,<svg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 256 144\'>'
          '<rect width=\'256\' height=\'144\' rx=\'10\' fill=\'%23EEF1F4\'/><text x=\'128\' y=\'76\' font-family=\'Segoe UI\' '
          'font-size=\'11\' fill=\'%238A94A1\' text-anchor=\'middle\'>No free photo for this type</text></svg>", u)', None, f, "ImageUrl")
    m.add("Focus Photo Credit",
          "VAR ft = [Focus Type]\nRETURN CALCULATE(MAX(AircraftPhoto[credit]), AircraftPhoto[typecode] = ft) & \"\"", None, f)
    m.add("Focus Spec Bars", spec_bars_measure(), None, f, "ImageUrl")
    m.add("Top Types",
          f"VAR N = SELECTEDVALUE({DT_}[model_name])\nVAR V = [Type Aircraft]\n"
          f"RETURN IF(ISBLANK(N) || ISBLANK(V) || N = \"Unknown type\", BLANK(),\n"
          f"    VAR R = RANKX(FILTER(ALLSELECTED({DT_}[model_name]), {DT_}[model_name] <> \"Unknown type\" && NOT ISBLANK([Type Aircraft])), [Type Aircraft], V, DESC)\n"
          "    RETURN IF(R <= 20, V))", "#,##0", f)
    m.add("Type Bar Color", f'IF(SELECTEDVALUE({DT_}[model_name]) = CALCULATE([Focus Model], ALLSELECTED({DT_}[model_name])), "{DARK}", "{AIR}")', None, f)
    m.add("Focus Operators",
          f"// the focus type must not depend on the operator row (each operator would find its own top type)\n"
          f"VAR ft = CALCULATE([Focus Type], REMOVEFILTERS(DimAircraft[operator]))\nVAR op = SELECTEDVALUE(DimAircraft[operator])\n"
          f"VAR V = CALCULATE([Type Aircraft], REMOVEFILTERS({DT_}), {DT_}[typecode] = ft)\n"
          "RETURN IF(ISBLANK(op) || ISBLANK(V), BLANK(),\n"
          f"    VAR R = RANKX(FILTER(ALLSELECTED(DimAircraft[operator]), NOT ISBLANK(DimAircraft[operator]) && NOT ISBLANK(CALCULATE([Type Aircraft], REMOVEFILTERS({DT_}), {DT_}[typecode] = ft))),\n"
          f"        CALCULATE([Type Aircraft], REMOVEFILTERS({DT_}), {DT_}[typecode] = ft), V, DESC)\n"
          "    RETURN IF(R <= 6, V))", "#,##0", f)
    m.add("Operators Title", '"Who flies the " & [Focus Model]', None, f)

    f = "3 Sea"
    m.add("Focus Port", "VAR p = SELECTEDVALUE(PortPick[locode])\nRETURN IF(ISBLANK(p), \"DEHAM\", p)", None, f)
    calls_filter = f"{FP}[is_seagoing] = TRUE(), NOT ISBLANK({FP}[locode])"
    m.add("Calls", f"CALCULATE(COUNTROWS({FP}), {calls_filter})", "#,##0", f)
    m.add("Dwell Median", f"CALCULATE(MEDIAN({FP}[duration_h]), {calls_filter}, {FP}[is_truncated] = FALSE())", "0.0", f)
    m.add("Long Stays", f"CALCULATE([Calls], {FP}[duration_h] >= 48)", "#,##0", f)
    m.add("Port Ships", f"CALCULATE(DISTINCTCOUNT({FP}[mmsi]), {calls_filter})", "#,##0", f)
    for nm in ("Calls", "Dwell Median", "Long Stays", "Port Ships"):
        m.add(f"Focus {nm}", f"CALCULATE([{nm}], TREATAS({{[Focus Port]}}, {DP}[locode]))",
              "0.0" if nm == "Dwell Median" else "#,##0", f)
    m.add("All Ports Dwell Median", f"CALCULATE([Dwell Median], REMOVEFILTERS({DP}))", "0.0", f)
    m.add("Focus Port Name", f"VAR p = [Focus Port]\nRETURN CALCULATE(MAX({DP}[port_name]), REMOVEFILTERS({DP}), {DP}[locode] = p)", None, f)
    m.add("Focus Port Sub",
          f'VAR p = [Focus Port]\nRETURN p & "  ·  " & CALCULATE(MAX({DP}[country]), REMOVEFILTERS({DP}), {DP}[locode] = p) & "  ·  " & '
          + txt("[Focus Calls]") + ' & " seagoing calls by " & ' + txt("[Focus Port Ships]") + ' & " ships"', None, f)
    m.add("Focus Dwell Text", 'IF(ISBLANK([Focus Dwell Median]), "–", FORMAT([Focus Dwell Median], "0.0") & " h")', None, f)
    m.add("Focus Dwell Compare",
          'VAR d = [Focus Dwell Median]\nVAR a = [All Ports Dwell Median]\n'
          'RETURN IF(ISBLANK(d), "", "median stay  ·  " & FORMAT(DIVIDE(d, a) - 1, "+0%;-0%") & " vs " & FORMAT(a, "0.0") & " h all ports")', None, f)
    m.add("Focus Long Text", txt("[Focus Long Stays] + 0"), None, f)
    m.add("Focus Long Sub", '"stays of 48 h or more  ·  " & FORMAT(DIVIDE([Focus Long Stays] + 0, [Focus Calls]), "0%")', None, f)
    m.add("Focus Ships Text", txt("[Focus Port Ships]"), None, f)
    m.add("Focus Calls per Ship Sub", '"ships  ·  " & FORMAT(DIVIDE([Focus Calls], [Focus Port Ships]), "0.00") & " calls each"', None, f)
    m.add("Focus Calls Day Text", txt(f"DIVIDE([Focus Calls], [Days With Data])", "#,##0.0"), None, f)
    m.add("Focus Calls Day Sub", '"calls a day  ·  #" & [Focus Port Rank] & " of " & ' + txt("[Ports With Calls]") + ' & " ports"', None, f)
    m.add("Focus Port Rank",
          f"VAR p = [Focus Port]\nVAR c = CALCULATE([Calls], REMOVEFILTERS({DP}), {DP}[locode] = p)\n"
          f"RETURN IF(ISBLANK(c), BLANK(), COUNTROWS(FILTER(ALL({DP}[locode]), [Calls] > c)) + 1)", "0", f)
    m.add("Ports With Calls", f"COUNTROWS(FILTER(ALL({DP}[locode]), NOT ISBLANK([Calls])))", "#,##0", f)
    m.add("Band Color", f'IF(SELECTEDVALUE({FP}[dwell_band]) = "48 h +", "{ALERT}", "{SEA}")', None, f)
    m.add("Top Ports",
          f"VAR N = SELECTEDVALUE({DP}[port_label])\nVAR V = [Calls]\n"
          f"RETURN IF(ISBLANK(N) || ISBLANK(V), BLANK(),\n"
          f"    VAR R = RANKX(FILTER(ALLSELECTED({DP}[port_label]), NOT ISBLANK([Calls])), [Calls], V, DESC)\n"
          "    RETURN IF(R <= 5, V))", "#,##0", f)
    m.add("Port Bar Color", f'IF(SELECTEDVALUE({DP}[locode]) = [Focus Port], "{DARK}", "{SEA}")', None, f)
    m.add("Top Stays h",
          f"// 12 longest seagoing stays at the focus port (a stay that touches the data edges is a lower bound)\n"
          f"VAR V = CALCULATE(SUM({FP}[duration_h]), {calls_filter}, TREATAS({{[Focus Port]}}, {DP}[locode]))\n"
          f"VAR Longer = CALCULATE(COUNTROWS(FILTER({FP}, {FP}[duration_h] > V)), ALLSELECTED(), {calls_filter}, TREATAS({{[Focus Port]}}, {DP}[locode]))\n"
          "RETURN IF(ISBLANK(V), BLANK(), IF(Longer + 0 < 12, V))", "#,##0.0", f)
    m.add("Top Ship Stay h",
          f"// each ship's longest single seagoing stay at the focus port; the 10 longest ships are kept\n"
          f"VAR N = SELECTEDVALUE(DimShip[ship_label])\n"
          f"VAR V = CALCULATE(MAX({FP}[duration_h]), {calls_filter}, TREATAS({{[Focus Port]}}, {DP}[locode]))\n"
          "RETURN IF(ISBLANK(N) || ISBLANK(V), BLANK(),\n"
          f"    VAR R = RANKX(FILTER(ALLSELECTED(DimShip[ship_label]), NOT ISBLANK(CALCULATE(MAX({FP}[duration_h]), {calls_filter}, TREATAS({{[Focus Port]}}, {DP}[locode])))),\n"
          f"        CALCULATE(MAX({FP}[duration_h]), {calls_filter}, TREATAS({{[Focus Port]}}, {DP}[locode])), V, DESC)\n"
          "    RETURN IF(R <= 10, V))", "#,##0", f)

    f = "4 Quality"
    m.add("Bronze Rows", f"SUM({FQ}[bronze_rows])", "#,##0", f)
    m.add("Silver Rows", f"SUM({FQ}[silver_rows])", "#,##0", f)
    m.add("Dropped Rows", f"SUM({FQ}[dropped_rows])", "#,##0", f)
    m.add("Dropped Share", "DIVIDE([Dropped Rows], [Bronze Rows])", "0.00%", f)
    m.add("Stale Share", f'DIVIDE(CALCULATE(SUM({FQ}[flag_stale]), {FQ}[dataset] = "Aircraft states"), CALCULATE([Silver Rows], {FQ}[dataset] = "Aircraft states"))', "0.0%", f)
    m.add("Aircraft Rows", f'CALCULATE([Bronze Rows], {FQ}[dataset] = "Aircraft states")', "#,##0", f)
    m.add("Ship Rows", f'CALCULATE([Bronze Rows], {FQ}[dataset] = "Ship positions")', "#,##0", f)
    m.add("Bronze Text", txt("[Bronze Rows]"), None, f)
    m.add("Silver Text", txt("[Silver Rows]"), None, f)
    m.add("Dropped Text", 'FORMAT([Dropped Share], "0.00%")', None, f)
    m.add("Stale Text", 'FORMAT([Stale Share], "0.0%")', None, f)
    m.add("Bronze Sub", '"raw rows landed  ·  " & [Days With Data] & " days, 3 sources"', None, f)
    m.add("Silver Sub", '"typed rows kept for gold"', None, f)
    m.add("Dropped Sub", txt("[Dropped Rows]") + ' & " rows failed a drop rule"', None, f)
    m.add("Stale Sub", '"aircraft fixes > 120 s old: kept, flagged"', None, f)
    return m


def write_model():
    tables = MODEL + "tables/"
    os.makedirs(tables, exist_ok=True)
    for fn in os.listdir(tables):
        os.remove(tables + fn)

    def w(path, text):
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)

    for t in ORDER[:-1]:
        w(tables + f"{t}.tmdl", tmdl_table(t))
    m = build_measures()
    w(tables + f"{MEAS}.tmdl", m.tmdl())
    w(MODEL + "relationships.tmdl", "".join(
        f"relationship {guid(f'rel/{a}.{b}')}\n\tfromColumn: {a}.{b}\n\ttoColumn: {c}.{d}\n\n" for a, b, c, d in RELS))
    expr = ""
    for pname, val in (("DatabricksHost", HOST), ("DatabricksHttpPath", HTTP_PATH)):
        expr += (f'expression {pname} = "{val}" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]\n'
                 f"\tlineageTag: {guid('e/' + pname)}\n\n\tannotation PBI_ResultType = Text\n\n")
    w(MODEL + "expressions.tmdl", expr)
    qorder = ["DatabricksHost", "DatabricksHttpPath"] + ORDER
    model = ("model Model\n\tculture: en-GB\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: en-GB\n"
             "\tvalueFilterBehavior: independent\n\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\n"
             "annotation __PBI_TimeIntelligenceEnabled = 0\n\n"
             "annotation PBI_QueryOrder = [" + ",".join('"' + t + '"' for t in qorder) + "]\n\n"
             'annotation PBI_ProTooling = ["DevMode"]\n\n'
             + "".join(f"ref table {tq(t)}\n" for t in ORDER) + "\nref cultureInfo en-GB\n\n")
    w(MODEL + "model.tmdl", model)
    os.makedirs(MODEL + "cultures", exist_ok=True)
    w(MODEL + "cultures/en-GB.tmdl", "cultureInfo en-GB\n\n")
    w(MODEL + "database.tmdl", "database\n\tcompatibilityLevel: 1606\n\n")
    pos = {"DimDate": (520, 0), "DimAircraftType": (0, 0), "DimAircraft": (0, 380), "FactAircraftDaily": (260, 380),
           "FactTrafficHourly": (520, 380), "FactQuality": (780, 0), "AircraftPhoto": (260, 0), "FactShipDaily": (780, 380), "FactPortCalls": (1040, 380),
           "DimShip": (1040, 760), "DimPort": (1300, 380), "PortPick": (1300, 0), MEAS: (1040, 0)}
    nodes = [{"location": {"x": pos[t][0], "y": pos[t][1]}, "nodeIndex": t, "nodeLineageTag": guid("t/" + t),
              "size": {"height": 300, "width": 234}, "zIndex": 0} for t in ORDER]
    dl = {"version": "1.1.0", "diagrams": [{"ordinal": 0, "scrollPosition": {"x": 0, "y": 0}, "nodes": nodes,
                                            "name": "All tables", "zoomValue": 100, "pinKeyFieldsToTop": False,
                                            "showExtraHeaderInfo": False, "hideKeyFieldsWhenCollapsed": False,
                                            "tablesLocked": False}], "selectedDiagram": "All tables", "defaultDiagram": "All tables"}
    json.dump(dl, open(SM + "diagramLayout.json", "w", encoding="utf-8", newline="\n"), indent=2)
    cache = SM + ".pbi/cache.abf"
    if os.path.exists(cache) and os.environ.get("GEN_KEEP_CACHE") != "1":
        os.remove(cache)
    return len(m.items)


# ============================================================== scaffold (copied from a working PBIP)
def scaffold():
    def w(path, obj):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(obj, fh, indent=2, ensure_ascii=False)

    w(ROOT + NAME + ".pbip", {"version": "1.0", "artifacts": [{"report": {"path": NAME + ".Report"}}],
                               "settings": {"enableAutoRecovery": True}})
    w(REPORT + "definition.pbir", {"version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    plat = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    w(REPORT + ".platform", {"$schema": plat, "metadata": {"type": "Report", "displayName": NAME},
                             "config": {"version": "2.0", "logicalId": guid("logical/report")}})
    w(SM + ".platform", {"$schema": plat, "metadata": {"type": "SemanticModel", "displayName": NAME},
                         "config": {"version": "2.0", "logicalId": guid("logical/model")}})
    w(SM + "definition.pbism", {"version": "4.2", "settings": {}})
    for rel in ("definition/version.json", "StaticResources/SharedResources/BaseThemes/Fluent2-CY26SU09.json"):
        os.makedirs(os.path.dirname(REPORT + rel), exist_ok=True)
        shutil.copyfile(TEMPLATE + ".Report/" + rel, REPORT + rel)
    os.makedirs(REPORT + "definition/pages", exist_ok=True)


def write_report_json(images):
    rj = json.load(open(TEMPLATE + ".Report/definition/report.json", encoding="utf-8"))
    rj["resourcePackages"] = [p for p in rj["resourcePackages"] if p["type"] != "RegisteredResources"]
    rj["resourcePackages"].append({"name": "RegisteredResources", "type": "RegisteredResources",
                                   "items": [{"name": n, "path": n, "type": "Image"} for n in images]})
    with open(REPORT + "definition/report.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(rj, fh, indent=2, ensure_ascii=False)


# ============================================================== report JSON helpers
VC_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"


def lit(v):
    return {"expr": {"Literal": {"Value": v}}}


def S(s):
    return lit("'" + s.replace("'", "''") + "'")


def B(b):
    return lit("true" if b else "false")


def D(n):
    return lit(f"{n}D")


def clr(h):
    return {"solid": {"color": lit(f"'{h}'")}}


def col(entity, prop):
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def mea(prop):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": MEAS}}, "Property": prop}}


def mclr(prop):
    return {"solid": {"color": {"expr": mea(prop)}}}


def cproj(entity, prop, active=True, display=None):
    p = {"field": col(entity, prop), "queryRef": f"{entity}.{prop}", "nativeQueryRef": prop}
    if display:
        p["displayName"] = display
    if active:
        p["active"] = True
    return p


def mproj(prop, display=None):
    p = {"field": mea(prop), "queryRef": f"{MEAS}.{prop}", "nativeQueryRef": prop}
    if display:
        p["displayName"] = display
    return p


def vcobj(title=None, sub=None):
    """Transparent container: the card itself is drawn in the page background."""
    d = {"background": [{"properties": {"show": B(False)}}], "border": [{"properties": {"show": B(False)}}],
         "dropShadow": [{"properties": {"show": B(False)}}]}
    if title:
        d["title"] = [{"properties": {"show": B(True), "text": S(title), "fontColor": clr(INK1), "alignment": S("left"),
                                      "bold": B(False), "fontSize": D(13), "fontFamily": S(FONT_H)}}]
    else:
        d["title"] = [{"properties": {"show": B(False)}}]
    if title and not sub:
        d["subTitle"] = [{"properties": {"show": B(False)}}]
    if sub:
        d["subTitle"] = [{"properties": {"show": B(True), "text": S(sub), "fontColor": clr(INK3), "alignment": S("left"),
                                         "fontSize": D(9), "fontFamily": S(FONT_B)}}]
    d["padding"] = [{"properties": {"top": D(4), "bottom": D(4), "left": D(4), "right": D(4)}}]
    return d


class Page:
    def __init__(self, pid, name, bg):
        self.pid, self.name, self.bg = pid, name, bg
        self.key = bg[3:-4]  # bg_<key>.png
        self.visuals = []
        self.z = 0

    def add(self, key, x, y, w, h, visual, extra=None):
        z = self.z
        self.z += 1000
        d = {"$schema": VC_SCHEMA, "name": hid(f"{self.pid}/{key}"),
             "position": {"x": x, "y": y, "z": z, "height": h, "width": w, "tabOrder": z}, "visual": visual}
        if extra:
            d.update(extra)
        self.visuals.append(d)
        return d


def T(v, sz, c=INK1, fam=FONT_B, bold=False):
    return (v, fam, sz, c, bold)


def textbox(page, key, x, y, w, h, paragraphs):
    paras = [{"textRuns": [{"value": v, "textStyle": {**({"fontWeight": "bold"} if bold else {}),
                                                     "fontFamily": fam, "fontSize": f"{sz}pt", "color": c}}
                           for (v, fam, sz, c, bold) in runs]} for runs in paragraphs]
    vc = vcobj()
    vc["padding"] = [{"properties": {"top": D(0), "bottom": D(0), "left": D(0), "right": D(0)}}]
    page.add(key, x, y, w, h, {"visualType": "textbox", "objects": {"general": [{"properties": {"paragraphs": paras}}]},
                               "visualContainerObjects": vc, "drillFilterOtherVisuals": True})


def card(page, key, x, y, w, h, measure, size, color=INK1, font=FONT_H):
    lab = {"color": clr(color), "fontFamily": S(font), "fontSize": D(size), "labelDisplayUnits": D(1)}
    vc = vcobj()
    vc["padding"] = [{"properties": {"top": D(0), "bottom": D(0), "left": D(0), "right": D(0)}}]
    page.add(key, x, y, w, h, {"visualType": "card",
                               "query": {"queryState": {"Values": {"projections": [mproj(measure)]}}},
                               "objects": {"categoryLabels": [{"properties": {"show": B(False)}}], "labels": [{"properties": lab}]},
                               "visualContainerObjects": vc, "drillFilterOtherVisuals": True})


def table_objs(size=10, font=FONT_B, color=INK1, row_pad=2, header=False, image=None):
    hide = {"fontColor": clr(CARD), "backColor": clr(CARD), "fontSize": D(1)} if not header else \
        {"fontColor": clr(INK3), "backColor": clr(CARD), "fontFamily": S(FONT_B), "fontSize": D(9), "bold": B(False)}
    if not header:
        hide["show"] = B(False)
    grid = {"gridHorizontal": B(header), "gridHorizontalColor": clr(LINE), "gridVertical": B(False),
            "outlineColor": clr(CARD), "rowPadding": D(row_pad), "textSize": D(size)}
    if image:
        grid["imageHeight"] = D(image[1])
        grid["imageWidth"] = D(image[0])
    return {"grid": [{"properties": grid}],
            "values": [{"properties": {"backColorPrimary": clr(CARD), "backColorSecondary": clr(CARD), "wordWrap": B(False),
                                       "fontColorPrimary": clr(color), "fontColorSecondary": clr(color),
                                       "fontFamily": S(font), "fontSize": D(size)}}],
            "columnHeaders": [{"properties": hide}],
            "total": [{"properties": {"totals": B(False)}}]}


def textline(page, key, x, y, w, h, measure, size, color=INK1, font=FONT_B, bg=CARD):
    """Left-aligned dynamic text: a multi-row card (a classic card centres its value; a one-cell table clips with a fade)."""
    vc = vcobj()
    vc["padding"] = [{"properties": {"top": D(0), "bottom": D(0), "left": D(0), "right": D(0)}}]
    x, w = x - 14, w + 14  # the new card keeps ~14 px inner padding: shift so text lines up with the labels
    dflt = {"id": "default"}
    off = lambda: [{"properties": {"show": B(False)}, "selector": dflt}]  # noqa: E731
    page.add(key, x, y, w, h, {"visualType": "cardVisual",
                               "query": {"queryState": {"Data": {"projections": [mproj(measure)]}}},
                               "objects": {"value": [{"properties": {"fontColor": clr(color), "fontSize": D(size), "fontFamily": S(font),
                                                                     "bold": B(False), "horizontalAlignment": S("left"), "textWrap": B(True)},
                                                      "selector": dflt}],
                                           "label": off(), "accentBar": off(), "outline": off(), "shadowCustom": off(),
                                           "glowCustom": off(), "divider": off(),
                                           "fillCustom": [{"properties": {"show": B(True), "fillColor": clr(bg), "transparency": D(0)}, "selector": dflt}],
                                           "layout": [{"properties": {"alignment": S("left"), "paddingUniform": D(0), "paddingIndividual": B(False),
                                                                      "backgroundShow": B(False)}, "selector": dflt}],
                                           "padding": [{"properties": {"paddingUniform": D(0), "paddingSelection": S("Uniform")}, "selector": dflt}],
                                           "cardCalloutArea": [{"properties": {"paddingUniform": D(0), "paddingIndividual": B(False), "backgroundFillColor": clr(bg)}, "selector": dflt}],
                                           "spacing": [{"properties": {"verticalSpacing": D(0)}, "selector": dflt}]},
                               "visualContainerObjects": vc, "drillFilterOtherVisuals": True})


def svg_image(page, key, x, y, w, h, measure, img_w, img_h):
    page.add(key, x, y, w, h, {"visualType": "tableEx",
                               "query": {"queryState": {"Values": {"projections": [mproj(measure, " ")]}}},
                               "objects": table_objs(image=(img_w, img_h)),
                               "visualContainerObjects": vcobj(), "drillFilterOtherVisuals": True})


def axis(size=9, color=INK3):
    return {"labelColor": clr(color), "fontFamily": S(FONT_B), "fontSize": D(size)}


def value_axis(show=True):
    return [{"properties": {"show": B(show), "gridlineShow": B(show), "gridlineColor": clr(LINE), "gridlineTransparency": D(0),
                            "showAxisTitle": B(False), "labelDisplayUnits": D(1), "labelPrecision": lit("0D"), **axis()}}]


def cat_axis(**extra):
    return [{"properties": {"showAxisTitle": B(False), "gridlineShow": B(False), **axis(), **extra}}]


def legend(show=True):
    if not show:
        return [{"properties": {"show": B(False)}}]
    return [{"properties": {"show": B(True), "position": S("Top"), "labelColor": clr(INK2), "fontFamily": S(FONT_B), "fontSize": D(9)}}]


def data_labels(size=9):
    return [{"properties": {"show": B(True), "color": clr(INK2), "fontFamily": S(FONT_B), "fontSize": D(size), "labelDisplayUnits": D(1)}}]


def measure_colors(pairs):
    return [{"properties": {"fill": clr(pairs[0][1])}}] + [
        {"properties": {"fill": clr(hx)}, "selector": {"metadata": f"{MEAS}.{p}"}} for p, hx in pairs]


def measure_fill(color_measure):
    return [{"properties": {"fill": mclr(color_measure)}, "selector": {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}}]


def chart(page, key, x, y, w, h, vtype, query, objects, title=None, sub=None):
    page.add(key, x, y, w, h, {"visualType": vtype, "query": query, "objects": objects,
                               "visualContainerObjects": vcobj(title, sub), "drillFilterOtherVisuals": True})


def sorted_q(cat_entity, cat_prop, cat_display, measures, sort_field, direction):
    return {"queryState": {"Category": {"projections": [cproj(cat_entity, cat_prop, display=cat_display)]},
                           "Y": {"projections": [mproj(mm, dn) for mm, dn in measures]}},
            "sortDefinition": {"sort": [{"field": sort_field, "direction": direction}], "isDefaultSort": True}}


def pills(page, key, x, y, w, h, entity, prop):
    vis = {"visualType": "slicer",
           "query": {"queryState": {"Values": {"projections": [cproj(entity, prop)]}}},
           "objects": {"data": [{"properties": {"mode": S("Tile")}}],
                       "general": [{"properties": {"orientation": D(1)}}],
                       "header": [{"properties": {"show": B(False)}}],
                       "items": [{"properties": {"fontColor": clr(INK1), "fontFamily": S(FONT_B), "textSize": D(9),
                                                 "background": clr(CARD), "outlineStyle": D(0)}}],
                       "pendingChangesIcon": [{"properties": {"color": clr(INK1)}}]},
           "visualContainerObjects": vcobj(), "drillFilterOtherVisuals": True}
    page.add(key, x, y, w, h, vis)


def dropdown(page, key, x, y, w, h, entity, prop, header, default=None):
    gen = {"selfFilterEnabled": B(True)}
    if default:
        gen["filter"] = {"filter": {"Version": 2, "From": [{"Name": "p", "Entity": entity, "Type": 0}],
                                    "Where": [{"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "p"}}, "Property": prop}}],
                                                                    "Values": [[{"Literal": {"Value": "'" + default.replace("'", "''") + "'"}}]]}}}]}}
    vis = {"visualType": "slicer",
           "query": {"queryState": {"Values": {"projections": [cproj(entity, prop)]}}},
           "objects": {"data": [{"properties": {"mode": S("Dropdown")}}],
                       "header": [{"properties": {"show": B(True), "text": S(header), "bold": B(False), "fontColor": clr(INK3),
                                                  "textSize": D(8), "fontFamily": S(FONT_B)}}],
                       "items": [{"properties": {"fontColor": clr(INK1), "fontFamily": S(FONT_B), "textSize": D(10), "background": clr(CARD)}}],
                       "general": [{"properties": gen}],
                       "selection": [{"properties": {"strictSingleSelect": B(False), "selectAllCheckboxEnabled": B(False), "singleSelect": B(True)}}]},
           "visualContainerObjects": vcobj(), "drillFilterOtherVisuals": True}
    page.add(key, x, y, w, h, vis)


def date_slicer(page):
    vis = {"visualType": "slicer",
           "query": {"queryState": {"Values": {"projections": [cproj("DimDate", "date")]}},
                     "sortDefinition": {"sort": [{"field": col("DimDate", "date"), "direction": "Ascending"}], "isDefaultSort": True}},
           "objects": {"data": [{"properties": {"mode": S("Between")}}],
                       "header": [{"properties": {"show": B(True), "text": S("DATE RANGE (UTC)"), "bold": B(False), "fontColor": clr(INK3),
                                                  "textSize": D(8), "fontFamily": S(FONT_B)}}],
                       "date": [{"properties": {"fontColor": clr(INK1), "background": clr(CARD), "textSize": D(9)}}],
                       "slider": [{"properties": {"color": clr(DARK)}}],
                       "pendingChangesIcon": [{"properties": {"color": clr(INK1)}}]},
           "visualContainerObjects": vcobj(), "drillFilterOtherVisuals": True}
    vis["syncGroup"] = {"groupName": "Date", "fieldChanges": True, "filterChanges": True}  # inside "visual", not at the root
    page.add("filter/date", 1000, 12, 256, 76, vis)


# ============================================================== layout + backgrounds
PAGES = [("pulse", "Pulse"), ("aircraft", "Aircraft in focus"), ("port", "Port dwell"), ("quality", "Data quality")]
PIDS = {k: hid("page/" + k) for k, _ in PAGES}
RAIL_Y = [132, 188, 244, 300]
CARDS = {
    "pulse": [(96, 100, 572, 300), (684, 100, 572, 300), (96, 416, 760, 280), (872, 416, 384, 280)],
    "aircraft": [(96, 140, 760, 468), (872, 100, 384, 744), (96, 624, 760, 220)],
    "port": [(96, 140, 760, 330), (872, 100, 384, 596), (96, 486, 760, 210)],
    "quality": [(96 + i * 296, 100, 280, 130) for i in range(4)] + [(96, 246, 760, 450), (872, 246, 384, 450)],
}


def hexrgb(h, a=255):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4)) + (a,)


def draw_icon(d, kind, cx, cy, color):
    s = SCALE
    if kind == "pulse":
        pts = [(-11, 2), (-5, 2), (-2, -7), (2, 9), (5, -2), (11, -2)]
        d.line([(cx + x * s, cy + y * s) for x, y in pts], fill=color, width=2 * s, joint="curve")
    elif kind == "aircraft":
        body = [(0, -11), (1.6, -8), (1.6, -2), (11, 3), (11, 5), (1.6, 2.5), (1.4, 7), (4.5, 9.5), (4.5, 11), (0, 9.8),
                (-4.5, 11), (-4.5, 9.5), (-1.4, 7), (-1.6, 2.5), (-11, 5), (-11, 3), (-1.6, -2), (-1.6, -8)]
        d.polygon([(cx + x * s, cy + y * s) for x, y in body], fill=color)
    elif kind == "port":
        hull = [(-11, 1), (11, 1), (8, 8), (-8, 8)]
        d.polygon([(cx + x * s, cy + y * s) for x, y in hull], fill=color)
        d.rectangle([cx - 5 * s, cy - 6 * s, cx + 3 * s, cy + 1 * s], fill=color)
        d.rectangle([cx - 2 * s, cy - 11 * s, cx + 0 * s, cy - 6 * s], fill=color)
    elif kind == "quality":
        d.ellipse([cx - 10 * s, cy - 10 * s, cx + 10 * s, cy + 10 * s], outline=color, width=2 * s)
        d.line([(cx - 5 * s, cy), (cx - 1 * s, cy + 4 * s), (cx + 6 * s, cy - 4 * s)], fill=color, width=2 * s, joint="curve")


def make_background(key, path):
    s = SCALE
    img = Image.new("RGBA", (W * s, PAGE_H.get(key, H) * s), hexrgb(CANVAS))
    shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    for x, y, w, h in CARDS[key]:
        sd.rounded_rectangle([x * s, (y + 4) * s, (x + w) * s, (y + h + 4) * s], radius=24 * s, fill=(17, 20, 24, 26))
    shadow = shadow.filter(ImageFilter.GaussianBlur(10 * s))
    img = Image.alpha_composite(img, shadow)
    d = ImageDraw.Draw(img)
    for x, y, w, h in CARDS[key]:
        d.rounded_rectangle([x * s, y * s, (x + w) * s, (y + h) * s], radius=24 * s, fill=hexrgb(CARD), outline=hexrgb(LINE), width=1)
    # icon rail
    d.rounded_rectangle([18 * s, 22 * s, 54 * s, 58 * s], radius=10 * s, fill=hexrgb(DARK))
    draw_icon(d, "aircraft", 36 * s, 40 * s, hexrgb("#FFFFFF"))
    d.line([(24 * s, 100 * s), (48 * s, 100 * s)], fill=hexrgb(LINE), width=s)
    for (k, _), cy in zip(PAGES, RAIL_Y):
        on = k == key
        d.ellipse([16 * s, (cy - 20) * s, 56 * s, (cy + 20) * s], fill=hexrgb(DARK if on else CARD),
                  outline=None if on else hexrgb(LINE), width=s)
        draw_icon(d, k, 36 * s, cy * s, hexrgb("#FFFFFF" if on else INK1))
    img.convert("RGB").save(path, optimize=True)


def page_chrome(P, key, title, sub_measure=None, sub_text=None):
    textbox(P, "head/title", 96, 22, 520, 46, [[T(title, 24, INK1, FONT_H)]])
    if sub_measure:
        textline(P, "head/sub", 96, 64, 760, 28, sub_measure, 10, INK3, bg=CANVAS)
    elif sub_text:
        textbox(P, "head/sub", 96, 66, 760, 24, [[T(sub_text, 10, INK3)]])
    # invisible buttons over the rail icons drawn in the background
    for (k, label), cy in zip(PAGES, RAIL_Y):
        if k == key:
            continue
        objs = {"icon": [{"properties": {"shapeType": S("blank")}, "selector": {"id": "default"}}],
                "fill": [{"properties": {"show": B(False)}}],
                "outline": [{"properties": {"show": B(False)}, "selector": {"id": "default"}}],
                "text": [{"properties": {"show": B(False)}}]}
        vco = {"background": [{"properties": {"show": B(False)}}], "border": [{"properties": {"show": B(False)}}],
               "dropShadow": [{"properties": {"show": B(False)}}], "title": [{"properties": {"show": B(False)}}],
               "visualLink": [{"properties": {"show": B(True), "type": S("PageNavigation"), "navigationSection": S(PIDS[k])}}],
               "general": [{"properties": {"altText": S(f"Go to {label}")}}]}
        P.add(f"nav/{k}", 16, cy - 20, 40, 40, {"visualType": "actionButton", "objects": objs, "visualContainerObjects": vco,
                                                "drillFilterOtherVisuals": True}, extra={"howCreated": "InsertVisualButton"})


def label(P, key, x, y, w, text, size=8, color=INK3):
    textbox(P, key, x, y, w, 22, [[T(text, size, color, FONT_B)]])


# ============================================================== pages
def page_pulse():
    P = Page(PIDS["pulse"], "Pulse", "bg_pulse.png")
    page_chrome(P, "pulse", "Pulse", sub_measure="Period Text")
    for i, (nm, title, hero, sub, base, color) in enumerate((
            ("air", "AIRCRAFT AIRBORNE · AVERAGE HOUR", "Air Hero Text", "Air Hero Sub", "Airborne per Hour", AIR),
            ("sea", "SHIPS UNDER WAY · AVERAGE HOUR", "Sea Hero Text", "Sea Hero Sub", "Ships Moving per Hour", SEA))):
        x = 96 + i * 588
        label(P, f"{nm}/label", x + 24, 118, 400, title)
        textline(P, f"{nm}/hero", x + 20, 134, 400, 70, hero, 40, INK1, FONT_L)
        textline(P, f"{nm}/sub", x + 24, 206, 530, 28, sub, 10, INK2)
        q = sorted_q("FactTrafficHourly", "hour_of_day", "Hour (UTC)", [(base, "Average")], col("FactTrafficHourly", "hour_of_day"), "Ascending")
        objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(axisType=S("Categorical")), "legend": legend(False),
                "dataPoint": measure_fill(f"{'Air' if nm == 'air' else 'Sea'} Hour Color")}
        chart(P, f"{nm}/hours", x + 16, 240, 540, 150, "clusteredColumnChart", q, objs)
    q = sorted_q("DimDate", "day_label", "Day", [("Aircraft Seen", "Aircraft"), ("Ships Seen", "Ships")], col("DimDate", "date_key"), "Ascending")
    objs = {"valueAxis": value_axis(), "categoryAxis": cat_axis(axisType=S("Categorical")), "legend": legend(True),
            "dataPoint": measure_colors([("Aircraft Seen", AIR), ("Ships Seen", SEA)])}
    chart(P, "daily", 112, 428, 728, 220, "clusteredColumnChart", q, objs, "Every day, different transponders",
          "Distinct aircraft and ships seen per UTC day")
    textline(P, "daily/note", 120, 656, 720, 28, "Busiest Day Text", 9, INK2)
    label(P, "week/label", 896, 434, 340, "WEEKEND VS WEEKDAY")
    for i, (nm, txt_m, sub_m, color) in enumerate((("air", "Weekend Air Text", "Weekend Air Sub", AIR),
                                                   ("sea", "Weekend Sea Text", "Weekend Sea Sub", SEA))):
        y = 462 + i * 112
        textbox(P, f"week/{nm}/name", 896, y, 200, 24, [[T("●  ", 10, color, FONT_B), T("Aircraft" if nm == "air" else "Ships", 11, INK1, FONT_H)]])
        card(P, f"week/{nm}/value", 1100, y - 6, 140, 52, txt_m, 26, INK1, FONT_L)
        textline(P, f"week/{nm}/sub", 896, y + 50, 350, 26, sub_m, 9, INK3)
    return P


def page_aircraft():
    P = Page(PIDS["aircraft"], "Aircraft in focus", "bg_aircraft.png")
    page_chrome(P, "aircraft", "Aircraft in focus", sub_text="Pick a class, then click a type on the right. Sizes: FAA Aircraft Characteristics DB · types: OpenSky registry")
    pills(P, "class", 96, 96, 760, 36, "DimAircraftType", "aircraft_class")
    textline(P, "focus/chip", 120, 156, 430, 22, "Focus Class Chip", 8, INK3)
    textline(P, "focus/model", 120, 176, 430, 56, "Focus Model", 17, INK1, FONT_H)
    textline(P, "focus/dims", 120, 234, 430, 30, "Focus Dims Text", 13, INK1, FONT_L)
    textline(P, "focus/sub", 120, 264, 430, 24, "Focus Sub Text", 9, INK3)
    svg_image(P, "focus/photo", 566, 148, 280, 168, "Focus Photo", 256, 144)
    textline(P, "focus/photo/credit", 578, 314, 266, 30, "Focus Photo Credit", 7, INK3)
    svg_image(P, "focus/silhouette", 112, 300, 476, 250, "Focus Silhouette", 440, 220)
    svg_image(P, "focus/bars", 596, 356, 256, 164, "Focus Spec Bars", 230, 138)
    textline(P, "focus/gate", 604, 520, 248, 24, "Focus Gate Text", 9, INK2)
    textline(P, "focus/seen", 604, 546, 248, 24, "Focus Seen Text", 9, INK2)
    textline(P, "focus/cruise", 604, 572, 248, 24, "Focus Cruise Text", 9, INK2)
    q = sorted_q("DimAircraftType", "model_name", "Type", [("Top Types", "Aircraft seen")], mea("Top Types"), "Descending")
    objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(fontSize=D(9)), "legend": legend(False),
            "labels": data_labels(), "dataPoint": measure_fill("Type Bar Color")}
    chart(P, "types", 888, 112, 352, 720, "clusteredBarChart", q, objs, "Seen in this period",
          "Distinct aircraft per type · click one to focus it")
    q = sorted_q("DimAircraft", "operator", "Operator", [("Focus Operators", "Aircraft")], mea("Focus Operators"), "Descending")
    objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(fontSize=D(9)), "legend": legend(False),
            "labels": data_labels(), "dataPoint": measure_colors([("Focus Operators", AIR)])}
    textline(P, "ops/title", 116, 636, 700, 28, "Operators Title", 12, INK1, FONT_H)
    chart(P, "ops", 112, 664, 728, 172, "clusteredBarChart", q, objs)
    return P


def page_port():
    P = Page(PIDS["port"], "Port dwell", "bg_port.png")
    page_chrome(P, "port", "Port dwell", sub_text="Seagoing ships (valid IMO) staying 60 min + within 20 km of a UN/LOCODE port · gaps-and-islands in Databricks")
    pills(P, "groups", 96, 96, 760, 36, "DimShip", "ship_group")
    label(P, "focus/chip", 120, 152, 300, "PORT IN FOCUS")
    textline(P, "focus/name", 120, 172, 470, 44, "Focus Port Name", 22, INK1, FONT_H)
    textline(P, "focus/sub", 120, 216, 470, 24, "Focus Port Sub", 9, INK3)
    textline(P, "focus/dwell", 604, 150, 244, 64, "Focus Dwell Text", 40, INK1, FONT_L)
    textline(P, "focus/dwell/sub", 604, 214, 244, 24, "Focus Dwell Compare", 8, INK2)
    q = sorted_q("FactPortCalls", "dwell_band", "Stay", [("Focus Calls", "Calls")], col("FactPortCalls", "band_order"), "Ascending")
    objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(axisType=S("Categorical"), fontSize=D(10)), "legend": legend(False),
            "labels": data_labels(10), "dataPoint": measure_fill("Band Color")}
    chart(P, "bands", 112, 244, 480, 218, "clusteredColumnChart", q, objs, "How long ships stay", "Seagoing calls by length of stay · 48 h + in orange")
    for i, (val, sub) in enumerate((("Focus Long Text", "Focus Long Sub"), ("Focus Ships Text", "Focus Calls per Ship Sub"),
                                    ("Focus Calls Day Text", "Focus Calls Day Sub"))):
        y = 248 + i * 72
        textline(P, f"kpi{i}/v", 604, y, 244, 40, val, 22, ALERT if i == 0 else INK1, FONT_L)
        textline(P, f"kpi{i}/s", 604, y + 38, 244, 24, sub, 8, INK3)
    dropdown(P, "pick", 888, 112, 352, 56, "PortPick", "port_pick", "PORT IN FOCUS (TYPE TO SEARCH)", "Hamburg · DEHAM")
    q = sorted_q("DimShip", "ship_label", "Ship", [("Top Ship Stay h", "Hours")], mea("Top Ship Stay h"), "Descending")
    objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(fontSize=D(9)), "legend": legend(False),
            "labels": data_labels(9), "dataPoint": measure_colors([("Top Ship Stay h", SEA)])}
    chart(P, "stays", 888, 184, 352, 500, "clusteredBarChart", q, objs, "Longest stays", "Longest single call per ship · hours at the focus port")
    q = sorted_q("DimPort", "port_label", "Port", [("Top Ports", "Seagoing calls")], mea("Top Ports"), "Descending")
    objs = {"valueAxis": value_axis(False), "categoryAxis": cat_axis(fontSize=D(9)), "legend": legend(False),
            "labels": data_labels(9), "dataPoint": measure_fill("Port Bar Color")}
    chart(P, "ports", 112, 498, 728, 190, "clusteredBarChart", q, objs, "Busiest ports", "Seagoing calls · focus port in black")
    return P


def page_quality():
    P = Page(PIDS["quality"], "Data quality", "bg_quality.png")
    page_chrome(P, "quality", "Data quality",
                sub_text="Volume → Auto Loader → bronze → silver (expectations) → gold star schema → SQL Warehouse → Power BI")
    for i, (lab, val, sub) in enumerate((("BRONZE ROWS", "Bronze Text", "Bronze Sub"), ("SILVER ROWS", "Silver Text", "Silver Sub"),
                                         ("DROPPED", "Dropped Text", "Dropped Sub"), ("STALE AIRCRAFT FIXES", "Stale Text", "Stale Sub"))):
        x = 96 + i * 296
        label(P, f"k{i}/label", x + 20, 114, 240, lab)
        textline(P, f"k{i}/value", x + 18, 132, 252, 56, val, 28, INK1, FONT_L)
        textline(P, f"k{i}/sub", x + 20, 192, 252, 26, sub, 8, INK3)
    q = sorted_q("DimDate", "day_label", "Day", [("Aircraft Rows", "Aircraft states"), ("Ship Rows", "Ship positions")],
                 col("DimDate", "date_key"), "Ascending")
    objs = {"valueAxis": value_axis(), "categoryAxis": cat_axis(axisType=S("Categorical")), "legend": legend(True),
            "dataPoint": measure_colors([("Aircraft Rows", AIR), ("Ship Rows", SEA)])}
    chart(P, "rows", 112, 258, 728, 420, "clusteredColumnChart", q, objs, "Rows landed per day",
          "Bronze row count per UTC file day · a dip = the Pi or a source was down")
    textbox(P, "notes", 892, 266, 344, 420, [
        [T("How the data gets here", 13, INK1, FONT_H)],
        [T(" ", 6, INK3)],
        [T("1  Land. ", 9, INK1, FONT_H), T("The Raspberry Pi collects OpenSky and AIS 24/7; daily .csv.gz files are uploaded to a Unity Catalog volume.", 9, INK2)],
        [T("2  Bronze. ", 9, INK1, FONT_H), T("Auto Loader streams only new files, every value kept as text.", 9, INK2)],
        [T("3  Silver. ", 9, INK1, FONT_H), T("Typed columns. Drop rules: bad ICAO/MMSI, no position, out of range, 0/0. Warn rules keep the row and flag it: stale fixes, impossible speeds.", 9, INK2)],
        [T("4  Gold. ", 9, INK1, FONT_H), T("Star schema plus port calls (gaps-and-islands), enriched with the OpenSky registry, ICAO Doc 8643 and FAA sizes.", 9, INK2)],
        [T("5  Serve. ", 9, INK1, FONT_H), T("A file-arrival Job reruns the pipeline; Power BI imports gold through the SQL Warehouse.", 9, INK2)],
        [T(" ", 6, INK3)],
        [T("Volunteer receivers, not official counts. Port calls match the local Polars build within 0.1 %.", 8, INK3)],
    ])
    return P


def write_report():
    pages = [page_pulse(), page_aircraft(), page_port(), page_quality()]
    res = REPORT + "StaticResources/RegisteredResources/"
    os.makedirs(res, exist_ok=True)
    for (k, _), P in zip(PAGES, pages):
        make_background(k, res + P.bg)
    write_report_json([P.bg for P in pages])
    pdir = REPORT + "definition/pages/"
    for d in os.listdir(pdir):
        if os.path.isdir(pdir + d):
            shutil.rmtree(pdir + d)
    for P in pages:
        date_slicer(P)
        os.makedirs(pdir + P.pid + "/visuals", exist_ok=True)
        item = {"expr": {"ResourcePackageItem": {"PackageName": "RegisteredResources", "PackageType": 1, "ItemName": P.bg}}}
        ph = PAGE_H.get(P.key, H)
        page_json = {"$schema": PAGE_SCHEMA, "name": P.pid, "displayName": P.name,
                     "displayOption": "FitToWidth" if ph > H else "FitToPage", "height": ph, "width": W,
                     "objects": {"background": [{"properties": {"image": {"image": {"name": S(P.bg), "url": item, "scaling": S("Fill")}},
                                                                "transparency": D(0)}}],
                                 "outspace": [{"properties": {"color": clr(WALL), "transparency": D(0)}}]}}
        json.dump(page_json, open(pdir + P.pid + "/page.json", "w", encoding="utf-8", newline="\n"), indent=2, ensure_ascii=False)
        for v in P.visuals:
            vd = pdir + P.pid + "/visuals/" + v["name"]
            os.makedirs(vd, exist_ok=True)
            json.dump(v, open(vd + "/visual.json", "w", encoding="utf-8", newline="\n"), indent=2, ensure_ascii=False)
    pj = {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
          "pageOrder": [P.pid for P in pages], "activePageName": PIDS["pulse"]}
    json.dump(pj, open(pdir + "pages.json", "w", encoding="utf-8", newline="\n"), indent=2)
    return pages


if __name__ == "__main__":
    if not os.path.exists(ROOT + NAME + ".pbip"):
        scaffold()
    n = write_model()
    pages = write_report()
    print("measures:", n, "| visuals:", {p.name: len(p.visuals) for p in pages})
