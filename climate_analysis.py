import csv, os, time
from typing import Dict, List, Optional, Tuple, Any

def safe_int(x: str) -> Optional[int]:
    try:
        if x is None: return None
        x = x.strip()
        if x == "" or x.lower() == "na": return None
        return int(float(x))
    except Exception:
        return None

def safe_float(x: str) -> Optional[float]:
    try:
        if x is None: return None
        x = x.strip().replace(",", "")
        if x == "" or x.lower() == "na": return None
        return float(x)
    except Exception:
        return None

def normalize_ws(s: str) -> str:
    return " ".join(s.split()) if isinstance(s, str) else s

def find_column(header: List[str], *candidates: str, contains_all: Tuple[str,...]=()) -> Optional[str]:
    header_l = [h.strip() for h in header]
    low = {h.lower(): h for h in header_l}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    if contains_all:
        for h in header_l:
            hl = h.lower()
            if all(part in hl for part in contains_all):
                return h
    return None

class Schema:
    def __init__(self, country, year, co2, temp_anomaly, gdp, extreme_events, deforestation):
        self.country = country
        self.year = year
        self.co2 = co2
        self.temp_anomaly = temp_anomaly
        self.gdp = gdp
        self.extreme_events = extreme_events
        self.deforestation = deforestation

def detect_schema(headers: List[str]) -> Schema:
    country = find_column(headers, "Country", "Entity")
    year = find_column(headers, "Year")
    co2 = find_column(headers, "CO2 Emissions", "CO2", contains_all=("co2",))
    temp_anomaly = find_column(headers, "Temperature Anomaly", "Temp Anomaly", contains_all=("temperature", "anomaly"))
    gdp = find_column(headers, "GDP", contains_all=("gdp",))
    extreme_events = find_column(headers, "Extreme Events", "No. of Extreme Events", contains_all=("extreme", "event"))
    deforestation = find_column(
        headers,
        "Deforestation_Rate",        
        "Deforestation Rate",
        "Deforestation",
        "Forest Loss",
        "Forest cover loss",
        "Forest Area Loss"
    )

    return Schema(country, year, co2, temp_anomaly, gdp, extreme_events, deforestation)

class ClimateData:
    def __init__(self, rows: List[Dict[str,Any]], schema: Schema):
        self.rows = rows
        self.schema = schema
        self.by_country: Dict[str, List[Dict[str,Any]]] = {}
        self.by_year: Dict[int, List[Dict[str,Any]]] = {}
        self._build_indexes()

    def _build_indexes(self):
        ccol, ycol = self.schema.country, self.schema.year
        for r in self.rows:
            c = normalize_ws(r.get(ccol,"")) or ""
            y = safe_int(str(r.get(ycol)))
            if c: self.by_country.setdefault(c.lower(),[]).append(r)
            if y is not None: self.by_year.setdefault(y,[]).append(r)

    @staticmethod
    def from_csv(path: str):
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            schema = detect_schema(rdr.fieldnames or [])
            rows=[]
            for raw in rdr:
                row={k:v for k,v in raw.items()}
                if schema.year in row: row[schema.year]=safe_int(row[schema.year])
                if schema.co2 and schema.co2 in row: row[schema.co2]=safe_float(row[schema.co2])
                if schema.temp_anomaly and schema.temp_anomaly in row: row[schema.temp_anomaly]=safe_float(row[schema.temp_anomaly])
                if schema.gdp and schema.gdp in row: row[schema.gdp]=safe_float(row[schema.gdp])
                if schema.extreme_events and schema.extreme_events in row: row[schema.extreme_events]=safe_float(row[schema.extreme_events])
                if schema.deforestation and schema.deforestation in row: row[schema.deforestation]=safe_float(row[schema.deforestation])
                if schema.country in row and isinstance(row[schema.country],str):
                    row[schema.country]=normalize_ws(row[schema.country])
                rows.append(row)
        return ClimateData(rows,schema)


def print_table(rows: List[Tuple[Any,...]], headers: Tuple[str,...]):
    if not rows:
        print("No results.")
        return
    widths=[len(h) for h in headers]
    for r in rows:
        for i,cell in enumerate(r):
            widths[i]=max(widths[i],len(f"{cell}"))
    def fmt_row(cells): return " | ".join(f"{str(cells[i]):<{widths[i]}}" for i in range(len(headers)))
    print(fmt_row(headers))
    print("-+-".join("-"*w for w in widths))
    for r in rows: print(fmt_row(r))

def print_kv_table(items: List[Tuple[str,float]], key_header: str, val_header: str):
    rows=[(k, f"{v:.6g}") for k,v in items]
    print_table(rows,(key_header,val_header))

def search_by_country(data, name): 
    return data.by_country.get(name.strip().lower(),[])

def search_by_year_range(data, start, end):
    out=[]
    for y in range(start,end+1):
        out.extend(data.by_year.get(y,[]))
    return out

def find_extreme_events_max(data, year=None, top=10):
    col=data.schema.extreme_events; ccol=data.schema.country
    if not col: 
        raise ValueError("Extreme Events column not found.")
    agg={}
    rows=data.by_year.get(year,[]) if year else data.rows
    for r in rows:
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)):
            agg[c]=agg.get(c,0)+float(v)
    return sorted(agg.items(), key=lambda kv: kv[1], reverse=True)[:top]

def find_extreme_events_min(data, year=None, top=10):
    col=data.schema.extreme_events; ccol=data.schema.country
    if not col: 
        raise ValueError("Extreme Events column not found.")
    agg={}
    rows=data.by_year.get(year,[]) if year else data.rows
    for r in rows:
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)):
            agg[c]=agg.get(c,0)+float(v)
    return sorted(agg.items(), key=lambda kv: kv[1])[:top]

def find_high_co2(data, year, top=10):
    col=data.schema.co2; ccol=data.schema.country
    if not col: 
        raise ValueError("CO2 column not found.")
    out=[]
    for r in data.by_year.get(year,[]):
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)): out.append((c,float(v)))
    return sorted(out,key=lambda kv: kv[1],reverse=True)[:top]

def urban_deforestation(data, year, top=10, mode="max"):
    
    if not data.schema.deforestation:
        raise ValueError("Dataset has no deforestation column")
    col=data.schema.deforestation; ccol=data.schema.country
    out=[]
    for r in data.by_year.get(year,[]):
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)): out.append((c,float(v)))
    out.sort(key=lambda kv: kv[1], reverse=(mode=="max"))
    return out[:top]

def sort_by_temp_anomaly_asc(data, year, top=None):
    col=data.schema.temp_anomaly; ccol=data.schema.country
    if not col: 
        raise ValueError("Temperature Anomaly column not found.")
    buckets={}
    for r in data.by_year.get(year,[]):
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)): buckets.setdefault(c,[]).append(float(v))
    out=[(c,sum(vs)/len(vs)) for c,vs in buckets.items()]
    out.sort(key=lambda kv: kv[1])
    return out[:top] if top else out

def sort_by_temp_anomaly_desc(data, year, top=None):
    out=sort_by_temp_anomaly_asc(data,year)
    out.sort(key=lambda kv: kv[1],reverse=True)
    return out[:top] if top else out

def sort_by_gdp_asc(data, year, top=None):
    col=data.schema.gdp; ccol=data.schema.country
    if not col: 
        raise ValueError("GDP column not found.")
    buckets={}
    for r in data.by_year.get(year,[]):
        c=r.get(ccol); v=r.get(col)
        if c and isinstance(v,(int,float)): buckets.setdefault(c,[]).append(float(v))
    out=[(c,sum(vs)/len(vs)) for c,vs in buckets.items()]
    out.sort(key=lambda kv: kv[1])
    return out[:top] if top else out

def sort_by_gdp_desc(data, year, top=None):
    out=sort_by_gdp_asc(data,year)
    out.sort(key=lambda kv: kv[1],reverse=True)
    return out[:top] if top else out

def average_metrics(data, country):
    rows=search_by_country(data,country)
    if not rows: return []
    metrics={}
    for r in rows:
        for col in (data.schema.co2,data.schema.temp_anomaly,data.schema.gdp,data.schema.extreme_events,data.schema.deforestation):
            if col and isinstance(r.get(col),(int,float)):
                metrics.setdefault(col,[]).append(float(r[col]))
    return [(col,sum(vs)/len(vs)) for col,vs in metrics.items()]


def autodetect_csv_in_same_folder():
    folder=os.path.dirname(__file__)
    csvs=[f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    return os.path.join(folder,csvs[0]) if csvs else None

def interactive_menu():
    print("=== Climate Analysis (11 functions) ===")
    # csv_path = input("Path to dataset CSV (Enter if same folder): ").strip()
    csv_path = 'data.csv'
    if not csv_path:
        auto = autodetect_csv_in_same_folder()
        if not auto:
            print("No CSV found.")
            return
        csv_path = auto
        print(f"Using {csv_path}")
    data = ClimateData.from_csv(csv_path)

    while True:
        print("\nChoose option:")
        print("1) search_by_country")
        print("2) search_by_year_range")
        print("3) find_extreme_events_max")
        print("4) find_extreme_events_min")
        print("5) find_high_co2")
        print("6) urban_deforestation")
        print("7) sort_by_temp_anomaly_asc")
        print("8) sort_by_temp_anomaly_desc")
        print("9) sort_by_gdp_asc")
        print("10) sort_by_gdp_desc")
        print("11) average_metrics")
        print("0) Exit")
        choice = input("Choice: ").strip()
        if choice == "0":
            break

        try:
            if choice == "1":
                name = input("Country: ")
                t0 = time.time()
                rows = search_by_country(data, name)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                cols = [
                    c for c in [
                        data.schema.country,
                        data.schema.year,
                        data.schema.co2,
                        data.schema.temp_anomaly,
                        data.schema.gdp,
                        data.schema.extreme_events,
                        data.schema.deforestation
                    ] if c
                ]
                out = [tuple(r.get(c, "") for c in cols) for r in rows[:20]]
                print_table(out, tuple(cols))

            elif choice == "2":
                s = int(input("Start year: "))
                e = int(input("End year: "))
                t0 = time.time()
                rows = search_by_year_range(data, s, e)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print(f"Found {len(rows)} rows")

            elif choice == "3":
                y = input("Year(blank=all): ")
                yv = int(y) if y else None
                t0 = time.time()
                items = find_extreme_events_max(data, yv, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "Extreme Events Max")

            elif choice == "4":
                y = input("Year(blank=all): ")
                yv = int(y) if y else None
                t0 = time.time()
                items = find_extreme_events_min(data, yv, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "Extreme Events Min")

            elif choice == "5":
                y = int(input("Year: "))
                t0 = time.time()
                items = find_high_co2(data, y, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "CO2")

            elif choice == "6":
                y = int(input("Year: ").strip())
                top = input("Top N (blank=10): ").strip()
                topv = int(top) if top else 10
                mode = input("Mode (max/min, blank=max): ").strip().lower() or "max"
                if mode not in ("max", "min"):
                    print("Invalid mode; use 'max' or 'min'.")
                    continue
                t0 = time.time()
                items = urban_deforestation(data, year=y, top=topv, mode=mode)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                metric_name = data.schema.deforestation or "Deforestation"
                print_kv_table(items, "Country", f"{metric_name} ({mode}) in {y}")

            elif choice == "7":
                y = int(input("Year: "))
                t0 = time.time()
                items = sort_by_temp_anomaly_asc(data, y, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "Temp Anomaly ASC")

            elif choice == "8":
                y = int(input("Year: "))
                t0 = time.time()
                items = sort_by_temp_anomaly_desc(data, y, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "Temp Anomaly DESC")

            elif choice == "9":
                y = int(input("Year: "))
                t0 = time.time()
                items = sort_by_gdp_asc(data, y, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "GDP ASC")

            elif choice == "10":
                y = int(input("Year: "))
                t0 = time.time()
                items = sort_by_gdp_desc(data, y, 10)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_kv_table(items, "Country", "GDP DESC")

            elif choice == "11":
                name = input("Country: ")
                t0 = time.time()
                items = average_metrics(data, name)
                t1 = time.time()
                print(f"[Runtime] {t1 - t0:.3f}s")
                print_table([(k, f"{v:.6g}") for k, v in items], ("Metric", "Average"))

        except Exception as e:
            print("Error:", e)

if __name__=="__main__":
    interactive_menu()
