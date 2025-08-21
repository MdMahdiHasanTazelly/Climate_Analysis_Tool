import csv
import sys
from operator import itemgetter

def _snake(s: str) -> str:
    return (
        s.strip()
         .replace("/", " ")
         .replace("-", " ")
         .replace("%", "pct")
         .replace(".", " ")
         .replace("(", " ")
         .replace(")", " ")
         .replace(",", " ")
         .lower()
         .split()
    )

def to_snake_case(s: str) -> str:
    return "_".join(_snake(s))

HEADER_ALIASES = {
    "country": "country",
    "nation": "country",

    "year": "year",

    "temperature_anomaly": "temperature_anomaly",
    "temperature_anamoly": "temperature_anomaly",
    "temp_anomaly": "temperature_anomaly",
    "temperature_change": "temperature_anomaly",

    "co2_emissions": "co2_emissions",
    "co₂_emissions": "co2_emissions",
    "co2": "co2_emissions",
    "co2_emission": "co2_emissions",
    "co2_emissions_mt": "co2_emissions",

    "gdp": "gdp",
    "gdp_usd": "gdp",
    "gdp_current_us$": "gdp",

    "extreme_weather_events": "extreme_weather_events",
    "extreme_events": "extreme_weather_events",
    "extreme_weather": "extreme_weather_events",

    "population": "population",
}

INT_FIELDS = {"year"}
FLOAT_FIELDS = {
    "temperature_anomaly",
    "co2_emissions",
    "gdp",
    "extreme_weather_events",
    "population",
}

def canonical_key(raw_key: str) -> str:
    k = to_snake_case(raw_key)
    return HEADER_ALIASES.get(k, k)

def parse_value(key: str, val: str):
    if val is None:
        return 0
    v = val.strip()
    if v == "" or v.lower() in {"na", "n/a", "null"}:
        return 0
    try:
        if key in INT_FIELDS:
            return int(float(v))
        if key in FLOAT_FIELDS:
            return float(v)
    except ValueError:
        return 0
    return v
class ClimateDataProcessor:
    def __init__(self, filename: str):
        self.data = []
        self._load_data(filename)

    def _load_data(self, filename: str):
        
          with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rec = {}
                for k, v in row.items():
                    ck = canonical_key(k)
                    rec[ck] = parse_value(ck, v)
        
                rec.setdefault("country", "")
                rec.setdefault("year", 0)
                rec.setdefault("temperature_anomaly", 0.0)
                rec.setdefault("co2_emissions", 0.0)
                rec.setdefault("gdp", 0.0)
                rec.setdefault("extreme_weather_events", 0.0)
                rec.setdefault("population", 0.0)
                self.data.append(rec)

    def search_by_country(self, country: str):
    
        return [r for r in self.data if r["country"].lower() == country.lower()]

    def search_by_year_range(self, start_year: int, end_year: int):
    
        return [r for r in self.data if start_year <= r["year"] <= end_year]

    def find_extreme_events(self, count: int = 10, highest: bool = True):
      
        totals = {}
        for r in self.data:
            c = r["country"]
            totals[c] = totals.get(c, 0.0) + (r.get("extreme_weather_events", 0.0) or 0.0)
        items = list(totals.items())
        items.sort(key=itemgetter(1), reverse=highest)
        return items[:count]

    def find_high_co2_emitters(self, year: int, count: int = 10):
        year_rows = [r for r in self.data if r["year"] == year]
        year_rows.sort(key=lambda r: r["co2_emissions"], reverse=True)
        return [(r["country"], r["co2_emissions"]) for r in year_rows[:count]]

    def sort_by_temperature_anomaly(self, ascending: bool = True):
        """O(N log N)"""
        return sorted(self.data, key=lambda r: r["temperature_anomaly"], reverse=not ascending)

    def sort_by_gdp(self, year: int, ascending: bool = True):
        """O(K log K) where K = rows for that year"""
        year_rows = [r for r in self.data if r["year"] == year]
        return sorted(year_rows, key=lambda r: r["gdp"], reverse=not ascending)

    def average_metrics(self, country: str, metrics: list[str]):
        """O(M) where M = #rows for country"""
        rows = self.search_by_country(country)
        if not rows:
            return None
        n = len(rows)
        out = {}
        for m in metrics:
            key = canonical_key(m)
            total = sum((r.get(key, 0) or 0) for r in rows)
            out[key] = total / n if n else 0.0
        return out

def print_table(rows, max_rows=10, columns=None, title=None):
    if title:
        print(f"\n=== {title} ===")
    if not rows:
        print("No results.\n")
        return
    if isinstance(rows[0], dict):
        cols = columns or list(rows[0].keys())
        print(" | ".join(cols))
        print("-" * (len(" | ".join(cols)) + 2))
        for r in rows[:max_rows]:
            print(" | ".join(str(r.get(c, "")) for c in cols))
        if len(rows) > max_rows:
            print(f"... ({len(rows)-max_rows} more)")
    else:
        for r in rows[:max_rows]:
            print(r)
        if len(rows) > max_rows:
            print(f"... ({len(rows)-max_rows} more)")
    print()

def print_kv(d: dict, title=None):
    if title:
        print(f"\n=== {title} ===")
    if not d:
        print("No results.\n")
        return
    for k, v in d.items():
        if isinstance(v, float):
            print(f"{k}: {v:.4f}")
        else:
            print(f"{k}: {v}")
    print()

def main():
    # default_csv = "global_warming_dataset.csv"
    # path = input(f"CSV path [{default_csv}]: ").strip() or default_csv
    path = "data.csv"
    try:
        proc = ClimateDataProcessor(path)
    except FileNotFoundError:
        print(f"File not found: {path}")
        sys.exit(1)
    MENU = """
Climate Data Processing System
------------------------------
1) Search by Country
2) Search by Year Range
3) Top/Bottom countries by Extreme Weather Events (total)
4) Top-N CO2 emitters in a given Year
5) Sort by Temperature Anomaly (asc/desc)
6) Sort by GDP for a given Year (asc/desc)
7) Average metrics for a Country
0) Exit
"""

    while True:
        print(MENU)
        choice = input("Choose an option: ").strip()

        if choice == "1":
            country = input("Country: ").strip()
            rows = proc.search_by_country(country)
            print_table(rows, max_rows=10, columns=["country", "year", "temperature_anomaly", "co2_emissions", "gdp", "extreme_weather_events", "population"],
                        title=f"Records for {country}")

        elif choice == "2":
            try:
                s = int(input("Start year: ").strip())
                e = int(input("End year: ").strip())
            except ValueError:
                print("Invalid year(s).")
                continue
            rows = proc.search_by_year_range(s, e)
            print_table(rows, max_rows=10, columns=["country", "year", "temperature_anomaly", "co2_emissions", "gdp", "extreme_weather_events"],
                        title=f"Records from {s} to {e}")

        elif choice == "3":
            try:
                n = int(input("How many countries to list? (e.g., 10): ").strip())
            except ValueError:
                print("Invalid number.")
                continue
            hb = input("Highest or Lowest? [h/l]: ").strip().lower() or "h"
            highest = hb.startswith("h")
            items = proc.find_extreme_events(count=n, highest=highest)
            title = ("Top" if highest else "Bottom") + f" {n} by Extreme Weather Events (sum across years)"
            print_table(items, title=title)

        elif choice == "4":
            try:
                year = int(input("Year: ").strip())
                n = int(input("Top N (e.g., 10): ").strip())
            except ValueError:
                print("Invalid input.")
                continue
            items = proc.find_high_co2_emitters(year=year, count=n)
            print_table(items, title=f"Top {n} CO2 Emitters in {year}")

        elif choice == "5":
            order = input("Ascending or Descending? [a/d]: ").strip().lower() or "d"
            asc = order.startswith("a")
            rows = proc.sort_by_temperature_anomaly(ascending=asc)
            print_table(
                [(r["country"], r["year"], r["temperature_anomaly"]) for r in rows],
                title=f"Sorted by Temperature Anomaly ({'asc' if asc else 'desc'})",
            )

        elif choice == "6":
            try:
                year = int(input("Year: ").strip())
            except ValueError:
                print("Invalid year.")
                continue
            order = input("Ascending or Descending? [a/d]: ").strip().lower() or "d"
            asc = order.startswith("a")
            rows = proc.sort_by_gdp(year=year, ascending=asc)
            print_table(
                [(r["country"], r["gdp"]) for r in rows],
                title=f"GDP in {year} ({'asc' if asc else 'desc'})"
            )

        elif choice == "7":
            country = input("Country: ").strip()
        
            metrics = ["co2_emissions", "temperature_anomaly", "gdp"]
            avg = proc.average_metrics(country, metrics)
            if avg is None:
                print("Country not found.\n")
            else:
                print_kv(avg, title=f"Average metrics for {country}")

        elif choice == "0":
            print("Bye!")
            break
        else:
            print("Invalid option. Try again.")

if __name__ == "__main__":
    main()
