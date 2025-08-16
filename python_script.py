import csv
from operator import itemgetter

class ClimateDataProcessor:
    def __init__(self, filename):
        self.data = []
        self.load_data(filename)
    
    def load_data(self, filename):
        """Task 1: Data Loading & Parsing"""
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                processed_row = {}
                for key, value in row.items():
                    if key in ['Year']:
                        processed_row[key] = int(value) if value else 0
                    elif key in ['Temperature_Anomaly', 'CO2_Emissions', 'GDP', 
                                'Extreme_Weather_Events', 'Population']:  # Added here
                        processed_row[key] = float(value) if value else 0.0
                    else:
                        processed_row[key] = value.strip() if value else ''
                self.data.append(processed_row)
    
    def search_by_country(self, country):
        """Task 2: Search by Country"""
        return [record for record in self.data if record['Country'].lower() == country.lower()]
    
    def search_by_year_range(self, start_year, end_year):
        """Task 2: Search by Year Range"""
        return [record for record in self.data if start_year <= record['Year'] <= end_year]
    def find_extreme_events(self, count=10, highest=True):
        """Task 2: Find Extreme Events"""
        # Group by country and sum extreme weather events
        country_events = {}
        for record in self.data:
            country = record['Country']
            events = record['Extreme_Weather_Events']
            country_events[country] = country_events.get(country, 0) + events
        
        # Convert to list of tuples and sort
        sorted_countries = sorted(country_events.items(), key=lambda x: x[1], reverse=highest)
        return sorted_countries[:count]
    
    
        
        # Convert to list of tuples and sort
        sorted_countries = sorted(country_events.items(), key=itemgetter(1), reverse=highest)
        return sorted_countries[:count]
    
    def find_high_co2_emitters(self, year, count=10):
        """Task 2: Find High CO₂ Emitters"""
        year_data = [record for record in self.data if record['Year'] == year]
        sorted_data = sorted(year_data, key=lambda x: x['CO2_Emissions'], reverse=True)
        return [(record['Country'], record['CO2_Emissions']) for record in sorted_data[:count]]
    
    def sort_by_temperature_anomaly(self, ascending=True):
        """Task 3: Sort by Temperature_Anomaly"""
        return sorted(self.data, key=lambda x: x['Temperature_Anomaly'], reverse=not ascending)
    
    def sort_by_gdp(self, year, ascending=True):
        """Task 3: Sort by GDP"""
        year_data = [record for record in self.data if record['Year'] == year]
        return sorted(year_data, key=lambda x: x['GDP'], reverse=not ascending)
    
    def average_metrics(self, country, metrics):
        """Task 3: Average Metrics"""
        country_data = self.search_by_country(country)
        if not country_data:
            return None
        
        results = {}
        for metric in metrics:
            total = sum(record[metric] for record in country_data)
            results[metric] = total / len(country_data)
        return results
    
    def display_results(self, results, title="Results"):
        """Task 4: Visualization (terminal output)"""
        print(f"\n=== {title} ===")
        if not results:
            print("No results found")
            return
        
        if isinstance(results, list):
            if len(results) == 0:
                print("No results found")
                return
            
            # Display first item to get keys
            sample = results[0]
            if isinstance(sample, dict):
                # Print header
                headers = sample.keys()
                print("\t".join(headers))
                
                # Print rows
                for item in results:
                    print("\t".join(str(item[h]) for h in headers))
            else:
                for item in results:
                    print(item)
        elif isinstance(results, dict):
            for key, value in results.items():
                print(f"{key}: {value:.2f}")
        else:
            print(results)

# Example usage
if __name__ == "__main__":
    processor = ClimateDataProcessor("data.csv")
    
    # Demonstrate all functionalities
    print("\nClimate Data Processing System")
    print("----------------------------")
    
    # Task 2 examples
    print("\n1. Searching for records from United States:")
    us_data = processor.search_by_country("United States")
    processor.display_results(us_data[:5], "United States Data (first 5 records)")
    
    print("\n2. Searching for records between 2000-2005:")
    year_range_data = processor.search_by_year_range(2000, 2005)
    processor.display_results(year_range_data[:5], "Year Range 2000-2005 (first 5 records)")
    
    print("\n3. Countries with highest extreme weather events:")
    extreme_events = processor.find_extreme_events(5, highest=True)
    processor.display_results(extreme_events, "Top 5 Countries by Extreme Weather Events")
    
    print("\n4. Top 5 CO2 emitters in 2020:")
    co2_emitters = processor.find_high_co2_emitters(2020, 5)
    processor.display_results(co2_emitters, "Top 5 CO2 Emitters in 2020")
    
    # Task 3 examples
    print("\n5. Countries sorted by temperature anomaly (descending):")
    temp_sorted = processor.sort_by_temperature_anomaly(ascending=False)
    processor.display_results([(d['Country'], d['Temperature_Anomaly']) for d in temp_sorted[:5]], 
                            "Top 5 Countries by Temperature_Anomaly")
    
    print("\n6. Countries sorted by GDP in 2020 (descending):")
    gdp_sorted = processor.sort_by_gdp(2020, ascending=False)
    processor.display_results([(d['Country'], d['GDP']) for d in gdp_sorted[:5]], 
                            "Top 5 Countries by GDP in 2020")
    
    print("\n7. Average metrics for China:")
    avg_metrics = processor.average_metrics("China", ['CO2 Emissions', 'Temperature Anomaly', 'GDP'])
    processor.display_results(avg_metrics, "Average Metrics for China")