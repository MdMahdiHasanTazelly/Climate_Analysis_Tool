FILE="data.csv"

# Ensure file exists
if [ ! -f "$FILE" ]; then
    echo "Error: $FILE not found!"
    exit 1
fi

#Ask to save CSV before showing results
ask_and_save() {

    if [ ! -d "./shell" ]; then
        mkdir "./shell"  #creates folder
    fi
    local output_file="./shell/$1" # 1 denotes function's name (1st param)
    local data="$2"   # 2 -> result(2nd param)
    read -p "Do you want to save this output as CSV? (y/n): " save_choice
    if [[ "$save_choice" == "y" || "$save_choice" == "Y" ]]; then
        {
            head 1 "$FILE"  #update (removed -n)
            echo "$data"
        } > "$output_file"
        echo "Output saved to $output_file"
    fi
    echo "======= Results ======"
    # echo "$data"
    head -n 1 "$FILE" | awk -F',' 'BEGIN {OFS=" | "} {print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14}'
    echo "$data" | awk -F',' 'BEGIN {OFS=" | "} {print $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14}'
    echo -e "\n************** Runtime : $3 ms. **************\n"  # 3-> runtime(3rd param)
}

#Search by Country
search_by_country() {
    read -p "Enter country name(eg: Country_103): " country
    start=$(date +%s%N)  # runtime calculation starts
    result=$(awk -F',' -v c="$country" 'NR>1 && $1 == c' "$FILE")
    end=$(date +%s%N) #runtime calculation ensd
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_search_by_country_$country.csv" "$result" "$runtime"
}

#Search by Year Range
search_by_year_range() {
    read -p "Enter start year: " start
    read -p "Enter end year: " end
    start_time=$(date +%s%N)  # runtime calculation starts
    result=$(awk -F',' -v s="$start" -v e="$end" 'NR>1 && ($2 >= s && $2 <= e)' "$FILE")
    end_time=$(date +%s%N) #runtime calculation ensd
    runtime=$(( (end_time - start_time) / 1000000 ))   # in milli
    ask_and_save "output_year_range_$start-$end.csv" "$result" "$runtime"
}

#Finds top 5 Extreme weather countries
find_extreme_events() {
    read -p "Find (max/min): " type
    if [[ "$type" == "max" ]]; then
        start=$(date +%s%N)  # runtime calculation starts
        result=$(tail -n +2 "$FILE" | sort -t',' -k14 -nr | head -n 5)
        end=$(date +%s%N) #runtime calculation ensd
    else
        start=$(date +%s%N)  # runtime calculation starts
        result=$(tail -n +2 "$FILE" | sort -t',' -k14 -n | head -n 5)
        end=$(date +%s%N) #runtime calculation ensd
    fi
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_extreme_events.csv" "$result" "$runtime"
}

#High CO₂ Emitters
find_high_co2() {
    read -p "Enter year: " year
    read -p "Enter top N: " n
    start=$(date +%s%N)  # runtime calculation starts
    result=$(awk -F',' -v y="$year" 'NR>1 && $2 == y' "$FILE" | sort -t',' -k4 -nr | head -n "$n")
    end=$(date +%s%N) #runtime calculation ensd
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_high_co2_$year-$n.csv" "$result" "$runtime"
}

#Urbanization & Deforestation for a country
urban_deforestation() {
    read -p "Enter country name: " country
    start=$(date +%s%N)  # runtime calculation starts
    result=$(awk -F',' -v c="$country" 'NR>1 && $1 == c {print $0}' "$FILE")
    end=$(date +%s%N) #runtime calculation ensd
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_urban_deforestation_$country.csv" "$result" "$runtime"
}

#Sort by Temperature Anomaly
sort_by_temp_anomaly() {
    read -p "Ascending or Descending (asc/desc): " order
    if [[ "$order" == "asc" ]]; then
        start=$(date +%s%N)  # runtime calculation starts
        result=$(tail -n +2 "$FILE" | sort -t',' -k3 -n)
        end=$(date +%s%N) #runtime calculation ensd
    else
        start=$(date +%s%N)  # runtime calculation starts
        result=$(tail -n +2 "$FILE" | sort -t',' -k3 -nr)
        end=$(date +%s%N) #runtime calculation ensd
    fi
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_temp_anomaly.csv" "$result" "$runtime"
}

#Sort by GDP
sort_by_gdp() {
    read -p "Enter year: " year
    read -p "Ascending or Descending (asc/desc): " order
    data=$(awk -F',' -v y="$year" 'NR>1 && $2 == y' "$FILE")
    if [[ "$order" == "asc" ]]; then
        start=$(date +%s%N)  # runtime calculation starts
        result=$(echo "$data" | sort -t',' -k7 -n)
        end=$(date +%s%N) #runtime calculation ensd
    else
        start=$(date +%s%N)  # runtime calculation starts
        result=$(echo "$data" | sort -t',' -k7 -nr)
        end=$(date +%s%N) #runtime calculation ensd
    fi
    runtime=$(( (end - start) / 1000000 ))   # in milli
    ask_and_save "output_gdp_$year-$order.csv" "$result" "$runtime"
}

#Average metrics for a country (txt only)
average_metrics() {
    read -p "Enter country name: " country
    start=$(date +%s%N)  # runtime calculation starts
    avg=$(awk -F',' -v c="$country" '
    BEGIN {CO2=0; TA=0; count=0}
    NR>1 && $1 == c {CO2 += $4; TA += $3; count++}
    END {
        if (count > 0) {
            print "Average CO2 Emissions:", CO2/count
            print "Average Temperature Anomaly:", TA/count
        } else {
            print "No data for", c
        }
    }' "$FILE")
    end=$(date +%s%N) #runtime calculation ensd

    echo -e "\n=== Results ===\n"
    echo -e "$avg\n"

    runtime=$(( (end - start) / 1000000 ))   # in milli
    
    echo -e "\n************** Runtime : $runtime ms. **************\n" 

    read -p "Do you want to save this output to a .txt file? (y/n): " save_choice
    if [[ "$save_choice" == "y" || "$save_choice" == "Y" ]]; then
        echo "$avg" > ./shell/output_avg_metrics_"$country".txt
        echo -e "\nSaved to output_avg_metrics.txt\n"
    fi
}

delete_files(){
    rm -rf "shell" 
    echo -e "\n\n"
    echo "########### The files have been deleted! ###############"
    echo -e "\n\n"
}

# Main menu 
while true; do
    echo -e "\n================== Climate Data Analysis Tool ==================\n"
    echo "1. Search by Country"
    echo "2. Search by Year Range(1950-2023)"
    echo "3. Find 5 Extreme weather countries"
    echo "4. High CO₂ Emitters"
    echo "5. Urbanization & Deforestation by Country"
    echo "6. Sort by Temperature Anomaly"
    echo "7. Sort by GDP"
    echo "8. Average Metrics for Country"
    echo "9. Want to delete existing files?"
    echo "10. Exit"
    echo -e "\n"
    read -p "Choose an option: " choice

    case $choice in
        1) search_by_country ;;
        2) search_by_year_range ;;
        3) find_extreme_events ;;
        4) find_high_co2 ;;
        5) urban_deforestation ;;
        6) sort_by_temp_anomaly ;;
        7) sort_by_gdp ;;
        8) average_metrics ;;
        9) delete_files;;
        10) echo -e "\n============ SEE YOU AGAAAAAAAAAIIIIIN ==========\n"; 
        break ;;
        *) echo "Invalid option" ;;
    esac
done