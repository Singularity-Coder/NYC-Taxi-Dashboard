# NYC-Taxi-Dashboard

An interactive Tableau dashboard showing NYC taxi patterns, weather impact, and neighborhood performance.

## Part 1: Download Datasets

1. **Taxi Trips**

   - Go to: [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page?utm_source=chatgpt.com)
   - Pick: Yellow Taxi → Any recent month → Download CSV
   - [Parquet to CSV](https://www.agentsfordata.com/parquet/to/csv)

2. **Zone Names**

   - Go to: [Taxi Zone Lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
   - This maps location codes to neighborhood names

3. **Weather**
   - Go to: [Meteostat](https://dev.meteostat.net/bulk/)
   - Find: Central Park station
   - Download: Hourly data for same month as taxi data
   - Format: CSV (unzip if .gz)

## Part 2: Setup Tableau Public

- [Download Tableau Public](https://www.tableau.com/products/public?utm_source=chatgpt.com)

## Part 3: Connect Your Data

* Tableau Public doesn't let you easily join multiple CSVs like Tableau Desktop. Pre-Join Files in Python BEFORE Tableau:
* Check [clean.ipynb](https://github.com/Singularity-Coder/NYC-Taxi-Dashboard/blob/main/clean.ipynb)
* The script above has all the calculated fields as well. So no need to create separate calculated fields in Tableau. Best to prepare them along with the cleaning process. Use Tableau purely for visualization.

## Part 4: Build Dashboard Components

### Chart 1: Trip Volume & Weather Over Time

**New Sheet** → Name: "Demand & Weather"

**Setup:**
1. Drag `hour_local` to Columns
2. Right-click → change to "Continuous" (creates line chart)
3. Drag `SUM(trips)` to Rows
4. Drag `AVG(temp_c)` to Rows (creates second chart)
5. Right-click the `AVG(temp_c)` axis → "Dual Axis"
6. Right-click again → "Synchronize Axis" (uncheck this to see both scales)

**Formatting:**
- Click Marks card for `SUM(trips)` → change to Line, color Blue
- Click Marks card for `AVG(temp_c)` → change to Line, color Orange
- Add `AVG(precip_mm)` to Color on trips marks (shows rain impact)

**Add Reference Band:**
- Analytics pane → drag "Reference Band" to temp axis
- Set range: 0-10°C (cold), 10-20°C (mild), 20+°C (warm)

**Shows**: Hourly demand patterns, temperature correlation, precipitation impact

---

### Chart 2: Neighborhood Performance Map

**New Sheet** → Name: "Zone Performance"

**Setup:**
1. Drag `Zone` to Detail
2. Drag `Borough` to Color
3. Drag `SUM(trips)` to Size
4. Change mark type to "Circle" or "Square"

**OR if you want a true geographic map:**
1. Double-click `Borough` (Tableau may auto-create map)
2. Drag `Zone` to Label
3. Drag `SUM(trips)` to Color
4. Adjust color gradient (green to red)

**Add Metric Selector:**
1. Create Parameter: "Map Metric"
   - Data type: String
   - Allowable values: List
   - Values: "Trip Count", "Avg Fare", "Tip %", "Revenue"

2. Create Calculated Field: "Selected Metric"
```
CASE [Map Metric]
    WHEN "Trip Count" THEN [trips]
    WHEN "Avg Fare" THEN [avg_fare]
    WHEN "Tip %" THEN [tip_pct] * 100
    WHEN "Revenue" THEN [trips] * [avg_total]
END
```

3. Replace Color field with `Selected Metric`
4. Show Parameter control (right-click parameter → Show Parameter)

**Shows**: Which neighborhoods generate most trips/revenue, tip patterns by area

---

### Chart 3: Service Quality - Speed Analysis

**New Sheet** → Name: "Speed & Efficiency"

**Setup:**
1. Drag `hour_local` to Columns (continuous)
2. Drag calculated field `Speed (km/h)` to Rows
3. Change aggregation to AVG
4. Add `Borough` to Color
5. Add Table Calculation:
   - Right-click the AVG line → Add Table Calculation
   - Type: Moving Average
   - Period: 7 (smooths out spikes)

**Add Context:**
- Analytics → Reference Line at 25 km/h (typical NYC speed)
- Add another at 15 km/h (congested) and 35 km/h (free-flowing)
- Color zones: Red (0-15), Yellow (15-25), Green (25-40)

**Add Precipitation Overlay:**
- Drag `AVG(precip_mm)` to Rows (dual axis)
- Change to Bar chart
- Reduce opacity to 30%

**Shows**: When/where traffic is worst, weather impact on speed

---

### Chart 4: Revenue & Tips Breakdown

**New Sheet** → Name: "Financial Performance"

**Setup:**
1. Create calculated field: `Revenue per Trip = [avg_total]`
2. Rows: `Borough`
3. Columns: `SUM(Revenue per Hour)` (calculated earlier)
4. Color: `AVG(tip_pct)` 
5. Size: `SUM(trips)`
6. Sort by revenue descending

**Make it a packed bubble chart:**
- Change mark type to Circle
- Adjust size range (large spread)

**Add Labels:**
- Drag `Borough` to Label
- Show SUM(trips) on label

**Shows**: Which boroughs are most profitable, tipping patterns by area

---

### Chart 5: Hourly Patterns Heatmap

**New Sheet** → Name: "Demand Heatmap"

**Setup:**
1. Rows: `Hour of Day` (calculated field from Part 4)
2. Columns: `Day of Week`
3. Color: `SUM(trips)`
4. Change mark type to Square
5. Adjust color gradient (white to dark blue)
6. Add `AVG(tip_pct)` to Label

**Shows**: Best times for drivers, demand patterns by day/hour

## Part 6: Assemble Dashboard

1. New Dashboard (click dashboard icon at bottom)
2. Drag all 3 sheets onto canvas
3. Layout style:
   - Top: Hourly Trends (full width)
   - Bottom-left: Zone Heatmap
   - Bottom-right: Speed Trends

4. Add filters:
   - Date range slider
   - Borough selector
   - Hour of day filter

5. Add title and labels


## Part 7: Make It Interactive

### Add Parameter Selector
1. Create Parameter: "Show Metric"
   - Values: Trips, Revenue, Tips, Speed
2. Create calculated field that switches based on parameter
3. Use in Color marks on map

### Enable Click-Through
- Dashboard → Actions → Add Action → Filter
- When you click a zone, other charts filter to that zone

### Add Tooltips
- Edit tooltip on each chart
- Include: Zone name, metric values, comparison to average


## Part 8: Publish

### Before Publishing:
- Remove extra fields from view
- Rename sheets clearly
- Add source credits in description
- Test all filters work

### Publish:
1. File → Save to Tableau Public
2. Sign in
3. Add description: "NYC Taxi Analysis - Data from TLC, Meteostat"
4. Save


## Key Insights to Highlight

- **Demand patterns**: Rush hours, weekend vs weekday
- **Weather impact**: Rainy days = more demand, lower speeds
- **Neighborhood differences**: Manhattan tips better, outer boroughs cheaper
- **Operational insights**: Where to position more taxis during peak times

## Troubleshooting

**Joins not working?** 
- Check datetime formats match
- Use DATETRUNC to standardize to hour

**Map not showing?**
- Tableau needs recognized geography
- Use zone coordinates or centroids

**Slow performance?**
- Extract data (Data → Extract)
- Limit to 1 month initially
- Aggregate before visualizing

**Missing weather data?**
- Fill nulls with previous value
- Or exclude incomplete hours