# NYC-Taxi-Dashboard
![alt text](https://github.com/Singularity-Coder/NYC-Taxi-Dashboard/blob/main/assets/banner.jpg)
An interactive Tableau dashboard showing NYC taxi patterns, weather impact, and neighborhood performance.

## Tableau Dashboard
[Tableau Visualization Link](https://public.tableau.com/app/profile/hithesh.v1025/viz/Book1_17614471053210/NYCTaxiDash1?publish=yes)
![alt text](https://github.com/Singularity-Coder/NYC-Taxi-Dashboard/blob/main/assets/sc1.png)
![alt text](https://github.com/Singularity-Coder/NYC-Taxi-Dashboard/blob/main/assets/sc2.png)

## Part 1: Download Datasets

1. **Taxi Trips**

   - Go to: [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page?utm_source=chatgpt.com)
   - Pick: Yellow Taxi → Any recent month → Download CSV
   - [Parquet to CSV](https://www.agentsfordata.com/parquet/to/csv)

2. **Zone Names**

   - Go to: [Taxi Zone Lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
   - This maps location codes to neighborhood names
   - For creating a map: [Taxi Zone Shapefile](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip)

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

**Formatting:**
- Click Marks card for `SUM(trips)` → change to Line, color Blue
- Click Marks card for `AVG(temp_c)` → change to Line, color Orange
- Add `AVG(precip_mm)` to Color on trips marks (shows rain impact)

**Add Reference Band:**
- Analytics pane → drag "Reference Band" to temp axis -> Drag it to the floating box & not the literal axis.
- Set range: 0-10°C (cold), 10-20°C (mild), 20+°C (warm)
   * In **Band From › Value**, choose **Create a New Parameter…**
     • Name: `Lower (0)` → Number (decimal) → Current value **0**.
   * In **Band To › Value**, choose **Create a New Parameter…**
     • Name: `Cold Upper (10)` → value **10**.
   * Click **Add** to make the second band:
     • **From › Value:** pick `Cold Upper (10)`
     • **To › Value:** **Create a New Parameter…** → `Mild Upper (20)` → value **20`.
   * (Optional) third band:
     • **From:** `Mild Upper (20)`
     • **To:** **Maximum**
   * Make sure the temp axis range shows the bands. Right-click Avg Temp C axis → Edit Axis… → Fixed Start = 0, End = 35–40.

**Shows**: Hourly demand patterns, temperature correlation, precipitation impact

---

### Chart 2: Neighborhood Performance Map

**New Sheet** → Name: "Zone Performance"

**Setup:**
1. Drag `Zone` to Detail Mark
2. Drag `Borough` to Color Mark
3. Drag `SUM(trips)` to Size Mark
4. Change mark type to "Circle" or "Square"
5. Drag `Borough` & `SUM(trips)` to Label Mark
6. Optional - Create true geographic map

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
   * Right-click the **AVG(Speed (km/h))** → **Add Table Calculation…**
   * Configure the dialog:
   * **Calculation Type:** *Moving Calculation*
   * **Previous Values: 6**
   * **Next Values: 0**
   * This creates a trailing **7-hour** average (current hour + previous 6).
   * **Compute Using:** choose **Specific Dimensions** and **check only `hour_local`**.
   * This guarantees the calc moves **along time**, and **partitions by Borough** (one independent MA per Borough).
   * If your line goes flat or looks wrong, set **Compute Using → Table Across** (since time is on Columns) and try again. The safe, explicit choice is **Specific Dimensions → hour_local only**.


**Add Context:**
- Analytics → Drag Reference Line → Avg Speed

**Add Precipitation Overlay:**
- Drag `AVG(precip_mm)` to Rows (dual axis)
- Change to Bar chart
- Reduce opacity in Marks to 30%

**Shows**: When/where traffic is worst, weather impact on speed

---

### Chart 4: Revenue & Tips Breakdown

**New Sheet** → Name: "Financial Performance"

**Setup:**
1. Create calculated field: `Revenue per Trip = [avg_total]`
2. Rows: `Borough`
3. Columns: `SUM(Revenue per Hour)`
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
1. Rows: `Hour of Day`
2. Columns: `Day of Week`
3. Color: `SUM(trips)`
4. Change mark type to Square
5. Adjust color gradient (white to dark blue)
6. Add `AVG(tip_pct)` to Label

**Shows**: Best times for drivers, demand patterns by day/hour

## Part 5: Assemble Dashboard

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

5. How to apply filters
   - To add Date Time Range Filter, Create parameter "Date Filter"
   - Data Type - DateTime
   - Set "Current Value". Ex: 31/01/2024 22:00:00
   - Allowable values: Select "Range"
   - Set Max, Min, Step Size 1 (Days), Fixed
   - Create "Calculated Field" called "Keep Row?"
   ```
   NOT ISNULL([Trip Date])
   AND DATETRUNC('day', [Trip Date]) <= DATETRUNC('day', [Date Filter])
   ```
   - Drag "Keep Row?" calc field to Filters pane and select "True"
   - Right Click the applied filter -> Apply to Worksheets -> All Using This DataSource

6. Add title and labels


## Part 6: Make It Interactive

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


## Part 7: Publish

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


## Notes on Adding Filters

Date field is `[Trip Date]` and parameter is named `Date Filter`.

### One-ended filter (everything **up to** a chosen date)

1. **Keep/Make the parameter**

   * Data pane → **Create Parameter…**
   * Name: `Date Filter`
   * **Data type:** *Date* (or *Date & Time* if your field is DateTime)
   * Pick any default date inside your data’s range → **OK**
   * Right-click `Date Filter` → **Show Parameter**

2. **Create the calculated field**

   * Right-click in Data pane → **Create Calculated Field…**
   * Name: `Keep Row?`
   * Formula:

     ```tableau
     NOT ISNULL([Trip Date])
     AND DATETRUNC('day', [Trip Date]) <= DATETRUNC('day', [Date Filter])
     ```
   * **OK**

3. **Use it to actually filter**

   * Drag `Keep Row?` to the **Filters** shelf → choose **True** → **OK**.

4. (Optional but recommended)

   * Right-click `Keep Row?` on Filters → **Add to Context**.
   * If on a dashboard: filter menu → **Apply to Worksheets → All Using This Data Source**.

Now moving the **Date Filter** parameter changes the viz.

### Range filter (Between start & end)

If you want a start and end date the user can change:

1. **Two parameters**

   * `p_StartDate` (Date), default to your min date.
   * `p_EndDate` (Date), default to your max date.
   * Show both parameters.

2. **Calculated field**

   * Name: `Keep Row (Range)?`
   * Formula:

     ```tableau
     NOT ISNULL([Trip Date])
     AND DATETRUNC('day', [Trip Date]) >= DATETRUNC('day', [p_StartDate])
     AND DATETRUNC('day', [Trip Date]) <= DATETRUNC('day', [p_EndDate])
     ```

3. **Filter with it**

   * Put `Keep Row (Range)?` on **Filters** → keep **True**.

### Notes so nothing “vanishes”

* If `[Trip Date]` is **DateTime**, keeping `DATETRUNC('day', …)` on both sides prevents time mismatches.
* Make sure your parameter values sit **within** the data’s min/max date.
* Remove any old native date filter pills (or ensure they don’t conflict).

Your **date field** only appears inside the calc (wrapped with `DATETRUNC('day', [Trip Date])`) and the **calc** is what goes on the **Filters** shelf.

## Credits
Photo by [Kai Pilger](https://www.pexels.com/photo/taxi-overtaking-bus-462867/)
