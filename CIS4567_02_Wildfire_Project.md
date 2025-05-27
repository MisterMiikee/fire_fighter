# Project Milestone 2: Data Preprocessing

**CIS 4567.02**

**Project Group 4**

## Project Overview

Our project objective is to analyze recent California wildfire data, providing actionable prevention methods and recommendation to reduce future cases of high structural damage and loss.

We will be performing our analyses through Apache Spark/PySpark, with visualizations being done with small samples through Pandas.

Our data is sourced from Kaggle ([The California Wildfire Data🔥🔥🔥](https://www.kaggle.com/datasets/vijayveersingh/the-california-wildfire-data)) and contains over 100,000 observations of structural damage across various fire incidents across California from 6/6/20 to 12/9/2024.

> This dataset is sourced from the California Open Data Portal and is maintained by CAL FIRE and associated agencies.

**Variables**
```
OBJECTID: A unique identifier for each record in the dataset.
DAMAGE: Indicates the level of fire damage to the structure (e.g., "No Damage", "Affected (1-9%)").
STREETNUMBER: The street number of the impacted structure.
STREETNAME: The name of the street where the impacted structure is located.
STREETTYPE: The type of street (e.g., "Road", "Lane").
STREETSUFFIX: Additional address information, such as apartment or building numbers (if applicable).
CITY: The city where the impacted structure is located.
STATE: The state abbreviation (e.g., "CA" for California).
ZIPCODE: The postal code of the impacted structure.
CALFIREUNIT: The CAL FIRE unit responsible for the area.
COUNTY: The county where the impacted structure is located.
COMMUNITY: The community or neighborhood of the structure.
INCIDENTNAME: The name of the fire incident that impacted the structure.
APN: The Assessor’s Parcel Number (APN) of the property.
ASSESSEDIMPROVEDVALUE: The assessed value of the improved property (e.g., structures, not just land).
YEARBUILT: The year the structure was built.
SITEADDRESS: The full address of the property, including city, state, and ZIP code.
GLOBALID: A globally unique identifier for each record.
Latitude: The latitude coordinate of the structure’s location.
Longitude: The longitude coordinate of the structure’s location.
UTILITYMISCSTRUCTUREDISTANCE: The distance between the main structure and any utility or miscellaneous structures (if recorded).
FIRENAME: An alternative or secondary name for the fire incident.
geometry: A geospatial representation of the location in a point format (e.g., "POINT (-13585927.697 4646740.750)").
```

### Initialize Spark, PySpark


```python
!apt-get update # Update apt-get repository.
!apt-get install openjdk-8-jdk-headless -qq > /dev/null # Install Java.
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz # Download Apache Sparks.
!tar xf spark-3.1.1-bin-hadoop3.2.tgz # Unzip the tgz file.
!pip install -q findspark # Install findspark. Adds PySpark to the System path during runtime.

# Set environment variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"

!ls

# Initialize findspark
import findspark
findspark.init()

# Create a PySpark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark
```

    Get:1 https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/ InRelease [3,632 B]
    Hit:2 https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64  InRelease
    Hit:3 http://archive.ubuntu.com/ubuntu jammy InRelease
    Get:4 http://security.ubuntu.com/ubuntu jammy-security InRelease [129 kB]
    Get:5 http://archive.ubuntu.com/ubuntu jammy-updates InRelease [128 kB]
    Get:6 https://r2u.stat.illinois.edu/ubuntu jammy InRelease [6,555 B]
    Get:7 http://archive.ubuntu.com/ubuntu jammy-backports InRelease [127 kB]
    Hit:8 https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy InRelease
    Hit:9 https://ppa.launchpadcontent.net/graphics-drivers/ppa/ubuntu jammy InRelease
    Hit:10 https://ppa.launchpadcontent.net/ubuntugis/ppa/ubuntu jammy InRelease
    Get:11 https://r2u.stat.illinois.edu/ubuntu jammy/main amd64 Packages [2,722 kB]
    Get:12 http://archive.ubuntu.com/ubuntu jammy-updates/universe amd64 Packages [1,546 kB]
    Get:13 https://r2u.stat.illinois.edu/ubuntu jammy/main all Packages [8,932 kB]
    Get:14 http://archive.ubuntu.com/ubuntu jammy-updates/main amd64 Packages [3,212 kB]
    Get:15 http://security.ubuntu.com/ubuntu jammy-security/main amd64 Packages [2,901 kB]
    Get:16 http://security.ubuntu.com/ubuntu jammy-security/universe amd64 Packages [1,245 kB]
    Fetched 21.0 MB in 8s (2,492 kB/s)
    Reading package lists... Done
    W: Skipping acquire of configured file 'main/source/Sources' as repository 'https://r2u.stat.illinois.edu/ubuntu jammy InRelease' does not seem to provide it (sources.list entry misspelt?)
    sample_data  spark-3.1.1-bin-hadoop3.2	spark-3.1.1-bin-hadoop3.2.tgz






    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://ee7fb2014d9c:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.1.1</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>pyspark-shell</code></dd>
    </dl>
</div>

    </div>





```python
spark_df = (
    spark
    .read
    .csv(
        '/content/Postfire_Master_Data.csv',
        header=True,
        inferSchema=True
    )
)
```

### Data Cleaning


```python
spark_df.printSchema()
```

    root
     |-- _id: string (nullable = true)
     |-- OBJECTID: string (nullable = true)
     |-- * Damage: string (nullable = true)
     |-- * Street Number: double (nullable = true)
     |-- * Street Name: string (nullable = true)
     |-- * Street Type (e.g. road, drive, lane, etc.): string (nullable = true)
     |-- Street Suffix (e.g. apt. 23, blding C): string (nullable = true)
     |-- * City: string (nullable = true)
     |-- State: string (nullable = true)
     |-- Zip Code: integer (nullable = true)
     |-- * CAL FIRE Unit: string (nullable = true)
     |-- County: string (nullable = true)
     |-- Community: string (nullable = true)
     |-- Battalion: integer (nullable = true)
     |-- * Incident Name: string (nullable = true)
     |-- Incident Number (e.g. CAAEU 123456): string (nullable = true)
     |-- Incident Start Date: string (nullable = true)
     |-- Hazard Type: string (nullable = true)
     |-- If Affected 1-9% - Where did fire start?: string (nullable = true)
     |-- If Affected 1-9% - What started fire?: string (nullable = true)
     |-- Structure Defense Actions Taken: string (nullable = true)
     |-- * Structure Type: string (nullable = true)
     |-- Structure Category: string (nullable = true)
     |-- # Units in Structure (if multi unit): integer (nullable = true)
     |-- # of Damaged Outbuildings < 120 SQFT: integer (nullable = true)
     |-- # of Non Damaged Outbuildings < 120 SQFT: integer (nullable = true)
     |-- * Roof Construction: string (nullable = true)
     |-- * Eaves: string (nullable = true)
     |-- * Vent Screen: string (nullable = true)
     |-- * Exterior Siding: string (nullable = true)
     |-- * Window Pane: string (nullable = true)
     |-- * Deck/Porch On Grade: string (nullable = true)
     |-- * Deck/Porch Elevated: string (nullable = true)
     |-- * Patio Cover/Carport Attached to Structure: string (nullable = true)
     |-- * Fence Attached to Structure: string (nullable = true)
     |-- Distance - Propane Tank to Structure: string (nullable = true)
     |-- Distance - Residence to Utility/Misc Structure &gt; 120 SQFT: string (nullable = true)
     |-- Fire Name (Secondary): string (nullable = true)
     |-- APN (parcel): string (nullable = true)
     |-- Assessed Improved Value (parcel): integer (nullable = true)
     |-- Year Built (parcel): integer (nullable = true)
     |-- Site Address (parcel): string (nullable = true)
     |-- GLOBALID: string (nullable = true)
     |-- Latitude: string (nullable = true)
     |-- Longitude: double (nullable = true)
     |-- x: double (nullable = true)
     |-- y: double (nullable = true)
    


Here we can see our initial schema from loading in our csv file.


```python
spark_df = spark_df.drop('_id','Battalion','Incident Number (e.g. CAAEU 123456)',
              'Incident Start Date',
              'If Affected 1-9% - Where did fire start?',
              'If Affected 1-9% - What started fire?',
              'Structure Defense Actions Taken','# Units in Structure (if multi unit)',
              '# of Damaged Outbuildings < 120 SQFT',
              '# of Non Damaged Outbuildings < 120 SQFT',
              'Distance - Propane Tank to Structure',
              'Distance - Residence to Utility/Misc Structure &gt; 120 SQFT',
              'Fire Name (Secondary)',
              'Assessed Improved Value (parcel)','x','y')
```


```python
# Get the list of columns we will be using
spark_df_columns = spark_df.columns
print(spark_df_columns)
```

    ['OBJECTID', '* Damage', '* Street Number', '* Street Name', '* Street Type (e.g. road, drive, lane, etc.)', 'Street Suffix (e.g. apt. 23, blding C)', '* City', 'State', 'Zip Code', '* CAL FIRE Unit', 'County', 'Community', '* Incident Name', 'Hazard Type', '* Structure Type', 'Structure Category', '* Roof Construction', '* Eaves', '* Vent Screen', '* Exterior Siding', '* Window Pane', '* Deck/Porch On Grade', '* Deck/Porch Elevated', '* Patio Cover/Carport Attached to Structure', '* Fence Attached to Structure', 'APN (parcel)', 'Year Built (parcel)', 'Site Address (parcel)', 'GLOBALID', 'Latitude', 'Longitude']


We will drop unnecessary columns to reduce dimensionality.


```python
# Check hazard type unique values
spark_df.select('Hazard Type').distinct().show()
```

    +-----------+
    |Hazard Type|
    +-----------+
    |       null|
    |       Fire|
    +-----------+
    


After looking at distinct values within `Hazard Type`, we can narrow this column down to solely focus on `Fire` related incidents.


```python
# Only use Fire related data
spark_df = spark_df.filter(spark_df['Hazard Type'] == 'Fire')
```

We are now only working with `Fire` related incidents.

Let's convert column names to a uniform format.


```python
# Convert all column names ot lowercase
spark_df = spark_df.toDF(*[c.lower() for c in spark_df.columns])
```


```python
# Remove ' *' from the begining of column names
spark_df = spark_df.toDF(*[c.replace('*', '') for c in spark_df.columns])
```


```python
# Replace parentheses and content
import re
spark_df = spark_df.toDF(*[re.sub(r'\(.*\)', '', c).strip() for c in spark_df.columns])
```


```python
# Strip whitespaces
spark_df = spark_df.toDF(*[c.strip() for c in spark_df.columns])
```


```python
# Replace places " " with "_"
spark_df = spark_df.toDF(*[c.replace(' ', '_') for c in spark_df.columns])
```

After our column name transformations, we can verify our results and view the new, uniformly formatted names.


```python
spark_df.printSchema()
```

    root
     |-- objectid: string (nullable = true)
     |-- damage: string (nullable = true)
     |-- street_number: double (nullable = true)
     |-- street_name: string (nullable = true)
     |-- street_type: string (nullable = true)
     |-- street_suffix: string (nullable = true)
     |-- city: string (nullable = true)
     |-- state: string (nullable = true)
     |-- zip_code: integer (nullable = true)
     |-- cal_fire_unit: string (nullable = true)
     |-- county: string (nullable = true)
     |-- community: string (nullable = true)
     |-- incident_name: string (nullable = true)
     |-- hazard_type: string (nullable = true)
     |-- structure_type: string (nullable = true)
     |-- structure_category: string (nullable = true)
     |-- roof_construction: string (nullable = true)
     |-- eaves: string (nullable = true)
     |-- vent_screen: string (nullable = true)
     |-- exterior_siding: string (nullable = true)
     |-- window_pane: string (nullable = true)
     |-- deck/porch_on_grade: string (nullable = true)
     |-- deck/porch_elevated: string (nullable = true)
     |-- patio_cover/carport_attached_to_structure: string (nullable = true)
     |-- fence_attached_to_structure: string (nullable = true)
     |-- apn: string (nullable = true)
     |-- year_built: integer (nullable = true)
     |-- site_address: string (nullable = true)
     |-- globalid: string (nullable = true)
     |-- latitude: string (nullable = true)
     |-- longitude: double (nullable = true)
    


### Duplicate Analysis


```python
# Check for exact duplicates
spark_df.count(), spark_df.distinct().count()
```




    (2375, 2375)




```python
# Remove exact duplicates
spark_df = spark_df.dropDuplicates()

# Verify removal
spark_df.count(), spark_df.distinct().count()
```




    (2375, 2375)



We can see that the amount of distinct rows matches the amount of total rows, meaning that there were no duplicates. We still provided code for dropping duplicates for example purposes.

### Handling Missing Values

We can now start to handle our missing data. Let's first investigate the amount of missing observations between each column. This will help us understand our data better and determine the next course of action.


```python
from pyspark.sql.functions import col, when, isnull, count

# Calculate missing values for each column
missing_counts = spark_df.select(
    [count(when(isnull(c), c)).alias(c) for c in spark_df.columns]
).collect()[0].asDict()

# Display the missing value counts for each column
for column, count in missing_counts.items():
    print(f"Column '{column}': {count} missing values")
```

    Column 'objectid': 0 missing values
    Column 'damage': 0 missing values
    Column 'street_number': 0 missing values
    Column 'street_name': 0 missing values
    Column 'street_type': 0 missing values
    Column 'street_suffix': 1246 missing values
    Column 'city': 0 missing values
    Column 'state': 0 missing values
    Column 'zip_code': 1332 missing values
    Column 'cal_fire_unit': 0 missing values
    Column 'county': 0 missing values
    Column 'community': 1944 missing values
    Column 'incident_name': 0 missing values
    Column 'hazard_type': 0 missing values
    Column 'structure_type': 0 missing values
    Column 'structure_category': 0 missing values
    Column 'roof_construction': 0 missing values
    Column 'eaves': 0 missing values
    Column 'vent_screen': 0 missing values
    Column 'exterior_siding': 0 missing values
    Column 'window_pane': 0 missing values
    Column 'deck/porch_on_grade': 0 missing values
    Column 'deck/porch_elevated': 0 missing values
    Column 'patio_cover/carport_attached_to_structure': 0 missing values
    Column 'fence_attached_to_structure': 0 missing values
    Column 'apn': 4 missing values
    Column 'year_built': 551 missing values
    Column 'site_address': 132 missing values
    Column 'globalid': 0 missing values
    Column 'latitude': 0 missing values
    Column 'longitude': 0 missing values


We can see that there are some columns with large amounts of missing data (`zip_code`, `community`), as well as some columns containing marginal amounts of missing data (`street_name`, `vent_screen`, `site_address`, etc.).

To better clean and reduce potential bias, we will remove any colummns that do not provide predictive input (i.e. nominal data, id columns, high missingness, etc.)


```python
# Drop unnecessary columns / high-null count columns
spark_df = spark_df.drop('objectid', 'hazard_type', 'street_suffix', 'zip_code', 'community', 'year_built',
                         'fence_attached_to_structure', 'object_id', 'street_number',
                         'street_name', 'street_type', 'city', 'state', 'cal_fire_unit',
                         'county', 'incident_name', 'apn', 'site_address', 'globalid',
                         'latitude', 'longitude')
```

Let's now see our total features that we are dealing with.


```python
spark_df.printSchema()
```

    root
     |-- damage: string (nullable = true)
     |-- structure_type: string (nullable = true)
     |-- structure_category: string (nullable = true)
     |-- roof_construction: string (nullable = true)
     |-- eaves: string (nullable = true)
     |-- vent_screen: string (nullable = true)
     |-- exterior_siding: string (nullable = true)
     |-- window_pane: string (nullable = true)
     |-- deck/porch_on_grade: string (nullable = true)
     |-- deck/porch_elevated: string (nullable = true)
     |-- patio_cover/carport_attached_to_structure: string (nullable = true)
    


We have narrowed down the dimensionality of our data and can proceed with null value handling.


```python
# Show null count
print(spark_df.count() - spark_df.dropna().count())
```

    0


After seeing the total null count, we can proceed to remove these as these only represent a small fraction (1-2%) of our total data (~100,000 observations).

Let's remove these nulls and compare our before and after.


```python
# Drop null-containing rows
spark_df_cleaned = spark_df.dropna()
spark_df.count(), spark_df_cleaned.count()
```




    (2375, 2375)



As we can see, we removed a small amount of our total data but reduced potential problems through null-containing entries.

Within our data we must also account for entries simply recorded as whitespace (` `). We will consider these observations to be (`Unknown`) and will replace the entries to reflect this logic.

This will also reduce feature complexity by combining (` `) and `Unknown` occurrences into a single category.


```python
# Replace blank observations with Unknown
spark_df_cleaned = spark_df_cleaned.replace(' ', 'Unknown')
```

### Outlier Analysis

Let's move on to outlier analysis. Since our data is heavily categorical, we will have to determine whether any classes are heavily under-represented as compared to other classes.


```python
from pyspark.sql.functions import col, when, count

def check_rare_categories(df, column_name, threshold=10):
    # Frequency distribution for 'structure_type'
    structure_type_frequency = df.groupBy(column_name).agg(count("*").alias("frequency"))

    # Filter for rare categories in 'structure_type'
    rare_structure_types = structure_type_frequency.filter(col("frequency") < threshold)

    # Display rare categories of structure_type
    rare_structure_types.show()

check_rare_categories(spark_df_cleaned, 'structure_type')
```

    +--------------------+---------+
    |      structure_type|frequency|
    +--------------------+---------+
    |              School|        3|
    |Mixed Commercial/...|        2|
    |Multi Family Resi...|        5|
    |Commercial Buildi...|        1|
    |Multi Family Resi...|        3|
    +--------------------+---------+
    



```python
# Display rare categories of structure_category
check_rare_categories(spark_df_cleaned, 'structure_category')
```

    +--------------------+---------+
    |  structure_category|frequency|
    +--------------------+---------+
    |  Multiple Residence|        8|
    |Mixed Commercial/...|        2|
    +--------------------+---------+
    



```python
# Display rare categories of roof_contruction
check_rare_categories(spark_df_cleaned, 'roof_construction')
```

    +-----------------+---------+
    |roof_construction|frequency|
    +-----------------+---------+
    +-----------------+---------+
    



```python
# Display rare categories of eaves
check_rare_categories(spark_df_cleaned, 'eaves')
```

    +-----+---------+
    |eaves|frequency|
    +-----+---------+
    +-----+---------+
    



```python
# Display rare categories of vent_screen
check_rare_categories(spark_df_cleaned, 'vent_screen')
```

    +-----------+---------+
    |vent_screen|frequency|
    +-----------+---------+
    +-----------+---------+
    



```python
# Display rare categories of exterior_siding
check_rare_categories(spark_df_cleaned, 'exterior_siding')
```

    +---------------+---------+
    |exterior_siding|frequency|
    +---------------+---------+
    +---------------+---------+
    



```python
# Display rare categories of window_pane
check_rare_categories(spark_df_cleaned, 'window_pane')
```

    +-----------+---------+
    |window_pane|frequency|
    +-----------+---------+
    +-----------+---------+
    



```python
# Display rare categories of deck/porch_on_grade
check_rare_categories(spark_df_cleaned, 'deck/porch_on_grade')
```

    +-------------------+---------+
    |deck/porch_on_grade|frequency|
    +-------------------+---------+
    +-------------------+---------+
    



```python
# Display rare categories of deck/porch_on_grade
check_rare_categories(spark_df_cleaned, 'deck/porch_elevated')
```

    +-------------------+---------+
    |deck/porch_elevated|frequency|
    +-------------------+---------+
    +-------------------+---------+
    



```python
# Display rare categories of patio_cover/carport_attached_to_structure
check_rare_categories(spark_df_cleaned, 'patio_cover/carport_attached_to_structure')
```

    +-----------------------------------------+---------+
    |patio_cover/carport_attached_to_structure|frequency|
    +-----------------------------------------+---------+
    +-----------------------------------------+---------+
    


## Descriptive Statistics & Visualization

### Descriptive Stats & Interpretation


```python
spark_df_cleaned.summary().show()
```

    +-------+---------------+--------------------+------------------+-----------------+--------+--------------------+---------------+-----------+-------------------+-------------------+-----------------------------------------+
    |summary|         damage|      structure_type|structure_category|roof_construction|   eaves|         vent_screen|exterior_siding|window_pane|deck/porch_on_grade|deck/porch_elevated|patio_cover/carport_attached_to_structure|
    +-------+---------------+--------------------+------------------+-----------------+--------+--------------------+---------------+-----------+-------------------+-------------------+-----------------------------------------+
    |  count|           2375|                2375|              2375|             2375|    2375|                2375|           2375|       2375|               2375|               2375|                                     2375|
    |   mean|           null|                null|              null|             null|    null|                null|           null|       null|               null|               null|                                     null|
    | stddev|           null|                null|              null|             null|    null|                null|           null|       null|               null|               null|                                     null|
    |    min|Affected (1-9%)|Commercial Buildi...|    Infrastructure|          Asphalt|Enclosed|Mesh Screen <= 1/...|          Metal| Multi Pane|          Composite|          Composite|                              Combustible|
    |    25%|           null|                null|              null|             null|    null|                null|           null|       null|               null|               null|                                     null|
    |    50%|           null|                null|              null|             null|    null|                null|           null|       null|               null|               null|                                     null|
    |    75%|           null|                null|              null|             null|    null|                null|           null|       null|               null|               null|                                     null|
    |    max|      No Damage|Utility Misc Stru...|  Single Residence|             Wood| Unknown|          Unscreened|           Wood|    Unknown|               Wood|               Wood|                                  Unknown|
    +-------+---------------+--------------------+------------------+-----------------+--------+--------------------+---------------+-----------+-------------------+-------------------+-----------------------------------------+
    


Given that most of our data is categorical, analytical statistics offer limited interpretive value. The key insights we can derive involve identifying the most and least common attributes within each variable. Notably, the most frequent exteral  type is `wood`, and the most prevalent structure category is `single residence`.

### Visualizations


```python
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Create Pandas DataFrame using sample of data
df_sample = spark_df_cleaned.sample(False, 0.01, seed=42).toPandas()
```

#### Histograms


```python

```

No numerical data would gain any advantage from being represented through a histogram.

#### Bar Charts


```python
fig = plt.figure(figsize=(20, 20))
plt.suptitle("Distributions", fontsize=20, y=1.01)

for i, col in enumerate(df_sample.columns):
    ax = fig.add_subplot(3, 4, i + 1)
    sns.countplot(data=df_sample, y=col, ax=ax, order=df_sample[col].value_counts().index)
    plt.xlabel('')
    plt.ylabel('')
    plt.title(col, fontsize=16)
    plt.tick_params('both', length=0)

sns.despine()
plt.tight_layout()
plt.savefig('/content/distributions.png', bbox_inches='tight')

plt.show()
```


    
![png](CIS4567_02_Wildfire_Project_files/CIS4567_02_Wildfire_Project_62_0.png)
    


 The key takeaway is that the majority of structures were either completely `destroyed` or emerged from the fires untouched with `no damage`. This stark contrast highlights the extremes in structural outcomes, with relatively few falling into intermediate damage categories. Such findings could guide further investigation into factors contributing to these outcomes, such as location, construction materials, or fire prevention measures.


#### Scatter Plots


```python

```

No numerical data would gain any advantage from being represented through a scatter plot.


```python

```

## Final Results Interpretation

Within our analysis of ~100,000 observations of wildfire data, we determined that there is a mainly binary pattern between high structural damage versus minimal or no damage. We also identified single-family wood residences to be the most common and most vulnerable structures, which highlights the need for targeted preventative measures. Overall quality of data and dimensionality of features were minimal post-cleaning as compared to the original raw data, and retains the most relevant features, albeit solely categorical. Focus as of now should be on protecting wood-based residences and other high-risk structural setups. We will be proceeding with our modeling stage to determine predictive capabilities and feature importance for wildfire damage prevention.

# Project Milestone 3: Modeling

**CIS 4567.02**

**Project Group 4**

In this section, we will finalize our data transformations as well as perform our classification modeling utilizing Logistic Regression and Random Forest, along with results interpretation and reports.

## Correlation Analysis


```python
spark_df_cleaned.printSchema()
```

    root
     |-- damage: string (nullable = true)
     |-- structure_type: string (nullable = true)
     |-- structure_category: string (nullable = true)
     |-- roof_construction: string (nullable = true)
     |-- eaves: string (nullable = true)
     |-- vent_screen: string (nullable = true)
     |-- exterior_siding: string (nullable = true)
     |-- window_pane: string (nullable = true)
     |-- deck/porch_on_grade: string (nullable = true)
     |-- deck/porch_elevated: string (nullable = true)
     |-- patio_cover/carport_attached_to_structure: string (nullable = true)
    


Because all features are categorical, we cannot apply Pearson's correlation directly to the raw data. While one-hot encoding technically produces numeric flags, those binary vectors measure only category co-occurrence.

## Data Preprocessing

We will utilize a Pipeline class from PySpark's ML module, as well as StringIndexer and OneHotEncoder to encode our categorical columns into an appropriate output.

We will also be utilizing n-1 columns (with dropLast parameter) to reduce redundancy and collinearity.

Since our data at this point does not contain any numeric features, our data is now appropriately transformed to be utilized in model training.


```python
spark_df.printSchema()
```

    root
     |-- damage: string (nullable = true)
     |-- structure_type: string (nullable = true)
     |-- structure_category: string (nullable = true)
     |-- roof_construction: string (nullable = true)
     |-- eaves: string (nullable = true)
     |-- vent_screen: string (nullable = true)
     |-- exterior_siding: string (nullable = true)
     |-- window_pane: string (nullable = true)
     |-- deck/porch_on_grade: string (nullable = true)
     |-- deck/porch_elevated: string (nullable = true)
     |-- patio_cover/carport_attached_to_structure: string (nullable = true)
    



```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import when, col

# Binarize `damage` into a 0/1 label
df2 = spark_df.withColumn(
    "label",
    when(col("damage") == "Destroyed (>50%)", 1).otherwise(0)
)

# Pick all other string columns
categorical_cols = [
    name
    for name, dtype in df2.dtypes
    if dtype == "string" and name != "damage"
]

indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_Index", handleInvalid='skip') for col in categorical_cols]  # Index for Strings, skipping any null entries
encoders = [OneHotEncoder(inputCol=f"{col}_Index", outputCol=f"{col}_Encoded", dropLast=True) for col in categorical_cols]  # One Hot encodes the index columns, dropping the last category

# Create pipeline with indexers and encoders
pipeline = Pipeline(stages=indexers + encoders)

# Fit and transform the DataFrame
ohe_model = pipeline.fit(df2)
transformed_df = ohe_model.transform(df2)

# Identify indexer columns (those ending with "_Index")
indexer_cols = [col for col in transformed_df.columns if col.endswith("_Index")]

# Drop the original and indexer columns
cleaned_df = transformed_df.drop(*indexer_cols)

# Show the transformed DataFrame
cleaned_df.show()

```

    +----------------+--------------------+--------------------+-----------------+----------+--------------------+-------------------+-----------+-------------------+-------------------+-----------------------------------------+-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    |          damage|      structure_type|  structure_category|roof_construction|     eaves|         vent_screen|    exterior_siding|window_pane|deck/porch_on_grade|deck/porch_elevated|patio_cover/carport_attached_to_structure|label|structure_type_Encoded|structure_category_Encoded|roof_construction_Encoded|eaves_Encoded|vent_screen_Encoded|exterior_siding_Encoded|window_pane_Encoded|deck/porch_on_grade_Encoded|deck/porch_elevated_Encoded|patio_cover/carport_attached_to_structure_Encoded|
    +----------------+--------------------+--------------------+-----------------+----------+--------------------+-------------------+-----------+-------------------+-------------------+-----------------------------------------+-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    |    Inaccessible|Single Family Res...|    Single Residence|          Unknown|   Unknown|             Unknown|            Unknown|    Unknown|            Unknown|            Unknown|                                  Unknown|    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|            Metal|  No Eaves|             Unknown|              Other|Single Pane|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[0],[1.0])|         (10,[8],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |       No Damage|          Motor Home|    Single Residence|            Metal|  No Eaves|            No Vents|              Metal|Single Pane|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    0|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |       No Damage|Utility Misc Stru...|Other Minor Struc...|          Asphalt|Unenclosed|            No Vents|              Vinyl| Multi Pane|   Masonry/Concrete|      No Deck/Porch|                     No Patio Cover/Ca...|    0|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[2],[1.0])|         (10,[9],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |       No Damage|Single Family Res...|    Single Residence|          Unknown|   Unknown|             Unknown|            Unknown|    Unknown|            Unknown|            Unknown|                                  Unknown|    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |       No Damage|Single Family Res...|    Single Residence|          Unknown|   Unknown|             Unknown|            Unknown|    Unknown|            Unknown|            Unknown|                                  Unknown|    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|          Unknown|   Unknown|             Unknown|            Unknown|    Unknown|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|          Unknown|   Unknown|             Unknown|            Unknown|    Unknown|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|          Asphalt|Unenclosed|Mesh Screen > 1/8...|               Wood|Single Pane|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|          Motor Home|    Single Residence|            Other|  No Eaves|Mesh Screen > 1/8...|              Metal|Single Pane|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[7],[1.0])|(5,[3],[1.0])|      (9,[1],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|          Asphalt|   Unknown|Mesh Screen > 1/8...|               Wood| No Windows|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[0],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[3],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    | Affected (1-9%)|Commercial Buildi...|Nonresidential Co...|            Metal|Unenclosed|Mesh Screen <= 1/...|               Wood| Multi Pane|      No Deck/Porch|               Wood|                              Combustible|    0|        (18,[4],[1.0])|             (6,[2],[1.0])|           (10,[1],[1.0])|(5,[1],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[0],[1.0])|              (5,[2],[1.0])|                                    (4,[2],[1.0])|
    |Destroyed (>50%)|Mobile Home Singl...|    Single Residence|            Metal|   Unknown|Mesh Screen <= 1/...|               Wood|Single Pane|               Wood|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[5],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[0],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[1],[1.0])|              (5,[4],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Utility Misc Stru...|Other Minor Struc...|          Asphalt|   Unknown|Mesh Screen <= 1/...|               Wood| Multi Pane|   Masonry/Concrete|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[0],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |       No Damage|Utility Misc Stru...|Other Minor Struc...|          Asphalt|Unenclosed|Mesh Screen > 1/8...|               Wood| Multi Pane|   Masonry/Concrete|      No Deck/Porch|                     No Patio Cover/Ca...|    0|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Single Family Res...|    Single Residence|          Asphalt|Unenclosed|          Unscreened|Stucco Brick Cement| Multi Pane|               Wood|               Wood|                          Non Combustible|    1|        (18,[2],[1.0])|             (6,[0],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[6],[1.0])|         (10,[3],[1.0])|      (6,[0],[1.0])|              (5,[4],[1.0])|              (5,[2],[1.0])|                                        (4,[],[])|
    |Destroyed (>50%)|Single Family Res...|    Single Residence|            Metal|   Unknown|Mesh Screen > 1/8...|               Wood| Multi Pane|      No Deck/Porch|               Wood|                     No Patio Cover/Ca...|    1|        (18,[2],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[0],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[0],[1.0])|              (5,[2],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|          Motor Home|    Single Residence|            Other|  No Eaves|            No Vents|              Metal|Single Pane|               Wood|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[7],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[4],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|          Motor Home|    Single Residence|            Metal|  No Eaves|            No Vents|              Metal|Single Pane|      No Deck/Porch|      No Deck/Porch|                     No Patio Cover/Ca...|    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |Destroyed (>50%)|Mobile Home Tripl...|    Single Residence|          Asphalt|Unenclosed|Mesh Screen > 1/8...|Stucco Brick Cement| Multi Pane|   Masonry/Concrete|               Wood|                              Combustible|    1|       (18,[11],[1.0])|             (6,[0],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[3],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[2],[1.0])|                                    (4,[2],[1.0])|
    +----------------+--------------------+--------------------+-----------------+----------+--------------------+-------------------+-----------+-------------------+-------------------+-----------------------------------------+-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    only showing top 20 rows
    


Next, we'll drop the original columns that we have encoded from.


```python
cleaned_df = cleaned_df.drop("damage","structure_type","structure_category","roof_construction",
                "eaves","vent_screen","exterior_siding","window_pane","deck/porch_on_grade",
                "deck/porch_elevated","patio_cover/carport_attached_to_structure")
cleaned_df.show()
```

    +-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    |label|structure_type_Encoded|structure_category_Encoded|roof_construction_Encoded|eaves_Encoded|vent_screen_Encoded|exterior_siding_Encoded|window_pane_Encoded|deck/porch_on_grade_Encoded|deck/porch_elevated_Encoded|patio_cover/carport_attached_to_structure_Encoded|
    +-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    |    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[0],[1.0])|         (10,[8],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    0|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    0|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[2],[1.0])|         (10,[9],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |    0|        (18,[0],[1.0])|             (6,[0],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[3],[1.0])|              (5,[3],[1.0])|                                    (4,[3],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[2],[1.0])|(5,[0],[1.0])|      (9,[0],[1.0])|         (10,[2],[1.0])|      (6,[2],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[7],[1.0])|(5,[3],[1.0])|      (9,[1],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[0],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[3],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    0|        (18,[4],[1.0])|             (6,[2],[1.0])|           (10,[1],[1.0])|(5,[1],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[0],[1.0])|              (5,[2],[1.0])|                                    (4,[2],[1.0])|
    |    1|        (18,[5],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[0],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[1],[1.0])|              (5,[4],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[0],[1.0])|      (9,[3],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    0|        (18,[1],[1.0])|             (6,[1],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[2],[1.0])|             (6,[0],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[6],[1.0])|         (10,[3],[1.0])|      (6,[0],[1.0])|              (5,[4],[1.0])|              (5,[2],[1.0])|                                        (4,[],[])|
    |    1|        (18,[2],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[0],[1.0])|      (9,[1],[1.0])|         (10,[0],[1.0])|      (6,[0],[1.0])|              (5,[0],[1.0])|              (5,[2],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[7],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[4],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|        (18,[6],[1.0])|             (6,[0],[1.0])|           (10,[1],[1.0])|(5,[3],[1.0])|      (9,[2],[1.0])|         (10,[6],[1.0])|      (6,[1],[1.0])|              (5,[0],[1.0])|              (5,[0],[1.0])|                                    (4,[0],[1.0])|
    |    1|       (18,[11],[1.0])|             (6,[0],[1.0])|           (10,[0],[1.0])|(5,[1],[1.0])|      (9,[1],[1.0])|         (10,[3],[1.0])|      (6,[0],[1.0])|              (5,[1],[1.0])|              (5,[2],[1.0])|                                    (4,[2],[1.0])|
    +-----+----------------------+--------------------------+-------------------------+-------------+-------------------+-----------------------+-------------------+---------------------------+---------------------------+-------------------------------------------------+
    only showing top 20 rows
    



```python
cleaned_df.printSchema()
```

    root
     |-- label: integer (nullable = false)
     |-- structure_type_Encoded: vector (nullable = true)
     |-- structure_category_Encoded: vector (nullable = true)
     |-- roof_construction_Encoded: vector (nullable = true)
     |-- eaves_Encoded: vector (nullable = true)
     |-- vent_screen_Encoded: vector (nullable = true)
     |-- exterior_siding_Encoded: vector (nullable = true)
     |-- window_pane_Encoded: vector (nullable = true)
     |-- deck/porch_on_grade_Encoded: vector (nullable = true)
     |-- deck/porch_elevated_Encoded: vector (nullable = true)
     |-- patio_cover/carport_attached_to_structure_Encoded: vector (nullable = true)
    


## Modeling

Here we import the necessary modules to perform additional transformations and continue with training models.


```python
# Install older version of numpy for compatibility
!pip install numpy==1.24.1
```

    Requirement already satisfied: numpy==1.24.1 in /usr/local/lib/python3.11/dist-packages (1.24.1)



```python
import pyspark.sql.functions as f
import pyspark.ml.classification as cl
import pyspark.ml.evaluation as ev
import pyspark.ml.feature as feat
import numpy as np
```

### Train/Test + VectorAssembler

Our final preparation before modeling will be splitting our data into training and testing splits (70/30 split) as well as storing our features as a vector.


```python
# Create train/test split
train_df, test_df = (
    cleaned_df
    .randomSplit([0.7, 0.3], seed=0)
)

# Store features into VectorAssembler
vectorAssembler = feat.VectorAssembler(
    inputCols=cleaned_df.columns[1:]
    , outputCol='features')
```

### Logistic Regression


```python
# Create Logistic Regression object
logReg_obj = cl.LogisticRegression(
    labelCol='label'
    , featuresCol='features'
    , regParam=0.01
    , elasticNetParam=1.0
    , family='multinomial'
)

# Create pipeline
pip = Pipeline(stages=[vectorAssembler, logReg_obj])

# Fit model
pModel = pip.fit(train_df)
```


```python
# Store results
results_logReg = (
    pModel
    .transform(test_df)
    .select('label', 'probability', 'prediction')
)

# Create evaluator
evaluator = ev.MulticlassClassificationEvaluator(
    predictionCol='prediction'
    , labelCol='label')

# Evaluate based on F1, weightedPrecision and accuracy
(
    evaluator.evaluate(results_logReg)
    , evaluator.evaluate(
        results_logReg
        , {evaluator.metricName: 'weightedPrecision'}
    )
    , evaluator.evaluate(
        results_logReg
        , {evaluator.metricName: 'accuracy'}
    )
)
```




    (0.8775312181119445, 0.8816328762590717, 0.8774984755064706)



### Random Forest


```python
# Create Random Forest object
rf_obj = cl.RandomForestClassifier(
    labelCol='label'
    , featuresCol='features'
    , minInstancesPerNode=10
    , numTrees=10
    , seed=0
)

# Create pipeline
pipeline = Pipeline(
    stages=[vectorAssembler, rf_obj]
)

# Fit model
pModel = pipeline.fit(train_df)
```


```python
# Store results
results_rf = (
    pModel
    .transform(test_df)
    .select('label', 'probability', 'prediction')
)

# Create evalulator
evaluator = ev.MulticlassClassificationEvaluator(
    predictionCol='prediction'
    , labelCol='label')

# Evaluate based on F1, weightedPrecision and accuracy
(
    evaluator.evaluate(results_rf)
    , evaluator.evaluate(
        results_rf
        , {evaluator.metricName: 'weightedPrecision'}
    )
    , evaluator.evaluate(
        results_rf
        , {evaluator.metricName: 'accuracy'}
    )
)
```




    (0.8559629349149424, 0.8575261677922598, 0.8563927095331662)



### Feature Importances


```python
from pyspark.ml.linalg import SparseVector

# 1. Get the feature importances as a NumPy array
importances = pModel.stages[-1].featureImportances
imp_arr = importances.toArray()

# 2. Recover the assembler and its input vector cols
assembler = pModel.stages[0]      # your VectorAssembler
vec_cols  = assembler.getInputCols()

# 3. For each vector cols, pull the category labels from the OneHotEncoderModel
indexer_models = ohe_model.stages[:len(categorical_cols)]
labels_per_col = [m.labels for m in indexer_models]

# 4. Build the full list of feature names
feature_names = []
for col_name, labels in zip(vec_cols, labels_per_col):
    for lbl in labels:
        safe = str(lbl).replace(" ", "_")
        feature_names.append(f"{col_name}_{safe}")

# 5. Pair, sort, and display
feat_imp = list(zip(feature_names, imp_arr))
feat_imp.sort(key=lambda x: x[1], reverse=True)

# Show top 10
for name, score in feat_imp[:10]:
    print(f"{name:40s}  {score:.4f}")
```

    roof_construction_Encoded_Wood            0.1723
    vent_screen_Encoded_Unscreened            0.1580
    vent_screen_Encoded__                     0.0904
    eaves_Encoded_Enclosed                    0.0886
    window_pane_Encoded_No_Windows            0.0670
    exterior_siding_Encoded_Vinyl             0.0383
    deck/porch_on_grade_Encoded_Wood          0.0379
    vent_screen_Encoded_Deck_Elevated         0.0359
    deck/porch_elevated_Encoded_No_Deck/Porch  0.0344
    exterior_siding_Encoded__                 0.0294



```python
# Create pandas DataFrame for visualization purposes
feat_imp_df = pd.DataFrame(feat_imp, columns=['Feature', 'Importance']).set_index('Feature')

plt.figure(figsize=(8, 6))

# Plot heatmap of feature importances
sns.heatmap(
    feat_imp_df.head(10),  # Top 10 features
    annot=True,  # Show numerical values in cells
    cmap='Blues',  # Use blue color palette
    fmt='.2f',  # Display numerical values with 3 decimal places
    cbar=False,  # Remove colorbar
)

plt.ylabel('')
plt.xlabel('')

# Customize title and remove visual ticks
plt.title('Top 10 Feature Importances (Random Forest)')
plt.tick_params('both', length=0)

plt.tight_layout()
plt.savefig('/content/feature_importances.png', bbox_inches='tight')

plt.show()
```


    
![png](CIS4567_02_Wildfire_Project_files/CIS4567_02_Wildfire_Project_97_0.png)
    


# Result Interpretation

**Introduction:**

We used three models to analyze structures to determine key features that will result in the best building practices to prevent fire damage in fire-prone areas. Our model determines what features result in the greatest fire damage.

**Key Findings:**

The top 3 features that damage a structure are: 1. The roof construction, 2. Vent Screens, 3. Enclosure of Eaves.

**Significance:**

Understanding and narrowing down the features that have the highest impact will allow city managers to determine what buildings need to be retrofitted to minimize/ prevent structural damage in wildfires.

**Limitations**

Due to constraints, we could not utilize the geographical information of buildings to see which areas are more prone to fire damage from the recent wildfires. Also, though we evaluated our models based on F1, weighted precision, and accuracy, the values were less than 90%. There is possible room for improvement in the models' implementation.

**Implications**

The results show that city managers should primarily focus on the type of material used in building construction. Roofs that have been constructed with wood as the construction material will result in a higher possibility of fire damage. The second most important feature is the use of vent screens. Buildings that do not have vent screens are more likely to be damaged by fires. This could be do to hot debris entering the ventilation systems and causing the fire to ignite from inside. Lastly, the third feature that should be taken notice of is the roof of constructions with enclosed eaves. We believe the type of material use to build the enclose eave would cause the strucutre to be more substance to catching on fire.


```python
!jupyter nbconvert --to html
```

    This application is used to convert notebook files (*.ipynb)
            to various other formats.
    
            WARNING: THE COMMANDLINE INTERFACE MAY CHANGE IN FUTURE RELEASES.
    
    Options
    =======
    The options below are convenience aliases to configurable class-options,
    as listed in the "Equivalent to" description-line of the aliases.
    To see all configurable class-options for some <cmd>, use:
        <cmd> --help-all
    
    --debug
        set log level to logging.DEBUG (maximize logging output)
        Equivalent to: [--Application.log_level=10]
    --show-config
        Show the application's configuration (human-readable format)
        Equivalent to: [--Application.show_config=True]
    --show-config-json
        Show the application's configuration (json format)
        Equivalent to: [--Application.show_config_json=True]
    --generate-config
        generate default config file
        Equivalent to: [--JupyterApp.generate_config=True]
    -y
        Answer yes to any questions instead of prompting.
        Equivalent to: [--JupyterApp.answer_yes=True]
    --execute
        Execute the notebook prior to export.
        Equivalent to: [--ExecutePreprocessor.enabled=True]
    --allow-errors
        Continue notebook execution even if one of the cells throws an error and include the error message in the cell output (the default behaviour is to abort conversion). This flag is only relevant if '--execute' was specified, too.
        Equivalent to: [--ExecutePreprocessor.allow_errors=True]
    --stdin
        read a single notebook file from stdin. Write the resulting notebook with default basename 'notebook.*'
        Equivalent to: [--NbConvertApp.from_stdin=True]
    --stdout
        Write notebook output to stdout instead of files.
        Equivalent to: [--NbConvertApp.writer_class=StdoutWriter]
    --inplace
        Run nbconvert in place, overwriting the existing notebook (only
                relevant when converting to notebook format)
        Equivalent to: [--NbConvertApp.use_output_suffix=False --NbConvertApp.export_format=notebook --FilesWriter.build_directory=]
    --clear-output
        Clear output of current file and save in place,
                overwriting the existing notebook.
        Equivalent to: [--NbConvertApp.use_output_suffix=False --NbConvertApp.export_format=notebook --FilesWriter.build_directory= --ClearOutputPreprocessor.enabled=True]
    --coalesce-streams
        Coalesce consecutive stdout and stderr outputs into one stream (within each cell).
        Equivalent to: [--NbConvertApp.use_output_suffix=False --NbConvertApp.export_format=notebook --FilesWriter.build_directory= --CoalesceStreamsPreprocessor.enabled=True]
    --no-prompt
        Exclude input and output prompts from converted document.
        Equivalent to: [--TemplateExporter.exclude_input_prompt=True --TemplateExporter.exclude_output_prompt=True]
    --no-input
        Exclude input cells and output prompts from converted document.
                This mode is ideal for generating code-free reports.
        Equivalent to: [--TemplateExporter.exclude_output_prompt=True --TemplateExporter.exclude_input=True --TemplateExporter.exclude_input_prompt=True]
    --allow-chromium-download
        Whether to allow downloading chromium if no suitable version is found on the system.
        Equivalent to: [--WebPDFExporter.allow_chromium_download=True]
    --disable-chromium-sandbox
        Disable chromium security sandbox when converting to PDF..
        Equivalent to: [--WebPDFExporter.disable_sandbox=True]
    --show-input
        Shows code input. This flag is only useful for dejavu users.
        Equivalent to: [--TemplateExporter.exclude_input=False]
    --embed-images
        Embed the images as base64 dataurls in the output. This flag is only useful for the HTML/WebPDF/Slides exports.
        Equivalent to: [--HTMLExporter.embed_images=True]
    --sanitize-html
        Whether the HTML in Markdown cells and cell outputs should be sanitized..
        Equivalent to: [--HTMLExporter.sanitize_html=True]
    --log-level=<Enum>
        Set the log level by value or name.
        Choices: any of [0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']
        Default: 30
        Equivalent to: [--Application.log_level]
    --config=<Unicode>
        Full path of a config file.
        Default: ''
        Equivalent to: [--JupyterApp.config_file]
    --to=<Unicode>
        The export format to be used, either one of the built-in formats
                ['asciidoc', 'custom', 'html', 'latex', 'markdown', 'notebook', 'pdf', 'python', 'qtpdf', 'qtpng', 'rst', 'script', 'slides', 'webpdf']
                or a dotted object name that represents the import path for an
                ``Exporter`` class
        Default: ''
        Equivalent to: [--NbConvertApp.export_format]
    --template=<Unicode>
        Name of the template to use
        Default: ''
        Equivalent to: [--TemplateExporter.template_name]
    --template-file=<Unicode>
        Name of the template file to use
        Default: None
        Equivalent to: [--TemplateExporter.template_file]
    --theme=<Unicode>
        Template specific theme(e.g. the name of a JupyterLab CSS theme distributed
        as prebuilt extension for the lab template)
        Default: 'light'
        Equivalent to: [--HTMLExporter.theme]
    --sanitize_html=<Bool>
        Whether the HTML in Markdown cells and cell outputs should be sanitized.This
        should be set to True by nbviewer or similar tools.
        Default: False
        Equivalent to: [--HTMLExporter.sanitize_html]
    --writer=<DottedObjectName>
        Writer class used to write the
                                            results of the conversion
        Default: 'FilesWriter'
        Equivalent to: [--NbConvertApp.writer_class]
    --post=<DottedOrNone>
        PostProcessor class used to write the
                                            results of the conversion
        Default: ''
        Equivalent to: [--NbConvertApp.postprocessor_class]
    --output=<Unicode>
        Overwrite base name use for output files.
                    Supports pattern replacements '{notebook_name}'.
        Default: '{notebook_name}'
        Equivalent to: [--NbConvertApp.output_base]
    --output-dir=<Unicode>
        Directory to write output(s) to. Defaults
                                      to output to the directory of each notebook. To recover
                                      previous default behaviour (outputting to the current
                                      working directory) use . as the flag value.
        Default: ''
        Equivalent to: [--FilesWriter.build_directory]
    --reveal-prefix=<Unicode>
        The URL prefix for reveal.js (version 3.x).
                This defaults to the reveal CDN, but can be any url pointing to a copy
                of reveal.js.
                For speaker notes to work, this must be a relative path to a local
                copy of reveal.js: e.g., "reveal.js".
                If a relative path is given, it must be a subdirectory of the
                current directory (from which the server is run).
                See the usage documentation
                (https://nbconvert.readthedocs.io/en/latest/usage.html#reveal-js-html-slideshow)
                for more details.
        Default: ''
        Equivalent to: [--SlidesExporter.reveal_url_prefix]
    --nbformat=<Enum>
        The nbformat version to write.
                Use this to downgrade notebooks.
        Choices: any of [1, 2, 3, 4]
        Default: 4
        Equivalent to: [--NotebookExporter.nbformat_version]
    
    Examples
    --------
    
        The simplest way to use nbconvert is
    
                > jupyter nbconvert mynotebook.ipynb --to html
    
                Options include ['asciidoc', 'custom', 'html', 'latex', 'markdown', 'notebook', 'pdf', 'python', 'qtpdf', 'qtpng', 'rst', 'script', 'slides', 'webpdf'].
    
                > jupyter nbconvert --to latex mynotebook.ipynb
    
                Both HTML and LaTeX support multiple output templates. LaTeX includes
                'base', 'article' and 'report'.  HTML includes 'basic', 'lab' and
                'classic'. You can specify the flavor of the format used.
    
                > jupyter nbconvert --to html --template lab mynotebook.ipynb
    
                You can also pipe the output to stdout, rather than a file
    
                > jupyter nbconvert mynotebook.ipynb --stdout
    
                PDF is generated via latex
    
                > jupyter nbconvert mynotebook.ipynb --to pdf
    
                You can get (and serve) a Reveal.js-powered slideshow
    
                > jupyter nbconvert myslides.ipynb --to slides --post serve
    
                Multiple notebooks can be given at the command line in a couple of
                different ways:
    
                > jupyter nbconvert notebook*.ipynb
                > jupyter nbconvert notebook1.ipynb notebook2.ipynb
    
                or you can specify the notebooks list in a config file, containing::
    
                    c.NbConvertApp.notebooks = ["my_notebook.ipynb"]
    
                > jupyter nbconvert --config mycfg.py
    
    To see all available configurables, use `--help-all`.
    

