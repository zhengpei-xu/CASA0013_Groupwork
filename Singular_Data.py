# ==========================================
# Part 1: Download, Save, and Unzip Raw Data
# ==========================================
import requests
from pathlib import Path
import gzip
import shutil
import pandas as pd

url = "https://orca.casa.ucl.ac.uk/~jreades/data/20250615-London-listings.csv.gz"

base_folder = Path.cwd()
raw_folder = base_folder / "01_Data/Raw"
raw_folder.mkdir(parents=True, exist_ok=True)

raw_gz_path = raw_folder / Path(url).name
raw_csv_path = raw_gz_path.with_suffix("")

if not raw_gz_path.exists():
    response = requests.get(url)
    response.raise_for_status()
    with open(raw_gz_path, "wb") as f:
        f.write(response.content)

if not raw_csv_path.exists():
    with gzip.open(raw_gz_path, "rb") as f_in:
        with open(raw_csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

df = pd.read_csv(raw_csv_path)

# ========================
# Part 2: Reducing Columns
# ========================

cols = ['id', 'listing_url', 'last_scraped', 'name', 
    'description', 'host_id', 'host_name', 'host_since', 
    'host_location', 'host_about', 'host_is_superhost', 
    'host_listings_count', 'host_total_listings_count', 
    'host_verifications', 'latitude', 'longitude', 
    'property_type', 'room_type', 'accommodates', 
    'bathrooms', 'bathrooms_text', 'bedrooms', 'beds', 
    'amenities', 'price', 'minimum_nights', 'maximum_nights', 
    'availability_365', 'number_of_reviews', 
    'first_review', 'last_review', 'review_scores_rating', 
    'license', 'reviews_per_month', 'estimated_occupancy_l365d', 
    'estimated_revenue_l365d', 'number_of_reviews_ltm']

df = df[cols]

# Set to show ALL columns without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)  # Prevent line wrapping

# ========================
# Part 3: Null Values
# ========================

# drop the columns which contain too many nans
df.drop(columns=['license','host_about'], inplace=True)

# Count rows by N/A values
probs = df.isnull().sum(axis=1)

# Optionally create a histogram but do not display it
probs.plot.hist(bins=30).get_figure().clf()  # closes figure to prevent output

# drop rows with more than 5 nans
cutoff = 5
df.drop(probs[probs > cutoff].index, inplace=True)


# ==============================
# Part 5: Fixing Data Types
# ==============================

# Boolean type data
bools = ['host_is_superhost']
for b in bools:
    df[b] = df[b].replace({'f': False, 't': True}).astype('bool')
    
# Date type data
dates = ['last_scraped', 'host_since', 'first_review', 'last_review']
for d in dates:
    df[d] = pd.to_datetime(df[d])
    
# Categories type
cats = ['property_type', 'room_type']
for c in cats:
    df[c] = df[c].astype('category')
    
# Strings type (price)
money = ['price']
for m in money:
    try:
        df[m] = (
            df[m].astype(str)                       # force to string
                 .str.replace("$", "", regex=False) # remove dollar signs
                 .str.replace(",", "", regex=False) # remove commas
                 .astype(float)                     # convert to float
        )
    except (ValueError, AttributeError):
        pass  # silently ignore conversion errors
    
# Integer type
ints = ['id','host_id','host_listings_count','host_total_listings_count','accommodates',
        'beds','minimum_nights','maximum_nights','availability_365']
for i in ints:
    try:
        df[i] = df[i].astype('float').astype('int')
    except ValueError:
        df[i] = df[i].astype('float').astype(pd.UInt16Dtype())
        
# Reset index after cleaning
df.reset_index(drop=True, inplace=True)


# ==============================
# Part 6: Storing Cleaned Data
# ==============================
from pathlib import Path
import pandas as pd

# Define output paths
csv_out = Path("01_Data/Cleaned/listings.csv")
pq_out  = Path("01_Data/Cleaned/listings.parquet")
csv_out.parent.mkdir(parents=True, exist_ok=True)

# Save CSV
df.to_csv(csv_out, index=False, encoding="utf-8")

# Save Parquet using fastparquet
df.to_parquet(pq_out, engine="fastparquet", index=False)

# Load cleaned data silently
df_cleaned = pd.read_csv(csv_out)