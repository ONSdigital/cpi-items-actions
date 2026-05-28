"""
Python script equivalent of manual-process.ipynb (local/manual workflow using test data).
Generates output files for comparison with the R notebook.
"""
import pandas as pd
import re
import io
from datetime import datetime, date

# --- Save timestamp ---
with open('timestamp.txt', 'w') as f:
    f.write(datetime.now().strftime("%Y-%b-%d"))

# --- Create metadata and unchained files from starting file ---
df_meta = pd.read_csv('2026_starting_file_test_data_v2.csv', skiprows=2)

if 'AVERAGE_PRICE' in df_meta.columns:
    last_col_idx = df_meta.columns.get_loc('AVERAGE_PRICE')
    df_meta = df_meta.iloc[:, :last_col_idx + 1]

df_meta.to_csv('2026_all_items_metadata.csv', index=False)

df_meta_deduped = df_meta.drop(columns=['ID_NAME'])
if 'CONSUMPTION_SEGMENT_CODE' in df_meta_deduped.columns:
    cols = ['CONSUMPTION_SEGMENT_CODE'] + [col for col in df_meta_deduped.columns if col != 'CONSUMPTION_SEGMENT_CODE']
    df_meta_deduped = df_meta_deduped[cols]
df_meta_deduped = df_meta_deduped.drop_duplicates(subset=['CONSUMPTION_SEGMENT_CODE'])
df_meta_deduped.to_csv('2025_metadata.csv', index=False)

df_start = pd.read_csv('2026_starting_file_test_data_v2.csv', skiprows=2)
cols_to_keep = ['CONSUMPTION_SEGMENT_CODE'] + [
    col for col in df_start.columns
    if re.match(r'^2021\d{2}$', col) or (re.match(r'^20\d{4}$', col) and col >= '202101')
]
unchained2025 = df_start[cols_to_keep].drop_duplicates(subset=['CONSUMPTION_SEGMENT_CODE'])
unchained2025 = unchained2025.replace('-', '', regex=True)
unchained2025.to_csv('2025_unchained.csv', index=False)

# --- Reference months ---
avgpriceRefMonth = pd.Timestamp('2026-01-01 00:00:00')
startref = pd.Timestamp('2022-01-01 00:00:00')

# --- Read metadata ---
meta = pd.read_csv('./2025_metadata.csv', index_col=0, parse_dates=['ID_START'], date_format="%Y%m")

def split(strng, sep, pos):
    strng = strng.split(sep)
    return sep.join(strng[:pos]), sep.join(strng[pos:])

# --- Read unchained ---
unchained = pd.read_csv('2025_unchained.csv')
latestmonth = datetime.strptime(unchained.columns[-1], "%Y%m")
print('Latest month in unchained:', latestmonth)

# --- Merge test data (manual/local workflow) ---
df = pd.read_csv('march_2026_example_test_data_v1.csv')
index_date = str(int(df.iloc[0, 0]))

columns = {}
for col in unchained.columns:
    try:
        columns[col] = datetime.strptime(str(col), "%Y%m")
    except ValueError:
        pass
unchained.rename(columns=columns, inplace=True)

un = unchained.merge(
    df[['CS_ID', 'CPI_INDEX']].rename(columns={"CPI_INDEX": datetime.strptime(index_date, "%Y%m")}),
    left_on="CONSUMPTION_SEGMENT_CODE",
    right_on='CS_ID',
    how='left'
)
un = un.drop(columns=["CS_ID"])
un.set_index("CONSUMPTION_SEGMENT_CODE", inplace=True)

renamed_cols = {}
for col in un.columns:
    if isinstance(col, str) and re.fullmatch(r"\d{6}", col):
        renamed_cols[col] = datetime.strptime(col, "%Y%m")
if renamed_cols:
    un.rename(columns=renamed_cols, inplace=True)

date_cols = [c for c in un.columns if isinstance(c, (datetime, date, pd.Timestamp))]
if date_cols:
    last_date_col = max(date_cols)
    if last_date_col.month == 1:
        print('chaining jan')
        jancol = last_date_col
        prev_dates = [c for c in date_cols if c < jancol]
        if prev_dates:
            prevdec = max(prev_dates)
            for index, value in un[jancol].items():
                un.at[index, jancol] = un.loc[index, prevdec] * value / 100

# --- Chaining ---
chained = un.copy()

for col in chained:
    for i, row_value in chained[col].items():
        if col >= meta.loc[i, 'ID_START']:
            if col == startref:
                chained.at[i, col] = 100
            elif col <= startref + pd.tseries.offsets.DateOffset(years=1):
                chained.at[i, col] = row_value
            else:
                if col.month == 1 and col > startref + pd.tseries.offsets.DateOffset(years=1):
                    chained.at[i, col] = float(row_value) * float(chained.loc[i][datetime(col.year - 1, 1, 1)]) / 100
                else:
                    chained.at[i, col] = float(row_value) * float(chained.loc[i][datetime(col.year, 1, 1)]) / 100
        elif col == meta.loc[i, 'ID_START'] - pd.tseries.offsets.DateOffset(months=1):
            chained.at[i, col] = 100
        else:
            chained.at[i, col] = None

# --- Save unchained and chained ---
columns = {}
for col in un.columns:
    try:
        columns[col] = col.date()
    except (ValueError, AttributeError):
        pass
un.rename(columns=columns, inplace=True)
un.to_csv('unchained.csv')

columns = {}
for col in chained.columns:
    try:
        columns[col] = col.date()
    except (ValueError, AttributeError):
        pass
chained.rename(columns=columns, inplace=True)
chained.astype(float).round(3).to_csv('chained.csv', date_format='%Y-%m-%d', na_rep='')

# --- Average prices merged ---
allitems = pd.read_csv('2026_all_items_metadata.csv')
if chained.index.name != 'CONSUMPTION_SEGMENT_CODE':
    chained = chained.reset_index().set_index('CONSUMPTION_SEGMENT_CODE')

avgprice_merged = allitems[['ID_NAME', 'CONSUMPTION_SEGMENT_CODE']].copy()
for col in chained.columns:
    avgprice_merged[str(col)] = avgprice_merged['CONSUMPTION_SEGMENT_CODE'].map(
        lambda seg: round((chained.loc[seg, col] / chained.loc[seg, chained.columns[0]] * allitems.loc[allitems['CONSUMPTION_SEGMENT_CODE'] == seg, 'AVERAGE_PRICE'].values[0]), 2)
        if seg in chained.index and not allitems.loc[allitems['CONSUMPTION_SEGMENT_CODE'] == seg, 'AVERAGE_PRICE'].empty else None
    )
avgprice_merged.to_csv('avgprice_merged.csv', index=False)

# --- Monthly growth merged ---
import math
allitems = pd.read_csv('2026_all_items_metadata.csv')
if chained.index.name != 'CONSUMPTION_SEGMENT_CODE':
    chained = chained.reset_index().set_index('CONSUMPTION_SEGMENT_CODE')

monthly_growth = allitems[['ID_NAME', 'CONSUMPTION_SEGMENT_CODE']].copy()
for idx, col in enumerate(list(chained.columns)[1:], start=1):
    prev_col = chained.columns[idx - 1]
    def calc_growth(seg):
        try:
            prev = chained.loc[seg, prev_col]
            curr = chained.loc[seg, col]
            if pd.isna(prev) or pd.isna(curr) or prev == 0:
                return None
            return int(round((curr - prev) * 100 / prev))
        except Exception:
            return None
    monthly_growth[str(col)] = monthly_growth['CONSUMPTION_SEGMENT_CODE'].map(calc_growth)
monthly_growth.to_csv('monthly_growth_merged.csv', index=False)

# --- Annual growth merged ---
allitems = pd.read_csv('2026_all_items_metadata.csv')
if chained.index.name != 'CONSUMPTION_SEGMENT_CODE':
    chained = chained.reset_index().set_index('CONSUMPTION_SEGMENT_CODE')

annual_growth = allitems[['ID_NAME', 'CONSUMPTION_SEGMENT_CODE']].copy()
for idx, col in enumerate(list(chained.columns)[12:], start=12):
    prev_col = chained.columns[idx - 12]
    def calc_annual_growth(seg):
        try:
            prev = chained.loc[seg, prev_col]
            curr = chained.loc[seg, col]
            if pd.isna(prev) or pd.isna(curr) or prev == 0:
                return None
            return int(round((curr - prev) * 100 / prev))
        except Exception:
            return None
    annual_growth[str(col)] = annual_growth['CONSUMPTION_SEGMENT_CODE'].map(calc_annual_growth)
annual_growth.to_csv('annual_growth_merged.csv', index=False)

# --- Excel datadownload ---
meta_for_datadownload = pd.read_csv('2026_all_items_metadata.csv')

with pd.ExcelWriter("datadownload.xlsx", mode="a", if_sheet_exists="replace",
                    date_format="YYYY-MM-DD", datetime_format="YYYY-MM-DD") as writer:
    meta_for_datadownload.drop(columns=["AVERAGE_PRICE"]).to_excel(writer, index=False, sheet_name="Metadata")

    chained.round(3).reset_index().melt(id_vars=['CONSUMPTION_SEGMENT_CODE'], var_name='Date', value_name='Value').dropna() \
        .merge(meta_for_datadownload.reset_index()[["ID_NAME", 'CONSUMPTION_SEGMENT_CODE', 'Category1', 'Category2', 'WEIGHT\\SIZE']]) \
        .iloc[:, [1, 3, 0, 4, 5, 6, 2]] \
        .to_excel(writer, index=False, sheet_name="Chained")

    avgprice_merged.round(2).melt(id_vars=["ID_NAME", 'CONSUMPTION_SEGMENT_CODE'], var_name='Date', value_name='Price').dropna(subset=['Price']) \
        .merge(meta_for_datadownload.reset_index()[["ID_NAME", 'CONSUMPTION_SEGMENT_CODE', 'Category1', 'Category2', 'WEIGHT\\SIZE']]) \
        .iloc[:, [2, 0, 1, 4, 5, 6, 3]] \
        .to_excel(writer, index=False, sheet_name="Average price")

    monthly_growth.round(0).melt(id_vars=["ID_NAME", 'CONSUMPTION_SEGMENT_CODE'], var_name='Date', value_name='Percentage').dropna(subset=['Percentage']) \
        .merge(meta_for_datadownload.reset_index()[["ID_NAME", 'CONSUMPTION_SEGMENT_CODE', 'Category1', 'Category2', 'WEIGHT\\SIZE']]) \
        .iloc[:, [2, 0, 1, 4, 5, 6, 3]] \
        .to_excel(writer, index=False, sheet_name="Monthly growth")

    annual_growth.round(0).melt(id_vars=["ID_NAME", 'CONSUMPTION_SEGMENT_CODE'], var_name='Date', value_name='Percentage').dropna(subset=['Percentage']) \
        .merge(meta_for_datadownload.reset_index()[["ID_NAME", 'CONSUMPTION_SEGMENT_CODE', 'Category1', 'Category2', 'WEIGHT\\SIZE']]) \
        .iloc[:, [2, 0, 1, 4, 5, 6, 3]] \
        .to_excel(writer, index=False, sheet_name="Annual growth")

print("Python process complete. Output files: unchained.csv, chained.csv, avgprice_merged.csv, monthly_growth_merged.csv, annual_growth_merged.csv, datadownload.xlsx")
