import pandas as pd
import numpy as np
import os


df = pd.read_csv("netflix_titles.csv")

df.columns = df.columns.str.strip()

missing_percent = (df.isna().mean() * 100).round(2)

drop_cols = missing_percent[missing_percent > 50].index.tolist()
df.drop(columns=drop_cols, inplace=True)


impute_flag_cols = missing_percent[(missing_percent > 10) & (missing_percent <= 50)].index.tolist()
for col in impute_flag_cols:
    df[col + "_missing_flag"] = df[col].isna().astype(int)


safe_cols = missing_percent[missing_percent <= 10].index.tolist()
numeric_cols = df.select_dtypes(include=[np.number]).columns
text_cols = df.select_dtypes(include="object").columns

for col in safe_cols:
    if col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())
    elif col in text_cols:
        df[col] = df[col].fillna("Unknown")


df = df.drop_duplicates()


text_clean_cols = ['title', 'director', 'cast', 'country', 'listed_in']
for col in text_clean_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.lower().str.strip()


if 'country' in df.columns:
    df['country'] = df['country'].replace({
        'us': 'united states',
        'usa': 'united states',
        'u.s.a.': 'united states',
        'uk': 'united kingdom'
    })
    df['country'] = df['country'].str.strip().str.title()


if 'rating' in df.columns:
    df['rating'] = df['rating'].fillna("not rated")


if 'date_added' in df.columns:
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['year_added'] = df['date_added'].dt.year
    df['month_added'] = df['date_added'].dt.month


if 'duration' in df.columns:
    df["duration_num"] = df["duration"].str.extract(r"(\d+)").astype(float)
    df["duration_type"] = np.where(df["duration"].str.contains("Season", na=False), "season", "minutes")
    df["season_count"] = np.where(df["duration_type"] == "season", df["duration_num"], np.nan)
    df["movie_minutes"] = np.where(df["duration_type"] == "minutes", df["duration_num"], np.nan)
    df["log_duration"] = np.log(df["duration_num"] + 1)


current_year = pd.Timestamp.now().year
if 'release_year' in df.columns:
    df.loc[(df["release_year"] < 1900) | (df["release_year"] > current_year), "release_year"] = np.nan
    df["years_since_release"] = current_year - df["release_year"]


if 'country' in df.columns:
    df["country_count"] = df['country'].str.split(",").apply(len)
if 'listed_in' in df.columns:
    df["genre_count"] = df['listed_in'].str.split(",").apply(len)
if 'director' in df.columns:
    df["num_directors"] = df["director"].fillna("").str.split(",").apply(lambda x: len(x) if x != [''] else 0)
if 'cast' in df.columns:
    df["num_cast_members"] = df["cast"].str.split(",").apply(len)
if 'type' in df.columns:
    df["is_tv_show"] = np.where(df["type"].str.lower() == "tv show", 1, 0)


df.columns = df.columns.str.strip()

df.to_csv("netflix_cleaned_mysql.csv", index=False, encoding='utf-8-sig')


print("CSV ready for MySQL import!")
print("Columns in CSV:", df.columns.tolist())
print("Current working directory:", os.getcwd())
print("Number of rows:", len(df))

import mysql.connector


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="netflix_analysis"
)

cursor = conn.cursor()
df = df.replace({np.nan: None})


sql = """
INSERT INTO netflix_titles (
show_id,type,title,director,cast,country,date_added,
release_year,rating,duration,listed_in,description,
director_missing_flag,year_added,month_added,duration_num,
duration_type,season_count,movie_minutes,country_count,
genre_count,num_directors,num_cast_members,is_tv_show,
years_since_release,log_duration
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

for _, row in df.iterrows():
    cursor.execute(sql, tuple(row))


conn.commit()

cursor.close()
conn.close()

print("Data successfully inserted into MySQL!")