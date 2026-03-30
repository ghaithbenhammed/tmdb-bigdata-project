import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")

st.title("🎬 TMDB Analytics Dashboard")

# =========================
# LOAD FUNCTION
# =========================

def read_parquet(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                return pd.read_parquet(os.path.join(root, file))

# =========================
# LOAD DATA
# =========================

df_genres = read_parquet("data/gold/top_genres/")
df_movies = read_parquet("data/gold/top_movies_global/")
df_stats = read_parquet("data/gold/stats_year/")

# =========================
# SIDEBAR FILTERS
# =========================

st.sidebar.header("🎯 Filtres")

years = sorted(df_stats["year"].unique())
selected_year = st.sidebar.selectbox("Année", years)

genres = df_genres["genre"].unique()
selected_genre = st.sidebar.selectbox("Genre", genres)

# =========================
# FILTER DATA
# =========================

df_movies_filtered = df_movies[df_movies["year"] == selected_year]
df_movies_filtered = df_movies_filtered[df_movies_filtered["genre"] == selected_genre]

df_stats_sorted = df_stats.sort_values("year")
df_genres_sorted = df_genres.sort_values("avg_score", ascending=False)

# =========================
# KPIs
# =========================

st.subheader("📊 Indicateurs Clés")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🎥 Nombre de films", len(df_movies_filtered))

with col2:
    st.metric("⭐ Score moyen global", round(df_stats["avg_score"].mean(), 2))

with col3:
    best_genre = df_genres_sorted.iloc[0]["genre"]
    st.metric("🏆 Meilleur genre", best_genre)

# =========================
# CHARTS
# =========================

col4, col5 = st.columns(2)

# TOP GENRES
with col4:
    st.subheader("🏆 Genres les plus performants")
    st.bar_chart(df_genres_sorted.set_index("genre")["avg_score"].head(10))

# =========================
# TOP MOVIES (PRO VERSION)
# =========================

with col5:
    st.subheader("🎥 Top Films")

    top_movies = df_movies_filtered.sort_values("performance_score", ascending=False).head(10)

    for i, row in top_movies.iterrows():
        st.markdown(f"### 🎬 {row['title']}")
        
        colA, colB = st.columns([1,3])
        
        with colA:
            st.metric("Score", round(float(row["performance_score"]), 2))
        
        with colB:
            st.progress(min(float(row["performance_score"]) / 10, 1.0))
        
        st.markdown("---")

    # graphique ranking
    st.subheader("🏆 Classement des films")
    chart_data = top_movies[["title", "performance_score"]].set_index("title")
    st.bar_chart(chart_data)

# =========================
# EVOLUTION
# =========================

st.subheader("📈 Evolution des performances dans le temps")
st.line_chart(df_stats_sorted.set_index("year")["avg_score"])

# =========================
# DISTRIBUTION
# =========================

st.subheader("🎭 Répartition des films par genre")

genre_counts = df_genres.groupby("genre")["nb_films"].sum()
st.bar_chart(genre_counts)