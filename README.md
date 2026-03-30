# TMDB Big Data Project

## Groupe

- Ghaith BEN HAMMED
- Chaïma ASTITOU
- Ben SHUM

1ère année Mastère Data Engineering & Intelligence Artificielle (M1-DE2)

---

## Contexte

Dans un contexte de plateforme de streaming, il est essentiel de comprendre les performances des films afin d’optimiser les décisions business.

Ce projet repose sur l’analyse des données TMDB (The Movie Database) entre 2021 et 2025.

---

## Objectif Business

L’objectif est d’identifier :

- Les films les plus performants
- Les genres les plus populaires
- Les tendances d’évolution par année

Ces analyses permettent d’aider une plateforme de streaming à :

- Optimiser son catalogue
- Améliorer ses recommandations
- Maximiser sa rentabilité

---

## Architecture Data (Medallion)

Le projet suit une architecture en 3 couches :

### RAW

- Données brutes (format Parquet)
- Aucune transformation

### SILVER

- Nettoyage et validation des données
- Suppression des valeurs nulles ou incohérentes
- Ajout de nouvelles colonnes :
  - année (year)
  - genres transformés
  - performance_score

### GOLD

- Données analytiques prêtes à être utilisées :
  - Top films
  - Top genres
  - Statistiques par année

---

## Pipeline Data (Apache Spark)

### Ingestion (RAW)

spark-submit scripts/feeder.py

### Processing (SILVER)

spark-submit scripts/processor.py

### Datamart (GOLD)

spark-submit scripts/datamart.py

## API (FastAPI)

### Lancement :

uvicorn api.main:app --reload

### Accès à la documentation :

http://127.0.0.1:8000/docs

### Endpoints disponibles :

/top-movies
/top-genres
/stats-year

## Dashboard (Streamlit)

### Lancement :

streamlit run dashboard/app.py

### Fonctionnalités :

Visualisation des meilleurs films
Analyse des genres les plus populaires
Filtrage par année et genre
Dashboard interactif

## Structure du projet

tmdb-bigdata-project/
│
├── data/
│ ├── raw/
│ ├── silver/
│ └── gold/
│
├── scripts/
│ ├── feeder.py
│ ├── processor.py
│ └── datamart.py
│
├── api/
│ └── main.py
│
├── dashboard/
│ └── app.py
│
├── logs/
│
└── README.md

## Technologies utilisées

Apache Spark
Python
FastAPI
Streamlit
Pandas
PyArrow

## Résultats

Ce projet permet de :

Identifier les films les plus performants
Analyser les tendances du marché
Visualiser les données via un dashboard interactif
Exposer les données via une API

## Conclusion

Ce projet met en place une architecture Big Data complète permettant :

le traitement de données volumineuses
la création d’indicateurs business
la visualisation interactive

Il répond aux besoins d’analyse d’une plateforme de streaming moderne.
