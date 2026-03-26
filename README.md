# TMDB Big Data Project (2021–2025)

## Problématique

Comment identifier les genres et profils de films les plus performants entre 2021 et 2025 afin d’optimiser la stratégie d’investissement d’une plateforme de streaming ?

---

## Architecture Médaillon

### RAW (Ingestion)

- Lecture du dataset TMDB (parquet)
- Stockage partitionné par date (year/month/day)
- Script : `feeder.py`

---

### SILVER (Processing)

- Nettoyage des données (validation)
- Transformation des colonnes
- Calcul d’un score de performance
- Jointure avec un fichier JSON (multi-source)
- Analyse avec Window Function (Top films par année)

Script : `processor.py`

---

## Score Business

performance_score = vote_average \* log(vote_count + 1)

Ce score permet d’évaluer la performance réelle d’un film en combinant sa note et sa popularité.

---

## Structure du projet

- data/
  - raw/
  - silver/

- scripts/
  - feeder.py
  - processor.py

- logs/
  - feeder.log
  - processor.log

---

## Technologies utilisées

- PySpark
- Parquet
- JSON
- Python

---

## Travail réalisé

- Ingestion des données (RAW)
- Validation et nettoyage
- Transformation des données
- Calcul de métriques business
- Jointure multi-source (parquet + JSON)
- Analyse avec Window Function
- Mise en place de logs

---

## Travail restant

- Création du datamart (GOLD layer)
- Intégration PostgreSQL
- Développement API (FastAPI)
- Création dashboard (Streamlit)

---

## Organisation

Projet réalisé en groupe de 3 :

- Data Engineering (RAW + SILVER)
- Data Storage (PostgreSQL) "A faire"
- API & Dashboard " A faire"

---

## Résultat

Ce projet permet d’identifier :

- les films les plus performants
- les tendances par genre
- les évolutions temporelles

---

## Auteur

Projet académique - Big Data
