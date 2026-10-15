# Job Intelligent : pipeline ETL et Data Warehouse d'offres d'emploi Data/IA

Pipeline de données qui centralise des offres d'emploi du domaine **Data / IA** issues de plusieurs sources (Kaggle, France Travail, Rekrute), les nettoie, puis les charge dans un **Data Warehouse PostgreSQL en schéma en étoile**. L'orchestration est assurée par **Apache Airflow** et l'ensemble tourne dans **Docker**.

## Architecture

```
 Kaggle (CSV)         ──┐
 France Travail (API) ──┼──> data/raw ──> EXTRACT ──> TRANSFORM ──> LOAD ──> PostgreSQL (DWH)
 Rekrute (scraping)   ──┘                 (Airflow)   (pandas)               schéma en étoile
```

| Service | Rôle | Port |
|---|---|---|
| `postgres-airflow` | Metastore Airflow | 5434 |
| `postgres-dwh` | Data Warehouse du projet | 5433 |
| `airflow-webserver` | Interface web Airflow | 8080 |
| `airflow-scheduler` | Planificateur des DAGs | n/a |

## Sources de données

| Source | Pays | Mode d'extraction | Fichier produit |
|---|---|---|---|
| Kaggle (offres LinkedIn) | International | Fichier CSV | `data/raw/kaggle_job_postings.csv` |
| France Travail | France | API REST (OAuth2) | `data/raw/france_travail_jobs.csv` |
| Rekrute | Maroc | Scraping HTML (BeautifulSoup) | `data/raw/rekrute_jobs.csv` |

## Pipeline Airflow

| DAG | Rôle |
|---|---|
| `test_environment` | Vérifie l'environnement et la connexion au DWH |
| `init_dwh` | Crée les tables du schéma en étoile et la table de référence `dim_source` (**attention : fait des `DROP TABLE`**) |
| `etl_jobs_pipeline` | Pipeline principal, déclenché manuellement |

`etl_jobs_pipeline` (API TaskFlow) enchaîne :

1. **Extract** : `extract_kaggle`, `extract_france_travail`, `extract_rekrute` standardisent chaque source vers un format commun (`id_offre`, `titre`, `entreprise`, `ville`, `type_contrat`, `description`, `date_publication`, `source`).
2. **Transform** : `transform_data` fusionne les sources, supprime les doublons et les titres vides, remplace les valeurs manquantes, tronque les descriptions et déduit le pays.
3. **Load** : `load_to_dwh` insère dimensions et faits avec `ON CONFLICT`, donc le chargement est **idempotent** (relancer le DAG ne crée pas de doublons).

## Modèle du Data Warehouse

Schéma en étoile (méthode Kimball) : une table de faits et cinq dimensions.

- **`fact_offres`** : une ligne par offre (type de contrat, description, salaires min/max, date de chargement)
- **`dim_entreprise`**, **`dim_localisation`**, **`dim_metier`**, **`dim_date`**, **`dim_source`**

Détails dans [`scripts/sql/00_modele_dwh.md`](scripts/sql/00_modele_dwh.md) et [`scripts/sql/PHASE_4_DOCUMENTATION.md`](scripts/sql/PHASE_4_DOCUMENTATION.md). Le script de création est dans [`scripts/sql/01_create_tables.sql`](scripts/sql/01_create_tables.sql).

## Technologies

Python, Apache Airflow 2.6, PostgreSQL 13, Docker Compose, pandas, Requests, BeautifulSoup, python-dotenv.

## Structure du projet

```
.
├── dags/
│   ├── test_environment.py        # Test de l'environnement
│   ├── init_dwh.py                # Création du DWH
│   └── etl_jobs_pipeline.py       # Pipeline ETL principal
├── scripts/
│   ├── scrapers/
│   │   ├── scrape_rekrute.py      # Scraper Rekrute
│   │   └── test_france_travail.py # Appel API France Travail
│   └── sql/                       # Schéma et documentation du DWH
├── data/                          # raw/ et processed/ (non versionnés)
├── notebooks/  plugins/           # Dossiers réservés
├── docker-compose.yml
└── .env.example                   # Modèle de configuration
```

## Installation

### Prérequis

- Docker et Docker Compose
- Un compte développeur [France Travail](https://francetravail.io/) pour obtenir un `client_id` et un `client_secret`

### 1. Cloner et configurer

```bash
git clone https://github.com/Hasnae112/job-intelligent.git
cd job-intelligent
cp .env.example .env      # puis renseigner vos valeurs
```

### 2. Préparer les données

Les fichiers de `data/raw/` ne sont pas versionnés (taille). Ils sont à générer ou à télécharger :

```bash
pip install requests beautifulsoup4 pandas python-dotenv
python scripts/scrapers/scrape_rekrute.py        # -> data/raw/rekrute_jobs.csv
python scripts/scrapers/test_france_travail.py   # -> data/raw/france_travail_jobs.csv
```

Pour Kaggle, télécharger le jeu de données d'offres LinkedIn (`job_postings`, `job_skills`, `job_summary`) et placer les CSV dans `data/raw/`.

### 3. Lancer la stack

```bash
docker compose up airflow-init       # première exécution seulement
docker compose up -d
```

Interface Airflow : http://localhost:8080 (utilisateur `admin`, mot de passe `admin`, **réservé à un usage local**).

### 4. Connexion au Data Warehouse dans Airflow

Dans **Admin → Connections**, créer la connexion `postgres_dwh` :

| Champ | Valeur |
|---|---|
| Type | Postgres |
| Host | `postgres-dwh` |
| Schema | valeur de `POSTGRES_DWH_DB` |
| Login / Password | valeurs de `POSTGRES_DWH_USER` / `POSTGRES_DWH_PASSWORD` |
| Port | `5432` |

### 5. Exécuter les DAGs

Dans l'interface Airflow, activer puis déclencher dans cet ordre :

1. `test_environment`
2. `init_dwh` (une seule fois)
3. `etl_jobs_pipeline`

### 6. Vérifier le résultat

```bash
docker exec -it postgres_dwh_jobint psql -U dwh_user -d jobs_dwh \
  -c "SELECT s.nom_source, COUNT(*) FROM fact_offres f JOIN dim_source s USING (id_source) GROUP BY 1;"
```

## Sécurité

- Ne jamais committer `.env` : il est dans `.gitignore`.
- Les identifiants Airflow (`admin/admin`) et la `SECRET_KEY` du `docker-compose.yml` sont prévus pour un **environnement local de développement** uniquement.
- Respecter les conditions d'utilisation de chaque source (API France Travail, scraping de Rekrute) et ne pas redistribuer les données collectées.

## Pistes d'amélioration

- Ajouter d'autres sources (par exemple Adzuna) et un Data Lake par couches (Bronze / Silver / Gold)
- Extraction de compétences pour alimenter `dim_metier`
- Tableaux de bord de reporting (Power BI)
- Planification automatique des DAGs et tests de qualité des données

## Auteure

**Hasnae Elballaoui**, élève-ingénieure en Intelligence Artificielle et Transformation Digitale (ENSAH)
[LinkedIn](https://linkedin.com/in/hasnae-elballaoui-312209256) · [GitHub](https://github.com/Hasnae112)
