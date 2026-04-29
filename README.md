[README_Forja_Data.md](https://github.com/user-attachments/files/27198755/README_Forja_Data.md)
#  Forja Data Pipeline

> Pipeline de données temps réel développé dans le cadre d'un **Projet de Fin d'Études (PFE)** avec la **SNRT** — Société Nationale de Radiodiffusion et de Télévision du Maroc.

---

##  C'est quoi Forja ?

Forja est un système qui collecte automatiquement les événements des utilisateurs de la plateforme SNRT (clics, visionnages, likes, abonnements...), les transforme et les stocke dans une base de données structurée pour pouvoir analyser le comportement des utilisateurs en quasi temps réel.

L'objectif final : avoir des **dashboards Grafana** qui permettent à l'équipe SNRT de voir en un coup d'œil ce qui se passe sur la plateforme — quels contenus cartonnent, quels utilisateurs sont actifs, quelles actions sont faites.

---

##  Pourquoi une V1 et une V2 ?

### V1 — La première approche (abandonnée)

La première version du pipeline passait par **MinIO** (un système de stockage de fichiers compatible S3) :


Kafka → Spark Streaming → MinIO (fichiers Parquet) → PostgreSQL


En théorie, c'était une architecture solide. En pratique, on a rencontré un problème récurrent : **les credentials d'authentification de MinIO expiraient sans prévenir**. Le pipeline s'arrêtait silencieusement, et on se retrouvait avec des **trous de plusieurs jours dans les données** — des journées entières d'événements utilisateurs perdues, sans aucune alerte.

### V2 — L'architecture actuelle (stable)

On a donc simplifié l'architecture en retirant MinIO complètement. La V2 va **directement de Kafka vers PostgreSQL**, sans intermédiaire fragile :


Kafka → Spark → PostgreSQL (Bronze → Silver → Gold)


Résultat : le pipeline est plus stable, plus facile à déboguer, et on ne dépend plus d'un système de stockage externe sujet aux coupures d'authentification.

---

##  Architecture V2 (actuelle)


┌─────────────────────────────────┐
│   Sources de données            │
│   GA4 (Google Analytics 4)      │
│   SNRT API (users, contenus...) │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Producteurs Kafka             │
│   ga4_producer.py               │
│   snrt_producer.py              │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Apache Kafka                  │
│   (tampon de messages)          │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Apache Spark Streaming        │
│   (traitement des événements)   │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│   PostgreSQL — Data Lake en 3 couches               │
│                                                     │
│   🟤 Bronze  → données brutes, telles qu'elles      │
│               arrivent depuis Kafka                  │
│                                                     │
│   🥈 Silver  → données nettoyées, dédupliquées,     │
│               enrichies avec jointures              │
│                                                     │
│   🥇 Gold    → KPIs agrégés par jour, prêts pour    │
│               les dashboards                        │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Grafana — Dashboards          │
│   Analyse quotidienne           │
│   Profils utilisateurs          │
└─────────────────────────────────┘
``

---

##  Les dashboards Grafana

Une fois le pipeline opérationnel, les données Gold alimentent deux dashboards principaux accessibles en live :

### Dashboard 1 — Analytiques quotidiennes

> **Forja – Gold V2 · Daily Analytics**

Visualise les métriques globales de la plateforme au quotidien : nombre de vues par contenu, volumes d'événements, tendances journalières, actions utilisateurs agrégées.

🔗 [Accéder au dashboard Daily Analytics](http://213.199.62.45:3001/d/forja-gold-v2/forja-e28093-gold-v2-daily-analytics?orgId=1&refresh=1h)
*(rafraîchissement automatique toutes les heures)*

---

### Dashboard 2 — Profil utilisateur

> **Forja – Profil Utilisateur**

Permet d'explorer le comportement d'un utilisateur individuel : historique de visionnage, actions effectuées (likes, favoris, abonnements), contenus préférés, et évolution dans le temps. Le dashboard accepte un `user_id` en paramètre pour zoomer sur n'importe quel profil.

🔗 [Accéder au dashboard Profil Utilisateur](http://213.199.62.45:3001/d/dfjus98lo5tdsa/forja-e28094-profil-utilisateur?orgId=1&from=1712707200000&to=1716336000000&var-user_id=2668)

---

## 🛠️ Stack technique

| Composant | Outil utilisé |
|---|---|
| Collecte des événements | Python (GA4 API, SNRT API) |
| Transport des messages | Apache Kafka + Zookeeper |
| Traitement streaming | Apache Spark Structured Streaming |
| Stockage | PostgreSQL |
| Orchestration | Apache Airflow |
| Visualisation | Grafana |
| Conteneurisation | Docker + Docker Compose |
| Langages | Python, SQL (PL/pgSQL) |

---

## ✅ Prérequis

Avant de commencer, assure-toi d'avoir :

- **Docker** et **Docker Compose** installés sur ta machine ou ton serveur
- Un fichier **Service Account JSON** pour l'API Google Analytics 4
- Les credentials d'accès à l'**API SNRT**
- Python 3.9+ si tu veux exécuter certains scripts en local

---

##  Installation

```bash
# 1. Cloner le projet
git clone https://github.com/Cyonecoder/Forja_Data.git
cd Forja_Data

# 2. Copier le fichier de configuration
cp .env.example .env

# 3. Remplir les variables dans .env
#    → credentials GA4, PostgreSQL, Kafka, etc.
```

---

## 🚀 Lancement

```bash
# 1. Démarrer toute l'infrastructure
docker compose up -d

# 2. Vérifier que tous les services sont bien up
docker compose ps

# 3. Démarrer le producer GA4 (collecte des événements)
docker restart ga4-producer

# 4. Lancer le job Spark (Kafka → PostgreSQL Bronze)
docker exec forja_spark_master spark-submit \
  /opt/spark_jobs/bronze_consumer.py
```

> Pour les étapes détaillées (initialisation des tables SQL, passage Silver/Gold, vérification qualité des données...), voir le fichier [GUIDE_LANCEMENT.md](./GUIDE_LANCEMENT.md).

---

## Structure du projet


Forja_Data/
│
├── producers/                  → Collecte des données
│   ├── ga4_producer.py         → Récupère les événements GA4 et les envoie à Kafka
│   ├── snrt_producer.py        → Récupère les données SNRT (users, contenus)
│   └── celery_tasks.py         → Tâches asynchrones
│
├── spark_jobs/                 → Traitement streaming
│   └── bronze_consumer.py      → Lit Kafka et insère dans PostgreSQL Bronze
│
├── spark_docker/               → Configuration Docker pour Spark
│
├── scripts/                    → Scripts utilitaires
│   ├── ingestion historique GA4
│   ├── ingestion sûre SNRT
│   └── contrôle qualité des données
│
├── sql/gold_v2/                → Toute la logique de transformation SQL
│   ├── 01_create_tables.sql                   → Création des tables Bronze
│   ├── 10_create_silver_watchings.sql         → Construction couche Silver
│   ├── 33_gold_daily_content_enriched.sql     → Agrégation contenu Gold
│   ├── 42_grafana_kpis.sql                    → KPIs finaux pour Grafana
│   ├── 06_check_missing_dates.sql             → Détection des jours manquants
│   └── 40_benchmark_raw_vs_gold.sql           → Comparaison volume brut vs Gold
│
├── tests/                      → Tests du pipeline
│
├── docker-compose.yml          → Infrastructure principale
├── docker-compose.airflow.yml  → Orchestration Airflow
├── .env.example                → Variables d'environnement à configurer
└── GUIDE_LANCEMENT.md          → Guide opérationnel complet pas-à-pas


---

##  Ce que le pipeline produit

Les tables Gold sont les tables "finales" utilisées dans Grafana. Voici les principales :

| Table Gold | Ce qu'elle contient |
|---|---|
| `gold_daily_content` | Nombre de vues par contenu par jour |
| `gold_daily_users` | Activité journalière des utilisateurs |
| `gold_daily_actions` | Likes, favoris, abonnements agrégés par jour |
| `gold_daily_ux_actions_v2` | Actions UX enrichies avec profils et catégories de contenu |

---

##  Qualité des données

Des scripts SQL dédiés permettent de surveiller l'intégrité du pipeline :

- **`06_check_missing_dates.sql`** → détecte les jours manquants dans les données (les fameux "gaps" de la V1)
- **`40_benchmark_raw_vs_gold.sql`** → compare les volumes bruts reçus depuis Kafka vs les volumes agrégés en Gold
- **`41_quality_compare_raw_vs_gold.sql`** → valide que les transformations Bronze → Silver → Gold sont cohérentes

---



*Projet réalisé dans le cadre d'un PFE - SNRT, Maroc.*
