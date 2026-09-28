# Microservices — Alertes de prix temps réel

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![asyncio](https://img.shields.io/badge/Python-asyncio-3776AB?style=for-the-badge)]()

> Architecture microservices événementielle sur **Apache Kafka** : un flux de prix crypto en temps réel (WebSocket) est publié sur un topic, consommé par plusieurs services indépendants (détection d'alertes, stockage, visualisation), avec une variante implémentant le **pattern Saga** pour gérer les transactions distribuées et leurs compensations.

## Architecture

```
┌─────────────────┐     ┌───────────┐     ┌──────────────────┐
│  WebSocket live   │ ──▶ │  producer   │ ──▶│   Kafka topic       │
│  (Polymarket)      │    │  .py         │    │   btc_price          │
└─────────────────┘     └───────────┘     └─────────┬────────┘
                                                             │
                       ┌────────────────────────────┼────────────────────────────┐
                       ▼                                     ▼                                     ▼
              ┌────────────────┐              ┌────────────────┐              ┌────────────────┐
              │  alert_service   │              │ consumer_storage │              │  consumer_graph   │
              │  détecte les       │              │  persiste les      │              │  visualise en       │
              │  variations >X%   │              │  prix                │              │  temps réel          │
              └────────┬───────┘              └────────────────┘              └────────────────┘
                                │
                                ▼
                       ┌────────────────┐
                       │  Kafka topic      │
                       │  btc_alerts        │
                       └────────────────┘
```

## Deux implémentations

### 1. Pipeline simple (`alert_service.py`)
Fenêtre glissante en mémoire : calcule la variation de prix sur les N dernières secondes et publie une alerte sur `btc_alerts` si le seuil est dépassé (`ALERT_PCT`, `ALERT_WINDOW_SEC`).

### 2. Pattern Saga (`*_saga.py`)
Décompose le traitement d'une alerte en étapes orchestrées par des messages Kafka (`alert_service_saga.py` → `storage_service_saga.py` → `notification_service_saga.py`), avec un service dédié de **compensation** (`storage_compensation_service.py`) qui annule/répare les effets d'une étape en cas d'échec plus loin dans la chaîne — le pattern de référence pour des transactions cohérentes réparties sur plusieurs services indépendants.

## Composants

| Fichier | Rôle |
|---|---|
| `producer.py` | Se connecte au WebSocket temps réel et publie chaque tick de prix sur Kafka |
| `alert_service.py` | Détecte les variations de prix significatives (fenêtre glissante) |
| `consumer_storage.py` | Persiste les prix reçus |
| `consumer_graph.py` | Consomme le flux et l'affiche graphiquement en direct (matplotlib) |
| `alert_service_saga.py` / `storage_service_saga.py` / `notification_service_saga.py` / `storage_compensation_service.py` | Variante Saga : chaîne d'étapes orchestrées avec compensation en cas d'échec |
| `docker-compose.yml` | Infrastructure Kafka + Zookeeper + Kafka UI |

## Lancer le projet

```bash
# 1. Démarrer l'infrastructure Kafka
docker compose up -d
# Kafka UI disponible sur http://localhost:9000

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Lancer le pipeline (dans des terminaux séparés)
python producer.py
python alert_service.py
python consumer_storage.py
```

La configuration (broker, topics, seuils d'alerte) se fait via `.env` — voir `.env.example`.

## Ce que ce projet démontre

- Conception d'une **architecture événementielle** découplée avec Kafka (producteurs/consommateurs indépendants)
- Traitement de flux en temps réel avec fenêtre glissante (`asyncio`, `aiokafka`)
- Implémentation du **pattern Saga** pour la cohérence transactionnelle distribuée, compensation incluse
- Orchestration d'une infrastructure multi-services avec Docker Compose

---

*Projet académique — architecture microservices et traitement de flux.*
