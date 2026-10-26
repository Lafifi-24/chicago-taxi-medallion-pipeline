# Test technique Data Engineer


---

## 1. Contexte & objectif

Notre équipe data chez Artefact construit et opère des plateformes de type *datalake* pour ses clients. Ce test a pour but d'évaluer ta capacité à **concevoir et implémenter un pipeline de données de bout en bout**, orchestré, reproductible et fonctionnel en local.

On ne cherche pas la solution la plus exhaustive possible, mais un projet **propre, structuré et qui tourne**. La qualité de l'architecture et de l'organisation du code compte autant que le résultat final.

---

## 2. Le sujet

Tu rejoins l'équipe data d'une entreprise de mobilité urbaine. On te confie la mise en place d'un pipeline qui doit :

1. **ingérer** des données brutes de trajets (fichiers sources) dans le datalake ;
2. **nettoyer et transformer** ces données pour en faire des tables analytiques ;
3. **exposer** des agrégats prêts à l'emploi pour l'équipe BI (ex. revenus par jour, durée moyenne des trajets, top zones de prise en charge).

Le tout doit être orchestré par un orchestrateur de ton choix (Dagster, Airflow?) et le *processing* réalisé avec **Spark (PySpark)**.

---

## 3. Le jeu de données

On te propose le dataset public **Chicago Taxi Trips**, fourni au format CSV.

- Portail officiel : https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew
- Le fichier complet est gros (plusieurs dizaines de Go), ne le télécharge pas en entier : récupère une tranche exploitable en local (ex. un trimestre, quelques millions de lignes) via l'API SODA, en filtrant sur `trip_start_timestamp` et en paginant. Vise un volume raisonnable.
- Documente dans le README la période choisie et la commande/le script de téléchargement, pour que le pipeline reste **reproductible**.

---

## 4. Architecture attendue

On attend une organisation en couches de type **médaillon** (raw → clean → aggregated), avec le stockage objet comme socle du datalake :

```
                 ┌─────────────┐
   Sources  ───► │   BRONZE    │  données brutes ingérées telles quelles
   (CSV)         │    (raw)    │  (partitionnées, non modifiées)
                 └──────┬──────┘
                        │  Spark : parsing, typage, dédup, nettoyage
                        ▼
                 ┌─────────────┐
                 │   SILVER    │  données nettoyées, normalisées,
                 │  (cleaned)  │  qualité contrôlée
                 └──────┬──────┘
                        │  Spark : jointures, agrégations métier
                        ▼
                 ┌─────────────┐
                 │    GOLD     │  tables analytiques / KPIs
                 │ (curated)   │  prêtes pour la BI
                 └──────┬──────┘
                        │
                        ▼
                 Couche d'exposition (SQL / API / dashboard)
```

Chaque couche est matérialisée dans le stockage objet (voir stack ci-dessous). L'ensemble est **orchestré par un ou plusieurs DAGs**.

---

## 5. Étapes à implémenter

Tu es libre d'implémenter comme bon te semble, en te basant sur ta propre expérience.

---

## 6. Stack technique

**Imposée :**
- **Airflow ou Dagster** pour l'orchestration ;
- **Spark / PySpark** pour le processing ;
- **Docker Compose** pour que tout tourne en local via une seule commande.


L'objectif : un `docker compose up` (éventuellement suivi d'un déclenchement de DAG) doit suffire à faire tourner le pipeline de bout en bout.

---

## 7. Livrables

1. Un **repo Git** contenant l'intégralité du code.
2. Un **README** décrivant :
   - le prérequis et la commande de lancement (pas à pas) ;
   - un **schéma d'architecture** (même simple) ;
   - tes choix techniques et tes arbitrages ;
   - ce que tu aurais fait avec plus de temps.
3. Le pipeline **fonctionnel en local**.

---
## 8. Optionnel
Toute autre proposition est la bienvenue.
---
Good luck!
