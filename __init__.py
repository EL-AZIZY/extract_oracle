"""
oracle_extract
==============
Package d'extraction Oracle → CSV.

Modules :
    settings      Chargement et validation des fichiers de configuration
    logger        Initialisation des logs (global + par job)
    security      Vérification et sécurisation des fichiers sensibles
    connection    Connexion à la base Oracle
    query_loader  Chargement des requêtes SQL depuis les fichiers .sql
    extractor     Extraction d'une requête vers un fichier CSV
    runner        Orchestration de l'ensemble des jobs
"""
