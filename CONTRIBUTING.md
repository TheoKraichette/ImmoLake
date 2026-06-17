# Contribution

## Workflow Git (règle obligatoire)

- **Jamais de commit direct sur `main`.**
- **1 issue = 1 branche.** Avant de coder, partir d'un `main` à jour :
  ```
  git switch main && git pull
  git switch -c feat/<num>-<slug>      # ex: feat/1-hook-ademe
  ```
- Pousser la branche, ouvrir une **Pull Request** vers `main`, la faire **relire par un autre membre**, puis merger.
- Supprimer la branche après merge. Une PR = une issue (PR courtes).

## Nommage des branches

- `feat/<num>-<slug>` : nouvelle fonctionnalité
- `fix/<num>-<slug>` : correctif

## Definition of Done (MVP)

- le DAG concerné tourne en *success* ;
- transformations **idempotentes** (rejouer un run = même résultat) ;
- les tests passent (`make test`) ;
- aucun secret commité (`.env` reste local).
