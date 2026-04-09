# Multi-Model Training Upgrade Ticket Backlog

## Current Constraints

- The training API still runs one hard-coded `RandomForest` pipeline and stores one final artifact.
- The training request already sends `intRandomState`, `intTopFeats`, and `fltF1`, but the backend does not use them.
- Model history is stored as generic metrics only, without model family, hyperparameters, artifact path, run grouping, or champion status.
- Inference can only load one saved model and has no way to select a model by ID, run, or family.
- The UI already has placeholder copy for model-family selection and comparisons, but there is no backend contract behind it.
- There is no migration framework or automated test suite in the repo today, so schema and contract changes need explicit rollout work.

## Recommended Build Order

1. MM-01
2. MM-02
3. MM-04
4. MM-05
5. MM-03
6. MM-06
7. MM-07
8. MM-08
9. MM-09
10. MM-10
11. MM-11
12. MM-12

## Tickets

### MM-01: Define the multi-model training contract

Summary: Create the API contract for training more than one model in a single run.

Scope:
- Extend the training request to accept model families, ranking metric, optional hyperparameters, and feature-importance settings.
- Extend the training response to return candidate-model results, selected champion model, run metadata, and normalized feature-importance output.
- Decide whether this stays on `POST /train/model` or becomes a versioned endpoint.

Done when:
- Backend and frontend use one shared request and response shape for multi-model runs.
- The contract supports both multi-model runs and a backward-compatible single-model fallback.

### MM-02: Refactor training into a model registry plus shared preprocessing

Summary: Split the current `ChurnPredictionModel` into reusable preprocessing and pluggable estimator-specific trainers.

Scope:
- Extract the shared feature-engineering pipeline from the hard-coded `RandomForest` flow.
- Introduce a trainer registry or factory that can build multiple supported estimators from config.
- Standardize fit, predict, metric, and explainability hooks across model families.

Done when:
- Adding a new model family no longer requires copying the entire training pipeline.
- At least the first supported model families can be created from one shared abstraction.

### MM-03: Build a multi-model orchestration and ranking service

Summary: Train and evaluate multiple candidate models in one run, then choose a champion consistently.

Scope:
- Run all requested candidate models on the same split and seed.
- Capture metrics, runtime, failures, and rank ordering for every candidate.
- Implement champion-selection logic using a configurable primary metric and tie-break rules.
- Decide how `fltF1` should behave as a threshold or gate in the new flow.

Done when:
- One training request produces a comparable result set across multiple models.
- The system can declare one champion model without overwriting the history of the other candidates.

### MM-04: Replace fixed filenames with versioned artifact storage

Summary: Stop overwriting one artifact and store model outputs per run and per family.

Scope:
- Replace `churn_prediction_model.pkl` and the fixed `RandomForest_*.pkl` files with a run-based storage layout.
- Save a manifest per run with artifact paths, model family, training timestamp, and dependency metadata.
- Align the runtime loader with the same storage convention used by training.

Done when:
- Multiple training runs can coexist on disk.
- The system can load a model artifact by run or model ID instead of only loading the latest overwrite.

### MM-05: Redesign persistence for model lineage and comparison

Summary: Expand the database schema so historical model records can represent real multi-model training runs.

Scope:
- Replace or extend `Historical_Models` to store run ID, model family, hyperparameters, artifact path, ranking score, champion flag, runtime, and feature summary.
- Add enough metadata to separate training-run-level fields from per-model candidate fields.
- Add migration scripts or a documented schema rollout path because no migration framework exists yet.

Done when:
- Historical model comparison can answer which run happened, which models were trained, and which one won.
- Database queries can retrieve both the latest champion and the full candidate set for a run.

### MM-06: Update inference to serve a selected model instead of one global model

Summary: Move scoring from a single in-memory model to an explicit serving and loading strategy.

Scope:
- Allow scoring to use the current champion, latest run, or an explicit model ID.
- Replace the single `app.state.model` assumption with a model cache or loader keyed by model identity.
- Fix startup loading so the server uses the same configured artifact location as the training flow.

Done when:
- Scoring can be traced to a specific trained model.
- Server startup and lazy loading no longer depend on one hard-coded artifact path.

### MM-07: Normalize explainability and feature importance across model families

Summary: Make explainability work for more than tree-based models and persist it in a stable format.

Scope:
- Add model-specific explainability strategies such as tree SHAP, coefficients, or permutation importance.
- Normalize the output so training and scoring always receive the same top-feature schema.
- Persist feature-importance summaries for historical best-model views.

Done when:
- `tblFeatureImportance` is populated for supported model families.
- Historical model views can show stored feature importance instead of only the latest live response.

### MM-08: Replace frontend placeholders with a real multi-model training workspace

Summary: Turn the existing UI placeholders into an actual model-selection and comparison workflow.

Scope:
- Add model-family selectors and per-model parameter controls to the Model Lab.
- Show candidate comparison results, champion selection rationale, and per-model metrics in the result pane.
- Stop assuming there is only one generic best row chosen by F1 across all history.

Done when:
- Operators can launch a multi-model run from the UI and understand which model was selected.
- The frontend no longer advertises model-family support as placeholder-only.

### MM-09: Add model lineage to scored outputs and Data Vault views

Summary: Ensure downstream scored data can always be traced back to the source model.

Scope:
- Add model ID, model family, or run ID to scored and downstream tables.
- Extend the database API and frontend result tables to display model lineage fields.
- Ensure any future email-generation lane can reference the source model that produced the prediction.

Done when:
- A scored customer row can be traced back to the exact model that generated it.
- Historical scoring analysis can be filtered by model or run.

### MM-10: Add automated test coverage for training, scoring, and ranking

Summary: Introduce regression coverage before the upgrade lands.

Scope:
- Add unit tests for trainer selection, ranking, artifact manifests, and explainability adapters.
- Add API tests for training and scoring endpoints using a small fixture dataset.
- Add database-write assertions for model history and scored lineage.

Done when:
- The core multi-model path can be validated automatically.
- Backward-compatibility and migration risks are covered by repeatable tests.

### MM-11: Finalize dependency and runtime support for the chosen model stack

Summary: Decide which model families are officially supported and package the environment accordingly.

Scope:
- Decide whether the first release stays scikit-learn-only or adds extra libraries such as XGBoost or LightGBM.
- Update Python dependencies, Docker/runtime packaging, and install notes.
- Add runtime safeguards for training duration, memory use, and SHAP cost.

Done when:
- The supported model list is explicit.
- A fresh environment can install and run every supported training family reliably.

### MM-12: Prepare data migration and rollout steps

Summary: Ship the upgrade without losing the current single-model behavior.

Scope:
- Decide how to map the current artifact and historical rows into the new schema.
- Create a one-time migration or backfill script for existing model history.
- Document rollout order, fallback behavior, and rollback steps.

Done when:
- Existing deployments can move to the new schema and artifact layout safely.
- The team has a clear cutover path from single-model to multi-model training.

## Code Touchpoints Behind These Tickets

- `app/routes/api_modelling.py`
- `model/ChurnPredictionModel.py`
- `model/utils_model.py`
- `app/routes/api_inference.py`
- `app/db/schema.py`
- `app/routes/api_database.py`
- `app/schema/schema.py`
- `app/core/config.py`
- `app/core/server_web.py`
- `website/static/script.js`
- `website/index.html`
- `requirements.txt`
