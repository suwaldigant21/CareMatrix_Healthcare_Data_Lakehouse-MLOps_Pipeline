"""
CareMatrix - 30-Day Readmission Model (V1)

Trains a binary classifier on the CMS-HCC V12 Gold layer:

  Features (X): raw_raf_score, demographic_weight, disease_weight,
                age band, sex
  Target  (y): readmitted_30d = 1 if the patient had an inpatient
               readmission (a new admission within 1-30 days of a prior
               discharge) during the payment year, else 0.

Pipeline:
  1. Pull a training set from Athena (fct_patient_risk_scores joined to
     int_patient_demographics and a claims-derived readmission label).
  2. Engineer features (age bands, one-hot encoding).
  3. Train a class-imbalance-aware XGBoost classifier on an 80/20 split.
  4. Evaluate on the holdout (ROC-AUC, Precision, Recall, F1).
  5. Persist readmission_model.joblib and metrics.json next to this script.

Readmission label derivation (SQL):
  For every patient, claims are sorted by start date. A claim is flagged as
  a readmission when its start date falls 1-30 days after the previous
  claim's end date (lag window). The patient-year label is 1 if any claim
  in that year was flagged. Patient-years with no observed claims are
  excluded from training (the label is unknown, not negative).
"""

import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

HERE = Path(__file__).resolve().parent

ATHENA_RESULTS = os.environ.get(
    "CAREMATRIX_ATHENA_RESULTS",
    "s3://carematrix-lakehouse-dev-demo/athena-results/",
)
REGION = "us-east-1"
SCHEMA = "carematrix_dev_db"

MODEL_PATH = HERE / "readmission_model.joblib"
METRICS_PATH = HERE / "metrics.json"

NUMERIC_FEATURES = ["demographic_weight", "disease_weight", "raw_raf_score"]
CATEGORICAL_FEATURES = ["age_band", "sex_code"]
THRESHOLDS = [0.3, 0.5, 0.7]

AGE_BINS = [0, 65, 70, 75, 80, 85, 200]
AGE_LABELS = ["0_64", "65_69", "70_74", "75_79", "80_84", "85_plus"]

TRAINING_SQL = f"""
with claims as (
    select
        patient_id_hash,
        claim_year,
        claim_start_date,
        claim_end_date
    from {SCHEMA}.stg_silver_inpatient_claims
    where claim_year != 1900
      and claim_start_date is not null
      and claim_end_date is not null
),

ordered as (
    select
        patient_id_hash,
        claim_year,
        claim_start_date,
        claim_end_date,
        lag(claim_end_date) over (
            partition by patient_id_hash
            order by claim_start_date, claim_end_date
        ) as prev_end_date
    from claims
),

flagged as (
    select
        *,
        case
            when prev_end_date is not null
                 and date_diff('day', prev_end_date, claim_start_date)
                     between 1 and 30
            then 1 else 0
        end as is_readmit
    from ordered
),

patient_year_label as (
    select
        patient_id_hash,
        claim_year,
        max(is_readmit) as readmitted_30d
    from flagged
    group by patient_id_hash, claim_year
)

select
    f.patient_id_hash,
    f.payment_year,
    f.demographic_weight,
    f.disease_weight,
    f.raw_raf_score,
    d.age,
    d.sex_code,
    l.readmitted_30d
from {SCHEMA}.fct_patient_risk_scores f
join {SCHEMA}.int_patient_demographics d
    on f.patient_id_hash = d.patient_id_hash
   and f.payment_year = d.payment_year
join patient_year_label l
    on f.patient_id_hash = l.patient_id_hash
   and f.payment_year = l.claim_year
"""


def pull_training_data() -> pd.DataFrame:
    print(f"[1/4] Pulling training set from Athena ({SCHEMA})...")
    conn = connect(
        region_name=REGION,
        s3_staging_dir=ATHENA_RESULTS,
        cursor_class=PandasCursor,
    )
    cursor = conn.cursor()
    cursor.execute(TRAINING_SQL)
    df = cursor.as_pandas()
    conn.close()
    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    print(f"      {len(df):,} labeled patient-years pulled.")
    return df


def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    print("[2/4] Engineering features (age band + one-hot encoding)...")
    df["age_band"] = pd.cut(df["age"], bins=AGE_BINS, labels=AGE_LABELS)
    encoded = pd.get_dummies(
        df[CATEGORICAL_FEATURES], prefix=CATEGORICAL_FEATURES, dtype=int
    )
    X = pd.concat([df[NUMERIC_FEATURES].reset_index(drop=True), encoded], axis=1)
    y = df["readmitted_30d"].astype(int)
    print(f"      Feature matrix: {X.shape}, positive rate: {y.mean():.3%}")
    return X, y


def train_and_evaluate(X: pd.DataFrame, y: pd.Series) -> dict:
    print("[3/4] Training XGBoost (80/20 stratified split)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    model = XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=neg / max(pos, 1),
        eval_metric="auc",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred_prob = model.predict_proba(X_test)[:, 1]

    threshold_metrics = []
    for t in THRESHOLDS:
        y_pred = (y_pred_prob >= t).astype(int)
        threshold_metrics.append(
            {
                "threshold": t,
                "precision": float(precision_score(y_test, y_pred)),
                "recall": float(recall_score(y_test, y_pred)),
                "f1": float(f1_score(y_test, y_pred)),
            }
        )
        print(
            f"      threshold {t}: Precision {threshold_metrics[-1]['precision']:.3f} | "
            f"Recall {threshold_metrics[-1]['recall']:.3f} | F1 {threshold_metrics[-1]['f1']:.3f}"
        )

    metrics = {
        "n_samples": int(len(y)),
        "n_train": int(len(y_train)),
        "n_test": int(len(y_test)),
        "positive_rate": float(y.mean()),
        "roc_auc": float(roc_auc_score(y_test, y_pred_prob)),
        "precision": threshold_metrics[1]["precision"],
        "recall": threshold_metrics[1]["recall"],
        "f1": threshold_metrics[1]["f1"],
        "thresholds": threshold_metrics,
        "scale_pos_weight": float(neg / max(pos, 1)),
        "features": list(X.columns),
        "age_bins": list(AGE_BINS),
        "age_labels": list(AGE_LABELS),
    }
    print(
        f"      ROC-AUC {metrics['roc_auc']:.3f} | "
        f"Precision@0.5 {metrics['precision']:.3f} | "
        f"Recall@0.5 {metrics['recall']:.3f}"
    )
    return metrics, model


def main() -> None:
    df = pull_training_data()
    X, y = engineer_features(df)
    metrics, model = train_and_evaluate(X, y)

    print("[4/4] Persisting artifacts...")
    joblib.dump(model, MODEL_PATH)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"      Model saved: {MODEL_PATH}")
    print(f"      Metrics saved: {METRICS_PATH}")
    print("Done.")


if __name__ == "__main__":
    main()
