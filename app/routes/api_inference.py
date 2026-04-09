import datetime
import os
import time
import uuid
from pathlib import Path

import joblib
import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from sqlalchemy.orm import Session

from app.core import config
from app.db.database import connect_db
from app.db.schema import Historical_Scored, Historical_Scoring, Latest_Scored, Latest_Scoring
from app.schema.schema import DTO_Respond_RunScoring, DTO_Respond_UploadDataFrame


BASE_DIR = Path(__file__).resolve().parents[2]
ARTIFACTS_DIR = BASE_DIR / "artifacts"
SCORING_COLUMNS = [
    "CustomerId",
    "Surname",
    "Email",
    "CreditScore",
    "Geography",
    "Gender",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary",
    "Exited",
    "RecentSatisfactionScore",
]
SCORED_IDENTIFIER_COLUMNS = ["CustomerId", "Surname", "Email"]
SCORED_OUTPUT_COLUMNS = [
    "Prediction",
    "Churn_Probability",
    "Top_1_Feat",
    "Top_1_Feat_Value",
    "Top_1_Feat_Score",
    "Top_2_Feat",
    "Top_2_Feat_Value",
    "Top_2_Feat_Score",
    "Top_3_Feat",
    "Top_3_Feat_Value",
    "Top_3_Feat_Score",
    "Top_4_Feat",
    "Top_4_Feat_Value",
    "Top_4_Feat_Score",
    "Top_5_Feat",
    "Top_5_Feat_Value",
    "Top_5_Feat_Score",
]


router = APIRouter(
    prefix="/score",
    tags=["score"],
)


def _validate_required_columns(
    tbl_input: pd.DataFrame,
    lisstr_required_columns: list[str],
    str_dataset_name: str,
) -> None:
    lisstr_missing_columns = [
        str_column
        for str_column in lisstr_required_columns
        if str_column not in tbl_input.columns
    ]
    if lisstr_missing_columns:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{str_dataset_name} is missing required columns: "
                f"{', '.join(lisstr_missing_columns)}"
            ),
        )


def _ensure_storage_directory() -> None:
    os.makedirs(config.strPathStorageML, exist_ok=True)


def _resolve_model_path() -> Path | None:
    path_runtime_model = Path(config.strPathStorageML) / config.strNameMLFinal
    if path_runtime_model.exists():
        return path_runtime_model

    lispath_model_artifacts = sorted(
        ARTIFACTS_DIR.glob("churn_model_*"),
        key=lambda path_item: path_item.stat().st_mtime,
        reverse=True,
    )
    if lispath_model_artifacts:
        return lispath_model_artifacts[0]

    return None


def _load_scoring_model(objServer: Request):
    obj_model = getattr(objServer.app.state, "model", None)
    if obj_model is not None:
        return obj_model

    path_model = _resolve_model_path()
    if path_model is None:
        raise HTTPException(status_code=404, detail="Model not found")

    try:
        obj_model = joblib.load(path_model)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load model from {path_model.name}",
        ) from exc

    objServer.app.state.model = obj_model
    return obj_model


def _normalize_scored_output(tbl_predictions: pd.DataFrame) -> pd.DataFrame:
    if "Prediction" not in tbl_predictions.columns:
        raise HTTPException(
            status_code=500,
            detail="Scoring output is missing the Prediction column",
        )
    if "Churn_Probability" not in tbl_predictions.columns:
        raise HTTPException(
            status_code=500,
            detail="Scoring output is missing the Churn_Probability column",
        )

    tbl_predictions = tbl_predictions.copy()
    for int_rank in range(1, 6):
        str_feat = f"Top_{int_rank}_Feat"
        str_feat_value = f"Top_{int_rank}_Feat_Value"
        str_feat_score = f"Top_{int_rank}_Feat_Score"
        if str_feat not in tbl_predictions.columns:
            tbl_predictions[str_feat] = ""
        if str_feat_value not in tbl_predictions.columns:
            tbl_predictions[str_feat_value] = ""
        if str_feat_score not in tbl_predictions.columns:
            tbl_predictions[str_feat_score] = 0.0

    return tbl_predictions[SCORED_OUTPUT_COLUMNS]


@router.post(
    "/upload",
    summary="Step 1: Upload scoring data",
    description=(" "),
)
def upload_scoring_data(
    objFile: UploadFile = File(...),
    objDB: Session = Depends(connect_db),
) -> DTO_Respond_UploadDataFrame:
    try:
        tbl_latest_scoring = pd.read_csv(objFile.file)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid file") from exc

    _validate_required_columns(
        tbl_input=tbl_latest_scoring,
        lisstr_required_columns=SCORING_COLUMNS,
        str_dataset_name="Scoring data",
    )
    tbl_latest_scoring = tbl_latest_scoring[SCORING_COLUMNS].copy()

    _ensure_storage_directory()
    tbl_latest_scoring.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVScoring),
        index=False,
    )

    tbl_historical_scoring = tbl_latest_scoring.copy()
    tbl_historical_scoring["meta_DateCreated"] = datetime.date.today()
    tbl_historical_scoring["meta_Id"] = [
        str(uuid.uuid4()) for _ in range(len(tbl_historical_scoring))
    ]

    try:
        objDB.query(Latest_Scoring).delete(synchronize_session=False)
        objDB.bulk_insert_mappings(
            Latest_Scoring,
            tbl_latest_scoring.to_dict(orient="records"),
        )
        objDB.bulk_insert_mappings(
            Historical_Scoring,
            tbl_historical_scoring.to_dict(orient="records"),
        )
        objDB.commit()
    except Exception as exc:
        objDB.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to persist scoring data",
        ) from exc

    return DTO_Respond_UploadDataFrame(
        dicStatus={200: "Success"},
        tblOutput=tbl_latest_scoring.to_dict(orient="records"),
    )


@router.post(
    "/model",
    summary="Step 2: Run inference",
    description=(" "),
)
def run_inference(
    objServer: Request,
    objDB: Session = Depends(connect_db),
) -> DTO_Respond_RunScoring:
    objModel = _load_scoring_model(objServer)

    str_path_scoring = os.path.join(
        config.strPathStorageML,
        config.strNameCSVScoring,
    )
    if not os.path.exists(str_path_scoring):
        raise HTTPException(status_code=404, detail="Scoring data not found")

    try:
        tbl_latest_scoring = pd.read_csv(str_path_scoring)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to load scoring data",
        ) from exc

    _validate_required_columns(
        tbl_input=tbl_latest_scoring,
        lisstr_required_columns=SCORING_COLUMNS,
        str_dataset_name="Scoring data",
    )
    tbl_latest_scoring = tbl_latest_scoring[SCORING_COLUMNS].copy()

    try:
        time_start = time.perf_counter()
        if hasattr(objModel, "transform"):
            tbl_predictions = objModel.transform(tbl_latest_scoring)
        elif hasattr(objModel, "get_predictions"):
            tbl_predictions = objModel.get_predictions(
                strPathScoring=str_path_scoring,
                boolVerbose=False,
            )
        else:
            raise HTTPException(
                status_code=500,
                detail="Loaded model does not support scoring",
            )
        time_taken = time.perf_counter() - time_start
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Scoring failed") from exc

    if not isinstance(tbl_predictions, pd.DataFrame):
        tbl_predictions = pd.DataFrame(tbl_predictions)

    tbl_predictions = tbl_predictions.drop(
        columns=SCORED_IDENTIFIER_COLUMNS,
        errors="ignore",
    )
    tbl_predictions = _normalize_scored_output(tbl_predictions)

    if len(tbl_predictions) != len(tbl_latest_scoring):
        raise HTTPException(
            status_code=500,
            detail="Scoring output row count does not match input data",
        )

    tbl_latest_scored = pd.concat(
        [
            tbl_latest_scoring[SCORED_IDENTIFIER_COLUMNS].reset_index(drop=True),
            tbl_predictions.reset_index(drop=True),
        ],
        axis=1,
    )

    _ensure_storage_directory()
    tbl_latest_scored.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVScored),
        index=False,
    )

    tbl_latest_scored = tbl_latest_scored.where(pd.notna(tbl_latest_scored), None)
    lisdic_latest_scored = tbl_latest_scored.to_dict(orient="records")

    dtm_created = datetime.datetime.now()
    tbl_historical_scored = tbl_latest_scored.copy()
    tbl_historical_scored["meta_DateCreated"] = dtm_created.date()
    tbl_historical_scored["meta_Id"] = [
        str(uuid.uuid4()) for _ in range(len(tbl_historical_scored))
    ]

    try:
        objDB.query(Latest_Scored).delete(synchronize_session=False)
        objDB.bulk_insert_mappings(Latest_Scored, lisdic_latest_scored)
        objDB.bulk_insert_mappings(
            Historical_Scored,
            tbl_historical_scored.to_dict(orient="records"),
        )
        objDB.commit()
    except Exception as exc:
        objDB.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to persist scored results",
        ) from exc

    return DTO_Respond_RunScoring(
        dicStatus={200: "Success"},
        timeTaken=time_taken,
        dateCreated=dtm_created.isoformat(),
        tblOutput=lisdic_latest_scored,
    )
