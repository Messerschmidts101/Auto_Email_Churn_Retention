import datetime
import os
import time
import uuid
from functools import partial

import joblib
import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from sqlalchemy.orm import Session

from ai_ml.b_model.pipeline import build_pipeline_model
from ai_ml.c_evaluator.transformers import SHAP_Transformer
from ai_ml.d_orchestrator.train import build_pipeline_model_best
from app.core import config
from app.db.database import connect_db
from app.db.schema import Historical_Models, Historical_Training, Latest_Training
from app.schema.schema import (
    DTO_ConfusionMatrix,
    DTO_DatasetSplit,
    DTO_Metrics,
    DTO_Request_RunTraining,
    DTO_Respond_RunTraining,
    DTO_Respond_UploadDataFrame,
)


TRAINING_COLUMNS = [
    "CustomerId",
    "Surname",
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
DEFAULT_MODEL_ID = 3
MIN_SCORING_FEATURES = 5


router = APIRouter(
    prefix="/train",
    tags=["train"],
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


def _build_training_pipeline_factory(int_random_state: int):
    def _build_pipeline(
        lisstrColNamesX: list[str],
        intModel: int,
        boolVerbose: bool = False,
        pipeFeatEng=None,
    ):
        pipe_model = build_pipeline_model(
            lisstrColNamesX=lisstrColNamesX,
            intModel=intModel,
            boolVerbose=boolVerbose,
            pipeFeatEng=pipeFeatEng,
        )
        obj_estimator = pipe_model.named_steps.get("model")
        if hasattr(obj_estimator, "random_state"):
            obj_estimator.set_params(random_state=int_random_state)
        return pipe_model

    return _build_pipeline


@router.post(
    "/upload",
    summary="Step 1: Upload training data",
    description=(" "),
)
def upload_training_data(
    objFile: UploadFile = File(...),
    objDB: Session = Depends(connect_db),
) -> DTO_Respond_UploadDataFrame:
    try:
        tbl_latest_training = pd.read_csv(objFile.file)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid file") from exc

    _validate_required_columns(
        tbl_input=tbl_latest_training,
        lisstr_required_columns=TRAINING_COLUMNS,
        str_dataset_name="Training data",
    )
    tbl_latest_training = tbl_latest_training[TRAINING_COLUMNS].copy()

    _ensure_storage_directory()
    tbl_latest_training.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVTrain),
        index=False,
    )

    tbl_historical_training = tbl_latest_training.copy()
    tbl_historical_training["meta_DateCreated"] = datetime.date.today()
    tbl_historical_training["meta_Id"] = [
        str(uuid.uuid4()) for _ in range(len(tbl_historical_training))
    ]

    try:
        objDB.query(Latest_Training).delete(synchronize_session=False)
        objDB.bulk_insert_mappings(
            Latest_Training,
            tbl_latest_training.to_dict(orient="records"),
        )
        objDB.bulk_insert_mappings(
            Historical_Training,
            tbl_historical_training.to_dict(orient="records"),
        )
        objDB.commit()
    except Exception as exc:
        objDB.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to persist training data",
        ) from exc

    return DTO_Respond_UploadDataFrame(
        dicStatus={200: "Success"},
        tblOutput=tbl_latest_training.to_dict(orient="records"),
    )


@router.post(
    "/model",
    summary="Step 2: Start modelling",
    description=(" "),
)
def run_training_model(
    objRequest: DTO_Request_RunTraining,
    objServer: Request,
    objDB: Session = Depends(connect_db),
) -> DTO_Respond_RunTraining:
    str_path_training_dataset = os.path.join(
        config.strPathStorageML,
        config.strNameCSVTrain,
    )
    if not os.path.exists(str_path_training_dataset):
        raise HTTPException(status_code=404, detail="Training data not found")

    try:
        tbl_training_data = pd.read_csv(str_path_training_dataset)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to load training data",
        ) from exc

    _validate_required_columns(
        tbl_input=tbl_training_data,
        lisstr_required_columns=TRAINING_COLUMNS,
        str_dataset_name="Training data",
    )

    int_top_feats = max(MIN_SCORING_FEATURES, objRequest.intTopFeats)
    tbl_training_data = tbl_training_data[TRAINING_COLUMNS].copy()
    tbl_training_model_input = tbl_training_data.drop(columns=["CustomerId"])

    try:
        time_start = time.perf_counter()
        objModel, dicResults = build_pipeline_model_best(
            intModel=DEFAULT_MODEL_ID,
            tblData=tbl_training_model_input,
            boolVerbose=False,
            classModel=_build_training_pipeline_factory(objRequest.intRandomState),
            classEval=partial(SHAP_Transformer, intTopFeats=int_top_feats),
        )
        time_taken = time.perf_counter() - time_start
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Training failed") from exc

    _ensure_storage_directory()
    try:
        joblib.dump(
            objModel,
            os.path.join(config.strPathStorageML, config.strNameMLFinal),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to persist trained model",
        ) from exc

    objServer.app.state.model = objModel

    obj_confusion_matrix = dicResults["objConfusionMatrix"]
    dtm_created = datetime.datetime.now()
    row_model = Historical_Models(
        meta_Id=str(uuid.uuid4()),
        meta_DateCreated=dtm_created.date(),
        Accuracy=float(dicResults["fltAccuracy"]),
        Precision=float(dicResults["fltPrecision"]),
        Recall=float(dicResults["fltRecall"]),
        F1=float(dicResults["fltF1"]),
        CountTrueNegative=int(obj_confusion_matrix[0][0]),
        CountFalsePositive=int(obj_confusion_matrix[0][1]),
        CountFalseNegative=int(obj_confusion_matrix[1][0]),
        CountTruePositive=int(obj_confusion_matrix[1][1]),
        CountTrainingPositiveClass=int(dicResults["intCountTrainPositiveClass"]),
        CountTrainingNegativeClass=int(dicResults["intCountTrainNegativeClass"]),
        CountTestPositiveClass=int(dicResults["intCountTestPositiveClass"]),
        CountTestNegativeClass=int(dicResults["intCountTestNegativeClass"]),
    )

    try:
        objDB.add(row_model)
        objDB.commit()
    except Exception as exc:
        objDB.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to persist model metrics",
        ) from exc

    return DTO_Respond_RunTraining(
        dicStatus={200: "Success"},
        timeTaken=time_taken,
        dateCreated=dtm_created.isoformat(),
        objDatasetSplit=DTO_DatasetSplit(
            intNegativeTesting=int(dicResults["intCountTestNegativeClass"]),
            intNegativeTraining=int(dicResults["intCountTrainNegativeClass"]),
            intPositiveTesting=int(dicResults["intCountTestPositiveClass"]),
            intPositiveTraining=int(dicResults["intCountTrainPositiveClass"]),
        ),
        objConfusionMatrix=DTO_ConfusionMatrix(
            intFalseNegative=int(obj_confusion_matrix[1][0]),
            intFalsePositive=int(obj_confusion_matrix[0][1]),
            intTrueNegative=int(obj_confusion_matrix[0][0]),
            intTruePositive=int(obj_confusion_matrix[1][1]),
        ),
        objMetrics=DTO_Metrics(
            fltAccuracy=float(dicResults["fltAccuracy"]),
            fltPrecision=float(dicResults["fltPrecision"]),
            fltRecall=float(dicResults["fltRecall"]),
            fltF1=float(dicResults["fltF1"]),
        ),
        tblFeatureImportance=[],
    )
