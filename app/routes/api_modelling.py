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
from ai_ml.d_orchestrator.train import build_pipeline_models_best
from app.core import config as c
from app.db.database import connect_db
from app.db.schema import Historical_Models, Historical_Training, Latest_Training
from app.schema.schema import (
    DTO_ConfusionMatrix,
    DTO_DatasetSplit,
    DTO_FeatureImportanceRow,
    DTO_Metrics,
    DTO_ModelTrainingResult,
    DTO_Request_RunTraining,
    DTO_Respond_RunTraining,
    DTO_Respond_UploadDataFrame,
)

router = APIRouter(
    prefix="/train",
    tags=["train"],
)

def _validate_required_columns(
    tblInput: pd.DataFrame,
    lisstr_required_columns: list[str],
    str_dataset_name: str,
) -> None:
    lisstr_missing_columns = [
        str_column
        for str_column in lisstr_required_columns
        if str_column not in tblInput.columns
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
    os.makedirs(c.strPathStorageML, exist_ok=True)


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


def _normalize_training_columns(
    tblInput: pd.DataFrame,
    lisstr_requested_features: list[str],
    str_target_column: str,
) -> list[str]:
    str_target_column = str_target_column.strip()
    if not str_target_column:
        raise HTTPException(status_code=400, detail="Training target column is required")
    if str_target_column not in tblInput.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Training target column not found: {str_target_column}",
        )

    lisstr_missing_features: list[str] = []
    lisstr_selected_features: list[str] = []
    for str_feature_name in lisstr_requested_features:
        if str_feature_name == str_target_column:
            continue
        if str_feature_name not in tblInput.columns:
            lisstr_missing_features.append(str_feature_name)
            continue
        if str_feature_name not in lisstr_selected_features:
            lisstr_selected_features.append(str_feature_name)

    if lisstr_missing_features:
        raise HTTPException(
            status_code=400,
            detail=(
                "Training features not found in the dataset: "
                + ", ".join(lisstr_missing_features)
            ),
        )
    if not lisstr_selected_features:
        raise HTTPException(
            status_code=400,
            detail="At least one training feature must be selected",
        )

    return lisstr_selected_features


def _build_dataset_split_dto(dic_results: dict) -> DTO_DatasetSplit:
    return DTO_DatasetSplit(
        intNegativeTesting=int(dic_results["intCountTestNegativeClass"]),
        intNegativeTraining=int(dic_results["intCountTrainNegativeClass"]),
        intPositiveTesting=int(dic_results["intCountTestPositiveClass"]),
        intPositiveTraining=int(dic_results["intCountTrainPositiveClass"]),
    )


def _build_confusion_matrix_dto(obj_confusion_matrix) -> DTO_ConfusionMatrix:
    return DTO_ConfusionMatrix(
        intFalseNegative=int(obj_confusion_matrix[1][0]),
        intFalsePositive=int(obj_confusion_matrix[0][1]),
        intTrueNegative=int(obj_confusion_matrix[0][0]),
        intTruePositive=int(obj_confusion_matrix[1][1]),
    )


def _build_metrics_dto(dic_results: dict) -> DTO_Metrics:
    return DTO_Metrics(
        fltAccuracy=float(dic_results["fltAccuracy"]),
        fltPrecision=float(dic_results["fltPrecision"]),
        fltRecall=float(dic_results["fltRecall"]),
        fltF1=float(dic_results["fltF1"]),
    )


def _build_feature_importance_rows(dic_feature_scores: dict) -> list[DTO_FeatureImportanceRow]:
    return [
        DTO_FeatureImportanceRow(
            strFeatureName=strFeat,
            fltImportance=float(fltScore),
            intRank=intIndex,
        )
        for intIndex, (strFeat, fltScore) in enumerate(
            dic_feature_scores.items(),
            start=1,
        )
    ]


def _build_model_training_result_dto(
    dic_model_run: dict,
    str_best_model_name: str,
) -> DTO_ModelTrainingResult:
    dic_results = dic_model_run["dicResults"]
    obj_confusion_matrix = dic_results["objConfusionMatrix"]
    return DTO_ModelTrainingResult(
        strModelName=dic_model_run["strModelName"],
        boolIsChampion=dic_model_run["strModelName"] == str_best_model_name,
        fltGridScore=float(dic_model_run["fltGridScore"]),
        fltTimeTaken=float(dic_model_run["fltGridTimeTaken"]),
        dicBestParams=dic_model_run["dicBestParams"],
        objConfusionMatrix=_build_confusion_matrix_dto(obj_confusion_matrix),
        objMetrics=_build_metrics_dto(dic_results),
        tblFeatureImportance=_build_feature_importance_rows(dic_results["dicFeats"]),
    )


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
        tblLatestTrainData = pd.read_csv(objFile.file)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid file") from exc

    _validate_required_columns(
        tblInput=tblLatestTrainData,
        lisstr_required_columns=c.lisstrFeatsDefault,
        str_dataset_name="Training data",
    )
    tblLatestTrainData = tblLatestTrainData.copy()

    _ensure_storage_directory()
    tblLatestTrainData.to_csv(
        os.path.join(c.strPathStorageML, c.strNameCSVTrain),
        index=False,
    )

    tblHistoricalTrainData = tblLatestTrainData.copy()
    tblHistoricalTrainData["meta_DateCreated"] = datetime.date.today()
    tblHistoricalTrainData["meta_Id"] = [
        str(uuid.uuid4()) for _ in range(len(tblHistoricalTrainData))
    ]

    try:
        objDB.query(Latest_Training).delete(synchronize_session=False)
        objDB.bulk_insert_mappings(
            Latest_Training,
            tblLatestTrainData.to_dict(orient="records"),
        )
        objDB.bulk_insert_mappings(
            Historical_Training,
            tblHistoricalTrainData.to_dict(orient="records"),
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
        tblOutput=tblLatestTrainData.to_dict(orient="records"),
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
    strPathTrainData = os.path.join(
        c.strPathStorageML,
        c.strNameCSVTrain,
    )
    if not os.path.exists(strPathTrainData):
        raise HTTPException(status_code=404, detail="Training data not found")

    try:
        tblTrainData = pd.read_csv(strPathTrainData)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to load training data",
        ) from exc

    _validate_required_columns(
        tblInput=tblTrainData,
        lisstr_required_columns=c.lisstrFeatsDefault,
        str_dataset_name="Training data",
    )

    lisstrSelectedFeatures = _normalize_training_columns(
        tblInput=tblTrainData,
        lisstr_requested_features=objRequest.lisstrFeats,
        str_target_column=objRequest.strFeatTarget,
    )
    intTopFeats = max(c.intCountFeatsScoring, objRequest.intTopFeats)
    tblTrainData = tblTrainData[
        lisstrSelectedFeatures + [objRequest.strFeatTarget]
    ].copy()
    try:
        time_start = time.perf_counter()
        dicTrainingRun = build_pipeline_models_best(
            tblData=tblTrainData,
            lisintModels=[3, 1, 2],
            intCv=objRequest.intCrossFold,
            fltTTSplit=objRequest.fltTTSplit,
            intPrimaryMetric=objRequest.intPrimaryMetric,
            intRandomState=objRequest.intRandomState,
            strTargetColumn=objRequest.strFeatTarget,
            boolVerbose=False,
            classModel=_build_training_pipeline_factory(objRequest.intRandomState),
            classEval=partial(SHAP_Transformer, intTopFeats=intTopFeats),
            boolPersistBestArtifact=True,
        )
        time_taken = time.perf_counter() - time_start
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Training failed") from exc

    objModel = dicTrainingRun["objBestModel"]
    strBestModelName = dicTrainingRun["strBestModelName"]
    dicResults = dicTrainingRun["dicBestResults"]
    lisdicModelRuns = dicTrainingRun["lisdicModelRuns"]

    _ensure_storage_directory()
    try:
        joblib.dump(
            objModel,
            os.path.join(c.strPathStorageML, c.strNameMLFinal),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to persist trained model",
        ) from exc

    objServer.app.state.model = objModel

    dicConfusionMatrix = dicResults["objConfusionMatrix"]
    dtCreated = datetime.datetime.now()
    rowModel = Historical_Models(
        meta_Id=str(uuid.uuid4()),
        meta_DateCreated=dtCreated.date(),
        Accuracy=float(dicResults["fltAccuracy"]),
        Precision=float(dicResults["fltPrecision"]),
        Recall=float(dicResults["fltRecall"]),
        F1=float(dicResults["fltF1"]),
        CountTrueNegative=int(dicConfusionMatrix[0][0]),
        CountFalsePositive=int(dicConfusionMatrix[0][1]),
        CountFalseNegative=int(dicConfusionMatrix[1][0]),
        CountTruePositive=int(dicConfusionMatrix[1][1]),
        CountTrainingPositiveClass=int(dicResults["intCountTrainPositiveClass"]),
        CountTrainingNegativeClass=int(dicResults["intCountTrainNegativeClass"]),
        CountTestPositiveClass=int(dicResults["intCountTestPositiveClass"]),
        CountTestNegativeClass=int(dicResults["intCountTestNegativeClass"]),
    )

    try:
        objDB.add(rowModel)
        objDB.commit()
    except Exception as exc:
        objDB.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to persist model metrics",
        ) from exc

    tblTopFeats = _build_feature_importance_rows(dicResults["dicFeats"])
    tblModelResults = [
        _build_model_training_result_dto(dic_model_run, strBestModelName)
        for dic_model_run in lisdicModelRuns
    ]
    objDatasetSplit = _build_dataset_split_dto(dicResults)
    objConfusionMatrix = _build_confusion_matrix_dto(dicConfusionMatrix)
    objMetrics = _build_metrics_dto(dicResults)

    return DTO_Respond_RunTraining(
        dicStatus={200: "Success"},
        timeTaken=time_taken,
        dateCreated=dtCreated.isoformat(),
        strBestModelName=strBestModelName,
        objDatasetSplit=objDatasetSplit,
        objConfusionMatrix=objConfusionMatrix,
        objMetrics=objMetrics,
        tblFeatureImportance=tblTopFeats,
        tblModelResults=tblModelResults,
    )
