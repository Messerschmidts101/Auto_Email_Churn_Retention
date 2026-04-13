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
from app.core import config as c
from app.db.database import connect_db
from app.db.schema import Historical_Models, Historical_Training, Latest_Training
from app.schema.schema import (
    DTO_ConfusionMatrix,
    DTO_DatasetSplit,
    DTO_Metrics,
    DTO_Request_RunTraining,
    DTO_Respond_RunTraining,
    DTO_Respond_UploadDataFrame,
    DTO_FeatureImportanceRow
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

    intTopFeats = max(c.intCountFeatsScoring, objRequest.intTopFeats)
    tblTrainData = tblTrainData[objRequest.lisstrFeats + [objRequest.strFeatTarget]].copy() # TODO: change here 
    try:
        time_start = time.perf_counter()
        objModel, dicResults = build_pipeline_model_best(
            intModel = c.intModelDefault, #TODO: change soon
            tblData = tblTrainData,
            boolVerbose = False,
            classModel = _build_training_pipeline_factory(objRequest.intRandomState),
            classEval = partial(SHAP_Transformer, intTopFeats=intTopFeats),
        )
        time_taken = time.perf_counter() - time_start
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Training failed") from exc

    _ensure_storage_directory()
    # TODO: address redundancy with building model
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
    
    tblTopFeats = [
        DTO_FeatureImportanceRow(
            strFeatureName=strFeat,
            fltImportance=float(fltScore),
            intRank=intIndex,
        )
        for intIndex, (strFeat, fltScore) in enumerate(
            dicResults["dicFeats"].items(),
            start=1,
        )
    ]

    return DTO_Respond_RunTraining(
        dicStatus={200: "Success"},
        timeTaken=time_taken,
        dateCreated=dtCreated.isoformat(),
        objDatasetSplit=DTO_DatasetSplit(
            intNegativeTesting=int(dicResults["intCountTestNegativeClass"]),
            intNegativeTraining=int(dicResults["intCountTrainNegativeClass"]),
            intPositiveTesting=int(dicResults["intCountTestPositiveClass"]),
            intPositiveTraining=int(dicResults["intCountTrainPositiveClass"]),
        ),
        objConfusionMatrix=DTO_ConfusionMatrix(
            intFalseNegative=int(dicConfusionMatrix[1][0]),
            intFalsePositive=int(dicConfusionMatrix[0][1]),
            intTrueNegative=int(dicConfusionMatrix[0][0]),
            intTruePositive=int(dicConfusionMatrix[1][1]),
        ),
        objMetrics=DTO_Metrics(
            fltAccuracy=float(dicResults["fltAccuracy"]),
            fltPrecision=float(dicResults["fltPrecision"]),
            fltRecall=float(dicResults["fltRecall"]),
            fltF1=float(dicResults["fltF1"]),
        ),
        tblFeatureImportance=tblTopFeats,
    )
