# schemas_response/modelling.py
from datetime import date
from typing import Any
from pydantic import BaseModel


class DTO_Request_UploadTrainingData(BaseModel):
    boolVerbose: bool = False
    #file object is expected
    #soon requires user id

class DTO_DatasetRow(BaseModel):
    strFeatureName: str
    anyValue: Any
    intIndex: int

class DTO_Respond_UploadDataFrame(BaseModel):
    dicStatus: dict # sample {500: 'Cant read file'}
    tblOutput: list[dict[str, Any]]

class DTO_Request_RunTraining(BaseModel):
    intRandomState: int = 0
    intTopFeats: int = 20
    fltF1: float = 1

class DTO_DatasetSplit(BaseModel):
    intNegativeTesting: int
    intNegativeTraining: int
    intPositiveTesting: int
    intPositiveTraining: int

class DTO_ConfusionMatrix(BaseModel):
    intFalseNegative: int
    intFalsePositive: int
    intTrueNegative: int
    intTruePositive: int

class DTO_Metrics(BaseModel):
    fltAccuracy: float
    fltPrecision: float
    fltRecall: float
    fltF1: float

class DTO_FeatureImportanceRow(BaseModel):
    strFeatureName: str
    fltImportance: float
    intRank: int

class DTO_Respond_RunTraining(BaseModel):
    dicStatus: dict # sample {500: 'Cant read file'}
    timeTaken: float
    dateCreated: str

    objDatasetSplit: DTO_DatasetSplit
    objConfusionMatrix: DTO_ConfusionMatrix
    objMetrics: DTO_Metrics
    tblFeatureImportance: list[DTO_FeatureImportanceRow]

class DTO_Request_UploadScoringData(BaseModel):
    boolVerbose: bool = False
    #file object is expected
    #soon requires user id

class DTO_Respond_RunScoring(BaseModel):
    dicStatus: dict # sample {500: 'Cant read file'}
    timeTaken: float
    dateCreated: str
    tblOutput: list[dict[str, Any]]

