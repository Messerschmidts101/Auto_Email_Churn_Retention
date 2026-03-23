
from app.schema.schema import DTO_Request_UploadTrainingData, DTO_Respond_UploadTrainingData, DTO_Request_RunTraining, DTO_Respond_RunTraining, DTO_DatasetSplit, DTO_ConfusionMatrix, DTO_Metrics, DTO_FeatureImportanceRow
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, Depends
import os
import pandas as pd
from app.core import config
from app.db.schema import Historical_Training, Latest_Training, Historical_Models
from sqlalchemy.orm import Session
from app.db.database import connect_db
import datetime
import uuid
from model.ChurnPredictionModel import ChurnPredictionModel
import joblib
from fastapi import Request
#def example(db: Session = Depends(connect_db)):

router = APIRouter(
    prefix="/train",
    tags=["train"]
)

@router.post(
    "/upload",
    summary="Step 1: Upload training data",
    description=(
        " "
    ),
)
def run(
    objFile: UploadFile = File(...),
    objDB: Session = Depends(connect_db)
) -> DTO_Respond_UploadTrainingData:
    try:
        tblLatestTraining = pd.read_csv(objFile.file)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file")

    ########################################################
    #######                                          #######
    #######         Step 1: Convert to CSV           #######
    #######                                          #######
    ########################################################
    tblLatestTraining.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVTrain),
        index=False,
    )

    ########################################################
    #######                                          #######
    #######        Step 2: Overwrite Latest          #######
    #######                                          #######
    ########################################################
    objDB.query(Latest_Training).delete()
    objDB.bulk_insert_mappings(
        Latest_Training,
        tblLatestTraining.to_dict(orient="records")
    )

    ########################################################
    #######                                          #######
    #######        Step 3: Append Historical         #######
    #######                                          #######
    ########################################################
    tblLatestTraining["meta_DateCreated"] = datetime.datetime.now()
    tblLatestTraining["meta_Id"] = [
        f"{uuid.uuid4()}" for _ in range(len(tblLatestTraining))
    ]
    objDB.bulk_insert_mappings(
        Historical_Training,
        tblLatestTraining.to_dict("records")
    )

    ########################################################
    #######                                          #######
    #######             Step 4: Execute              #######
    #######                                          #######
    ########################################################
    objDB.commit()
    
    ########################################################
    #######                                          #######
    #######           Step 5: Return Result          #######
    #######                                          #######
    ########################################################
    tblLatestTraining = objDB.query(Latest_Training).all()

    tblLatestTraining = [
        {
            "CustomerId": rowRow.CustomerId,
            "Surname": rowRow.Surname,
            "CreditScore": rowRow.CreditScore,
            "Geography": rowRow.Geography,
            "Gender": rowRow.Gender,
            "Age": rowRow.Age,
            "Tenure": rowRow.Tenure,
            "Balance": rowRow.Balance,
            "NumOfProducts": rowRow.NumOfProducts,
            "HasCrCard": rowRow.HasCrCard,
            "IsActiveMember": rowRow.IsActiveMember,
            "EstimatedSalary": rowRow.EstimatedSalary,
            "Exited": rowRow.Exited,
            "RecentSatisfactionScore": rowRow.RecentSatisfactionScore,
        }
        for rowRow in tblLatestTraining
    ]

    return DTO_Respond_UploadTrainingData(
        dicStatus = {200:"Success"},
        tblOutput = tblLatestTraining
    )


@router.post(
    "/model",
    summary="Step 2: Start modelling",
    description=(
        " "
    ),
)
def run(
    objRequest: DTO_Request_RunTraining, 
    objServer: Request,
    objDB: Session = Depends(connect_db)
    ) -> DTO_Respond_RunTraining:
    ########################################################
    #######                                          #######
    #######      Step 1: Load Training Framework     #######
    #######                                          #######
    ########################################################
    objModellingClass = ChurnPredictionModel.ChurnPredictionModel(
        strPathTrainDataset = os.path.join(config.strPathStorageML, config.strNameCSVTrain),
        strPathToSaveModels = config.strPathStorageML
    )
    ########################################################
    #######                                          #######
    #######      Step 2: Run Training Framework      #######
    #######                                          #######
    ########################################################
    timeStart = datetime.datetime.time()
    objModellingClass.run_training(boolVerbose = False)
    timeEnd = datetime.datetime.time()
    
    ########################################################
    #######                                          #######
    #######        Step 3: Save Trained Model        #######
    #######                                          #######
    ########################################################
    # Step 3.1: Save physically
    joblib.dump(
        objModellingClass, 
        os.path.join(config.strPathStorageML, config.strNameMLFinal)
    ) 
    # Step 3.2: Save to server
    objServer.state.model = objModellingClass

    ########################################################
    #######                                          #######
    #######           Step 4: Save Metrics           #######
    #######                                          #######
    ########################################################
    cm = objModellingClass.objConfusionMatrix
    rowModel = Historical_Models(
        meta_Id =  f"{uuid.uuid4()}" ,
        meta_DateCreated = datetime.datetime.now(),
        Accuracy = objModellingClass.fltAccuracy,
        Precision = objModellingClass.fltPrecision,
        Recall = objModellingClass.fltRecall,
        F1 = objModellingClass.fltF1,
        CountTrueNegative = cm[0][0],
        CountFalsePositive = cm[0][1],
        CountFalseNegative = cm[1][0],
        CountTruePositive = cm[1][1],
        CountTrainingPositiveClass = objModellingClass.intCountTrainPositiveClass,
        CountTrainingNegativeClass = objModellingClass.intCountTrainNegativeClass,
        CountTestPositiveClass = objModellingClass.intCountTestPositiveClass,
        CountTestNegativeClass = objModellingClass.intCountTestNegativeClass
    )
    objDB.add(rowModel)
    objDB.commit()

    return DTO_Respond_RunTraining(
        dicStatus = {200:"Success"},
        timeTaken = timeEnd-timeStart,
        dateCreated = datetime.datetime.now(),
        objDatasetSplit = DTO_DatasetSplit(
                intNegativeTesting = objModellingClass.intCountTestNegativeClass,
                intNegativeTraining = objModellingClass.intCountTrainNegativeClass,
                intPositiveTesting = objModellingClass.intCountTestPositiveClass,
                intPositiveTraining = objModellingClass.intCountTrainPositiveClass,
            ),
        objConfusionMatrix = DTO_ConfusionMatrix(
                intFalseNegative = cm[1][0],
                intFalsePositive = cm[0][1],
                intTrueNegative = cm[0][0],
                intTruePositive = cm[1][1],
            ),
        objMetrics = DTO_Metrics(
                fltAccuracy = objModellingClass.fltAccuracy,
                fltPrecision = objModellingClass.fltPrecision,
                fltRecall = objModellingClass.fltRecall,
                fltF1 = objModellingClass.fltF1,
            ),
        # TODO: check if we have this in the classs
        #tblFeatureImportance = list[DTO_FeatureImportanceRow]
    )