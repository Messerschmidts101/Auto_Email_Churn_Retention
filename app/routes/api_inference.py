
from app.schema.schema import DTO_Request_UploadTrainingData, DTO_Respond_UploadDataFrame, DTO_Respond_RunScoring
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, Depends
import os
import pandas as pd
from app.core import config
from app.db.schema import Historical_Scoring, Latest_Scoring, Latest_Scored, Historical_Scored
from sqlalchemy.orm import Session
from app.db.database import connect_db
import datetime
import time
import uuid
from model.ChurnPredictionModel import ChurnPredictionModel
import joblib
from fastapi import Request
#def example(db: Session = Depends(connect_db)):

router = APIRouter(
    prefix="/score",
    tags=["score"]
)

@router.post(
    "/upload",
    summary="Step 1: Upload scoring data",
    description=(
        " "
    )
)
def run(
    objFile: UploadFile = File(...),
    objDB: Session = Depends(connect_db)
) -> DTO_Respond_UploadDataFrame:
    try:
        tblLatestScoring = pd.read_csv(objFile.file)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file")

    ########################################################
    #######                                          #######
    #######         Step 1: Convert to CSV           #######
    #######                                          #######
    ########################################################
    tblLatestScoring.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVScoring),
        index=False,
    )

    ########################################################
    #######                                          #######
    #######        Step 2: Overwrite Latest          #######
    #######                                          #######
    ########################################################
    objDB.query(Latest_Scoring).delete()
    objDB.bulk_insert_mappings(
        Latest_Scoring,
        tblLatestScoring.to_dict(orient="records")
    )

    ########################################################
    #######                                          #######
    #######        Step 3: Append Historical         #######
    #######                                          #######
    ########################################################
    tblLatestScoring["meta_DateCreated"] = datetime.datetime.now()
    tblLatestScoring["meta_Id"] = [
        f"{uuid.uuid4()}" for _ in range(len(tblLatestScoring))
    ]
    objDB.bulk_insert_mappings(
        Historical_Scoring,
        tblLatestScoring.to_dict("records")
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
    tblLatestScoring = objDB.query(Latest_Scoring).all()

    tblLatestScoring = [
        {
            "CustomerId": rowRow.CustomerId,
            "Surname": rowRow.Surname,
            "Email": rowRow.Email,
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
        for rowRow in tblLatestScoring
    ]

    return DTO_Respond_UploadDataFrame(
        dicStatus = {200:"Success"},
        tblOutput = tblLatestScoring
    )

@router.post(
    "/model",
    summary="Step 2: Run inference",
    description=(
        " "
    ),
)
def run(
    objServer: Request,
    objDB: Session = Depends(connect_db)
    ) -> DTO_Respond_RunScoring:
    ########################################################
    #######                                          #######
    #######             Step 1: Get Model            #######
    #######                                          #######
    ########################################################
    objModel = getattr(objServer.state, "model", None)
    if objModel is None:
        strPathModel = os.path.join(config.strPathStorageML, config.strNameMLFinal)
        if not os.path.exists(strPathModel):
            raise HTTPException(status_code=404, detail="Model not found")
        objModel = joblib.load(strPathModel)
        objServer.state.model = objModel

    ########################################################
    #######                                          #######
    #######             Step 2: Run Scoring          #######
    #######                                          #######
    ########################################################
    strPathScoring = os.path.join(config.strPathStorageML, config.strNameCSVScoring)
    if not os.path.exists(strPathScoring):
        raise HTTPException(status_code=404, detail="Scoring data not found")

    timeStart = time.perf_counter()
    tblLatestScored = objModel.get_predictions(
        strPathScoring=strPathScoring,
        boolVerbose=False,
    )

    ########################################################
    #######                                          #######
    #######      Step 3: Combine Results and PII     #######
    #######                                          #######
    ########################################################
    # Step 3.1. Get only customer data
    tblScoringCustomerDetails = pd.DataFrame(
        objDB.query(
            Latest_Scoring.CustomerId,
            Latest_Scoring.Surname,
            Latest_Scoring.Email,
        ).all(),
        columns=["CustomerId", "Surname", "Email"],
    )
    # Step 3.2. Combine customer data to master
    tblLatestScored = pd.concat(
        [tblScoringCustomerDetails.reset_index(drop=True), tblLatestScored.reset_index(drop=True)],
        axis=1,
    )
    timeTaken = time.perf_counter() - timeStart

    ########################################################
    #######                                          #######
    #######        Step 4: Store Results to DB       #######
    #######                                          #######
    ########################################################
    tblLatestScoredRecords = tblLatestScored.to_dict(orient="records")
    objDB.query(Latest_Scored).delete(synchronize_session=False)
    objDB.bulk_insert_mappings(Latest_Scored, tblLatestScoredRecords)

    dtmCreated = datetime.datetime.now()
    tblHistoricalScored = tblLatestScored.copy()
    tblHistoricalScored["meta_DateCreated"] = dtmCreated
    tblHistoricalScored["meta_Id"] = [str(uuid.uuid4()) for _ in range(len(tblHistoricalScored))]
    objDB.bulk_insert_mappings(
        Historical_Scored,
        tblHistoricalScored.to_dict(orient="records"),
    )
    objDB.commit()

    return DTO_Respond_RunScoring(
        dicStatus={200: "Success"},
        timeTaken=timeTaken,
        dateCreated=dtmCreated.isoformat(),
        tblOutput=tblLatestScoredRecords,
    )
