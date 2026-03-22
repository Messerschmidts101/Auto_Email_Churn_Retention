########################################################
#######                                          #######
#######            Import Environment            #######
#######                                          #######
########################################################
import os
import re
import smtplib
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date
from email.mime.text import MIMEText
from typing import Any

os.environ.setdefault(
    "PYSPARK_DRIVER_PYTHON",
    os.path.join("venv", "Scripts", "python.exe"),
)
os.environ.setdefault(
    "PYSPARK_PYTHON",
    os.path.join("venv", "Scripts", "python.exe"),
)
os.environ.setdefault("JAVA_HOME", "C:/Program Files/Java/jdk-11")
os.environ.setdefault("HADOOP_HOME", "C:/Program Files/Hadoop")

########################################################
#######                                          #######
#######             Import Libraries             #######
#######                                          #######
########################################################
import joblib
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import pass_context
from pydantic import BaseModel
from sqlalchemy import text

########################################################
#######                                          #######
#######         Import Custom Libraries          #######
#######                                          #######
########################################################
utils_path = os.path.join(os.getcwd(), "model")
if utils_path not in sys.path:
    sys.path.append(utils_path)

from app.db.server_database import (  # noqa: E402
    Historical_Emails,
    Historical_Models,
    Historical_Scored,
    Historical_Scoring,
    Historical_Training,
    Latest_Emails,
    Latest_Scored,
    Latest_Scoring,
    Latest_Training,
    db,
)
import ChurnPredictionModel  # noqa: E402
import server_web_config  # noqa: E402
import utils_server  # noqa: E402

########################################################
#######                                          #######
#######             Server Constants             #######
#######                                          #######
########################################################


def resolve_first_existing_dir(*candidates: str) -> str:
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    return candidates[0]


TEMPLATE_DIR = resolve_first_existing_dir("website", "Website")
STATIC_DIR = os.path.join(TEMPLATE_DIR, "static")
MODEL_PATH = os.path.join(
    server_web_config.strPathStorageML,
    server_web_config.strNameMLFinal,
)


class ViewResultsRequest(BaseModel):
    strTableName: str
    strTableVersion: str


TABLE_NAME_MAP = {
    ("latest", "training"): Latest_Training.__tablename__,
    ("latest", "scoring"): Latest_Scoring.__tablename__,
    ("latest", "scored"): Latest_Scored.__tablename__,
    ("latest", "emails"): Latest_Emails.__tablename__,
    ("latest", "models"): Historical_Models.__tablename__,
    ("historical", "training"): Historical_Training.__tablename__,
    ("historical", "scoring"): Historical_Scoring.__tablename__,
    ("historical", "scored"): Historical_Scored.__tablename__,
    ("historical", "emails"): Historical_Emails.__tablename__,
    ("historical", "models"): Historical_Models.__tablename__,
}

########################################################
#######                                          #######
#######           Database Compatibility         #######
#######                                          #######
########################################################
# The database layer still uses Flask-SQLAlchemy models, so keep a hidden
# Flask app for DB setup and scoped sessions while exposing FastAPI routes.
db_app = Flask(__name__)
db_app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///database.db"
db_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(db_app)


def load_saved_model():
    if not os.path.exists(MODEL_PATH):
        return None
    try:
        return joblib.load(MODEL_PATH)
    except Exception:
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    with db_app.app_context():
        db.create_all()

    try:
        app.state.objLLM = utils_server.create_llm()
        app.state.llm_error = None
    except Exception as exc:
        app.state.objLLM = None
        app.state.llm_error = str(exc)

    app.state.objGlobalModellingClass = load_saved_model()
    yield


########################################################
#######                                          #######
#######              Create Server               #######
#######                                          #######
########################################################
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATE_DIR)


@pass_context
def compatible_url_for(context, name: str, **path_params: Any):
    request = context["request"]
    if name == "static" and "filename" in path_params and "path" not in path_params:
        path_params["path"] = path_params.pop("filename")
    return request.url_for(name, **path_params)


templates.env.globals["url_for"] = compatible_url_for


########################################################
#######                                          #######
#######               Helpers                    #######
#######                                          #######
########################################################
def get_valid_table_name(table_version: str, table_name: str) -> str:
    key = (table_version.lower(), table_name.lower())
    strResolvedTableName = TABLE_NAME_MAP.get(key)
    if not strResolvedTableName:
        raise HTTPException(status_code=400, detail="Invalid table selection.")
    return strResolvedTableName


def get_llm_or_raise():
    objLLM = getattr(app.state, "objLLM", None)
    if objLLM is not None:
        return objLLM

    strError = getattr(app.state, "llm_error", None)
    if strError:
        raise HTTPException(
            status_code=503,
            detail=f"LLM is unavailable: {strError}",
        )
    raise HTTPException(status_code=503, detail="LLM is unavailable.")


def get_model_or_raise():
    objPipeline = getattr(app.state, "objGlobalModellingClass", None)
    if objPipeline is not None:
        return objPipeline

    objPipeline = load_saved_model()
    if objPipeline is None:
        raise HTTPException(status_code=404, detail="No trained model available.")

    app.state.objGlobalModellingClass = objPipeline
    return objPipeline


def add_spaces_to_camel_case(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", " ", str(name))


########################################################
#######                                          #######
#######                 Routes                   #######
#######                                          #######
########################################################
@app.get("/", response_class=HTMLResponse)
def hello(request: Request):
    return templates.TemplateResponse(
        name="index.html",
        request=request,
        context={},
    )


@app.post("/upload_train")
def upload_train(file: UploadFile = File(...)):
    try:
        tblLatestTraining = pd.read_csv(file.file)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read training CSV: {exc}",
        ) from exc

    tblLatestTraining.to_csv(
        os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVTrain,
        ),
        index=False,
    )

    with db_app.app_context():
        Latest_Training.overwrite_self(tblLatestTraining)
        Latest_Training.append_historical(tblLatestTraining, Historical_Training)
        return Latest_Training.to_json()


@app.post("/upload_scoring")
def upload_scoring(file: UploadFile = File(...)):
    try:
        tblLatestScoring = pd.read_csv(file.file)
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read scoring CSV: {exc}",
        ) from exc

    tblLatestScoring.to_csv(
        os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVScoring,
        ),
        index=False,
    )

    with db_app.app_context():
        Latest_Scoring.overwrite_self(tblLatestScoring)
        Latest_Scoring.append_historical(tblLatestScoring, Historical_Scoring)
        return Latest_Scoring.to_json()


@app.get("/train_model")
def train_model():
    objModellingClass = ChurnPredictionModel.ChurnPredictionModel(
        strPathTrainDataset=os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVTrain,
        ),
        strPathToSaveModels=server_web_config.strPathStorageML,
    )

    timeStart = time.time()
    objModellingClass.run_training(boolVerbose=False)
    joblib.dump(objModellingClass, MODEL_PATH)
    app.state.objGlobalModellingClass = objModellingClass

    dicSamples = {
        "Number of Positive Class In Training": [objModellingClass.intCountTrainPositiveClass],
        "Number of Negative Class In Training": [objModellingClass.intCountTrainNegativeClass],
        "Number of Positive Class In Testing": [objModellingClass.intCountTestPositiveClass],
        "Number of Negative Class In Testing": [objModellingClass.intCountTestNegativeClass],
    }
    dicMetrics = {
        "Accuracy": [objModellingClass.fltAccuracy],
        "Precision": [objModellingClass.fltPrecision],
        "Recall": [objModellingClass.fltRecall],
        "F1": [objModellingClass.fltF1],
    }
    cm = objModellingClass.objConfusionMatrix
    dicConfusionMatrix = {
        "True Negative": [cm[0][0]],
        "False Positive": [cm[0][1]],
        "False Negative": [cm[1][0]],
        "True Positive": [cm[1][1]],
    }
    tblMetrics = pd.DataFrame(dicMetrics)
    tblSamples = pd.DataFrame(dicSamples)
    tblConfusionMatrix = pd.DataFrame(dicConfusionMatrix)
    timeEnd = time.time()

    dtNow = date.today()
    rowModel = Historical_Models(
        meta_Id=f"{dtNow}_{uuid.uuid4()}",
        meta_DateCreated=dtNow,
        Accuracy=objModellingClass.fltAccuracy,
        Precision=objModellingClass.fltPrecision,
        Recall=objModellingClass.fltRecall,
        F1=objModellingClass.fltF1,
        CountTrueNegative=cm[0][0],
        CountFalsePositive=cm[0][1],
        CountFalseNegative=cm[1][0],
        CountTruePositive=cm[1][1],
        CountTrainingPositiveClass=objModellingClass.intCountTrainPositiveClass,
        CountTrainingNegativeClass=objModellingClass.intCountTrainNegativeClass,
        CountTestPositiveClass=objModellingClass.intCountTestPositiveClass,
        CountTestNegativeClass=objModellingClass.intCountTestNegativeClass,
    )

    with db_app.app_context():
        db.session.add(rowModel)
        db.session.commit()

    return {
        "samples": tblSamples.to_dict(orient="records"),
        "metrics": tblMetrics.to_dict(orient="records"),
        "confusion_matrix": tblConfusionMatrix.to_dict(orient="records"),
        "time": timeEnd - timeStart,
    }


@app.get("/run_inference")
def get_prediction():
    objPipeline = get_model_or_raise()

    timeStart = time.time()
    tblLatestScored = objPipeline.get_predictions(
        strPathScoring=os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVScoring,
        )
    )

    with db_app.app_context():
        tblScoringCustomerDetails = pd.read_sql(
            f'SELECT * FROM "{Latest_Scoring.__tablename__}"',
            db.engine,
        )[["CustomerId", "Surname", "Email"]]

        tblLatestScored = pd.concat(
            [tblScoringCustomerDetails, tblLatestScored],
            axis=1,
        )
        Latest_Scored.overwrite_self(tblLatestScored)
        Latest_Scored.append_historical(tblLatestScored, Historical_Scored)
        lisJsonResponse = Latest_Scored.to_json()

    timeEnd = time.time()
    print(f"Time taken: {timeEnd - timeStart}")
    return lisJsonResponse


@app.get("/create_emails")
def create_emails():
    objLLM = get_llm_or_raise()

    with db_app.app_context():
        tblLatestScored = pd.read_sql(
            f'SELECT * FROM "{Latest_Scored.__tablename__}"',
            db.engine,
        )

    tblLatestScored = tblLatestScored[tblLatestScored["Prediction"] == 1].copy()

    if tblLatestScored.empty:
        with db_app.app_context():
            db.session.query(Latest_Emails).delete()
            db.session.commit()
            return Latest_Emails.to_json()

    lisLLMResponses = []
    for _, row in tblLatestScored.iterrows():
        dicResult = objLLM.generate_email(
            row["Surname"],
            add_spaces_to_camel_case(row["Top_1_Feat"]),
            row["Top_1_Feat_Value"],
            add_spaces_to_camel_case(row["Top_2_Feat"]),
            row["Top_2_Feat_Value"],
            add_spaces_to_camel_case(row["Top_3_Feat"]),
            row["Top_3_Feat_Value"],
        )
        lisLLMResponses.append(dicResult["Response"])

    tblLatestScored["LLM_Response"] = lisLLMResponses
    tblLatestScored = tblLatestScored[
        [
            strColName
            for strColName in tblLatestScored.columns
            if strColName
            not in [
                "Top_4_Feat",
                "Top_4_Feat_Value",
                "Top_4_Feat_Score",
                "Top_5_Feat",
                "Top_5_Feat_Value",
                "Top_5_Feat_Score",
            ]
        ]
    ].reset_index(drop=True)
    tblLatestScored.to_csv(
        os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVEmails,
        ),
        index=False,
    )

    with db_app.app_context():
        Latest_Emails.overwrite_self(tblLatestScored)
        Latest_Emails.append_historical(tblLatestScored, Historical_Emails)
        return Latest_Emails.to_json()


@app.get("/send_emails")
def send_emails():
    with db_app.app_context():
        tblEmails = pd.read_sql(
            f'SELECT * FROM "{Latest_Emails.__tablename__}"',
            db.engine,
        )

    if tblEmails.empty:
        strPathEmails = os.path.join(
            server_web_config.strPathStorageML,
            server_web_config.strNameCSVEmails,
        )
        if os.path.exists(strPathEmails):
            tblEmails = pd.read_csv(strPathEmails)

    if tblEmails.empty:
        return {"status": "Finished", "sent_count": 0}

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(
            server_web_config.strEmailUser,
            server_web_config.strEmailPass,
        )
        for intIndex, rowRow in tblEmails.iterrows():
            if (intIndex % 100 == 0) and (intIndex != 0):
                print(
                    "[[send_emails()]] Email sending limit reached. Sleeping for 5 minutes..."
                )
                time.sleep(300)

            msg = MIMEText(rowRow["LLM_Response"])
            msg["Subject"] = server_web_config.strEmailSubject
            msg["From"] = server_web_config.strEmailFrom
            msg["To"] = rowRow["Email"]
            server.send_message(msg)
            print("===== check this email sending: =====")
            print(f"To: {rowRow['Email']}")
            print(msg)
            time.sleep(1)

    return {"status": "Finished", "sent_count": len(tblEmails)}


@app.post("/view_results")
def view_tables(data: ViewResultsRequest):
    strTableName = get_valid_table_name(data.strTableVersion, data.strTableName)

    with db_app.app_context():
        with db.engine.connect() as conn:
            sql = text(f'SELECT * FROM "{strTableName}"')
            result = conn.execute(sql)
            tblResult = pd.DataFrame(result.mappings().all())

    return {
        "records": tblResult.to_dict(orient="records"),
        "row_count": len(tblResult),
    }




from fastapi import FastAPI

from routes.api_database import router as database_router
from routes.api_inference import router as inference_router
from routes.api_modelling import router as modelling_router

def create_app() -> FastAPI:
    app = FastAPI(title="LLM Legal API")
    app.include_router(modelling_router)
    app.include_router(inference_router)
    app.include_router(database_router)

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server_web:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
