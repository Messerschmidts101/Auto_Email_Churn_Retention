# python -m app.core.server_web
import os
from pathlib import Path
import pickle
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.routes.api_database import router as database_router
from app.routes.api_modelling import router as training_router
from app.routes.api_inference import router as scoring_router

from app.db.database import objEngine, objBase


BASE_DIR = Path(__file__).resolve().parents[2]
WEBSITE_DIR = BASE_DIR / "website"
STATIC_DIR = WEBSITE_DIR / "static"
INDEX_PATH = WEBSITE_DIR / "index.html"
MAX_LOAD_RETRY = 5


@asynccontextmanager
async def lifespan(app: FastAPI):
    ########################################################
    #######                                          #######
    #######           Step 1: Load Database          #######
    #######                                          #######
    ########################################################
    boolLoadedDatabase = False
    strReasonFailure = ""
    for intRetry in range(0,MAX_LOAD_RETRY):
        try:
            app.state.engine = objEngine
            objBase.metadata.create_all(bind=objEngine)
            boolLoadedDatabase = True
        except Exception as e:
            strReasonFailure = e
    if boolLoadedDatabase:
        print("✅ Database loaded")
    else:
        print("❌ Database not loaded - killing itself")
        print(strReasonFailure)
        

    ########################################################
    #######                                          #######
    #######           Step 2: Load ML Model          #######
    #######                                          #######
    ########################################################
    model_path = os.path.join("models", "model.pkl")

    try:
        with open(model_path, "rb") as f:
            app.state.model = pickle.load(f)
        print("✅ ML Model loaded")
    except:
        print("🛑 No ML Model loaded")

    ########################################################
    #######                                          #######
    #######            Step 3: Run App Now           #######
    #######                                          #######
    ########################################################
    yield

    ########################################################
    #######                                          #######
    #######             Step Extra: Close            #######
    #######                                          #######
    ########################################################
    objEngine.dispose() # cleans up app connection; doesnt delete data nor kill it
    print("🛑 Engine disposed")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Auto Email Churn Prediction",
        lifespan=lifespan
    )

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def landing_page():
        return FileResponse(INDEX_PATH)

    app.include_router(training_router)
    app.include_router(scoring_router)
    app.include_router(database_router)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.core.server_web:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )
