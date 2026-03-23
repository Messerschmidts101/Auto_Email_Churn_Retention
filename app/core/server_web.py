# python -m app.core.server_web
from fastapi import FastAPI
from contextlib import asynccontextmanager
import pickle
import os

#from app.routes.api_database import router as database_router
#from app.routes.api_inference import router as inference_router
from app.routes.api_modelling import router as modelling_router

from app.db.database import objEngine, objBase


@asynccontextmanager
async def lifespan(app: FastAPI):
    ########################################################
    #######                                          #######
    #######           Step 1: Load Database          #######
    #######                                          #######
    ########################################################
    app.state.engine = objEngine # app.state.engine = adds this to fastapi
    objBase.metadata.create_all(bind=objEngine)
    print("✅ Database loaded")

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

    app.include_router(modelling_router)
    #app.include_router(inference_router)
    #app.include_router(database_router)

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