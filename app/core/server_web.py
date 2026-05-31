# python -m app.core.server_web
import os
import pickle
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.routes.api_database import router as database_router
from app.routes.api_modelling import router as training_router
from app.routes.api_inference import router as scoring_router

from app.db.database import objEngine, objBase, run_model_table_migrations

import time


BASE_DIR = Path(__file__).resolve().parents[2]
WEBSITE_DIR = BASE_DIR / "website"
STATIC_DIR = WEBSITE_DIR / "static"
INDEX_PATH = WEBSITE_DIR / "index.html"
MAX_LOAD_RETRY = 5
NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code in (200, 304):
            response.headers.update(NO_CACHE_HEADERS)
        return response


def _asset_version(path_asset: Path) -> str:
    try:
        return str(path_asset.stat().st_mtime_ns)
    except FileNotFoundError:
        return "0"


def _render_index_html() -> str:
    str_html = INDEX_PATH.read_text(encoding="utf-8")
    dic_asset_paths = {
        "/static/styles.css": f"/static/styles.css?v={_asset_version(STATIC_DIR / 'styles.css')}",
        "/static/script.js": f"/static/script.js?v={_asset_version(STATIC_DIR / 'script.js')}",
    }
    for str_old_path, str_versioned_path in dic_asset_paths.items():
        str_html = str_html.replace(f'"{str_old_path}"', f'"{str_versioned_path}"')
    return str_html


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
            run_model_table_migrations()
            boolLoadedDatabase = True
        except Exception as e:
            print(f"❌ Database not loaded at attempt {intRetry}")
            time.sleep(30)
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

    app.mount("/static", NoCacheStaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def landing_page():
        return HTMLResponse(
            content=_render_index_html(),
            headers=NO_CACHE_HEADERS,
        )

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
