from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from collections.abc import Generator

# Global and Shared
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
objEngine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=objEngine
)
objBase = declarative_base()

MODEL_TABLE_SCHEMA_STATEMENTS = [
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "meta_TrainingRunId" VARCHAR(50)',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "meta_TimestampCreated" TIMESTAMP',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "ModelId" INTEGER',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "ModelName" VARCHAR(100)',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "IsChampion" BOOLEAN',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "GridScore" DOUBLE PRECISION',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "GridTimeTaken" DOUBLE PRECISION',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "Hyperparameters" JSON',
    'ALTER TABLE IF EXISTS "Latest_Models" ADD COLUMN IF NOT EXISTS "TrainingSettings" JSON',
    'ALTER TABLE IF EXISTS "Latest_Models" DROP COLUMN IF EXISTS "FeatureImportance"',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "meta_TrainingRunId" VARCHAR(50)',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "meta_TimestampCreated" TIMESTAMP',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "ModelId" INTEGER',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "ModelName" VARCHAR(100)',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "IsChampion" BOOLEAN',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "GridScore" DOUBLE PRECISION',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "GridTimeTaken" DOUBLE PRECISION',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "Hyperparameters" JSON',
    'ALTER TABLE IF EXISTS "Historical_Models" ADD COLUMN IF NOT EXISTS "TrainingSettings" JSON',
    'ALTER TABLE IF EXISTS "Historical_Models" DROP COLUMN IF EXISTS "FeatureImportance"',
    (
        'UPDATE "Historical_Models" '
        'SET "meta_TrainingRunId" = COALESCE("meta_TrainingRunId", "meta_Id"), '
        '"meta_TimestampCreated" = COALESCE("meta_TimestampCreated", "meta_DateCreated"::timestamp), '
        '"ModelId" = COALESCE("ModelId", 0), '
        '"ModelName" = COALESCE("ModelName", \'Legacy Champion\'), '
        '"IsChampion" = COALESCE("IsChampion", TRUE), '
        '"GridScore" = COALESCE("GridScore", "F1"), '
        '"GridTimeTaken" = COALESCE("GridTimeTaken", 0.0), '
        '"Hyperparameters" = COALESCE("Hyperparameters", \'{}\'::json), '
        '"TrainingSettings" = COALESCE("TrainingSettings", \'{"migration":"legacy_history_row"}\'::json) '
        'WHERE "meta_TrainingRunId" IS NULL '
        'OR "meta_TimestampCreated" IS NULL '
        'OR "ModelId" IS NULL '
        'OR "ModelName" IS NULL '
        'OR "IsChampion" IS NULL '
        'OR "GridScore" IS NULL '
        'OR "GridTimeTaken" IS NULL '
        'OR "Hyperparameters" IS NULL '
        'OR "TrainingSettings" IS NULL'
    ),
]


def run_model_table_migrations() -> None:
    with objEngine.begin() as objConnection:
        for strStatement in MODEL_TABLE_SCHEMA_STATEMENTS:
            objConnection.execute(text(strStatement))

def connect_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
