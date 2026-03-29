from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.db.database import connect_db
from app.db.schema import (
    Historical_Emails,
    Historical_Models,
    Historical_Scored,
    Historical_Scoring,
    Historical_Training,
    Latest_Emails,
    Latest_Scored,
    Latest_Scoring,
    Latest_Training,
)
from app.schema.schema import DTO_Request_ViewTable, DTO_Respond_ViewTable


router = APIRouter(
    prefix="/database",
    tags=["database"]
)


TABLE_MODEL_MAP = {
    ("latest", "training"): Latest_Training,
    ("latest", "scoring"): Latest_Scoring,
    ("latest", "scored"): Latest_Scored,
    ("latest", "emails"): Latest_Emails,
    ("historical", "training"): Historical_Training,
    ("historical", "scoring"): Historical_Scoring,
    ("historical", "scored"): Historical_Scored,
    ("historical", "emails"): Historical_Emails,
    ("historical", "models"): Historical_Models,
}


def orm_rows_to_dicts(rows: list[object]) -> list[dict]:
    return [
        jsonable_encoder(
            {
                column.name: getattr(row, column.name)
                for column in row.__table__.columns
            }
        )
        for row in rows
    ]


@router.get(
    "/table",
    summary="Access tables from the database"
)
def run(
    objRequest: DTO_Request_ViewTable = Depends(),
    objDB: Session = Depends(connect_db)
) -> DTO_Respond_ViewTable:
    if objRequest.strTableName == "models" and objRequest.strTableVersion == "latest":
        rows = (
            objDB.query(Historical_Models)
            .order_by(
                Historical_Models.meta_DateCreated.desc(),
                Historical_Models.meta_Id.desc(),
            )
            .limit(1)
            .all()
        )
    else:
        objTableModel = TABLE_MODEL_MAP.get(
            (objRequest.strTableVersion, objRequest.strTableName)
        )
        if objTableModel is None:
            raise HTTPException(status_code=400, detail="Unsupported table request")

        rows = objDB.query(objTableModel).all()

    tblOutput = orm_rows_to_dicts(rows)

    return DTO_Respond_ViewTable(
        dicStatus={200: "Success"},
        intRowCount=len(tblOutput),
        tblOutput=tblOutput,
    )
