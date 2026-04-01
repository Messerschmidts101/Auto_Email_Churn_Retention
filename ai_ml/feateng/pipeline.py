from collections.abc import Sequence

from sklearn.base import BaseEstimator
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline

from ai_ml.feateng import transformers as t

import os


ROW_NUMBER_COL = "Das_Row_Number"
AGE_TENURE_RATIO_COL = "Age_Tenure_Ratio"
BALANCE_SALARY_RATIO_COL = "Balance_Salary_Ratio"


def _normalize_feature_columns(feature_columns: Sequence[str]) -> list[str]:
    columns = [column for column in feature_columns if column != ROW_NUMBER_COL]
    if not columns:
        raise ValueError("feature_columns must contain at least one input column.")
    return columns


def build_feature_pipeline(
    feature_columns: Sequence[str],
    *,
    boolVerbose: bool = False,
    dicOrderParams: dict | None = None,
    lisstrDisguisedNulls: list[str] | None = None,
    dicCoerce: dict | None = None,
    strMethod: str = "Frequency",
    boolAscending: bool = False,
    strColNameAge: str = "Age",
    strColNameTenure: str = "Tenure",
    strColNameBalance: str = "Balance",
    strColNameSalary: str = "EstimatedSalary",
    strColNameAgeTenureRatio: str = AGE_TENURE_RATIO_COL,
    strColNameBalanceSalaryRatio: str = BALANCE_SALARY_RATIO_COL,
) -> Pipeline:
    base_columns = _normalize_feature_columns(feature_columns)
    excluded_columns = [ROW_NUMBER_COL]
    selected_columns = [
        *base_columns,
        strColNameAgeTenureRatio,
        strColNameBalanceSalaryRatio,
        ROW_NUMBER_COL,
    ]

    return Pipeline(
        [
            ("order", t.Order_Transformer(dicOrderParams=dicOrderParams)),
            (
                "disguised_nulls",
                t.Disguised_Nulls_Transformer(
                    lisstrColNames=list(base_columns),
                    lisstrDisguisedNulls=lisstrDisguisedNulls or ["_", "", " "],
                    boolVerbose=boolVerbose,
                    lisstrColNamesExclude=excluded_columns.copy(),
                ),
            ),
            (
                "coerce_type",
                t.Coerce_Type_Transformer(
                    lisstrColNames=list(base_columns),
                    boolVerbose=boolVerbose,
                    lisstrColNamesExclude=excluded_columns.copy(),
                    dicCoerce=dicCoerce or {},
                ),
            ),
            (
                "imputer",
                t.Imputer_Transformer(
                    lisstrColNames=list(base_columns),
                    boolVerbose=boolVerbose,
                    lisstrColNamesExclude=excluded_columns.copy(),
                ),
            ),
            (
                "encoder",
                t.Encoder_Transformer(
                    lisstrColNames=list(base_columns),
                    boolVerbose=boolVerbose,
                    lisstrColNamesExclude=excluded_columns.copy(),
                    strMethod=strMethod,
                    boolAscending=boolAscending,
                ),
            ),
            (
                "age_tenure_ratio",
                t.Age_Tenure_Ratio(
                    strColNameAge=strColNameAge,
                    strColNameTenure=strColNameTenure,
                    strColNameAgeTenureRatio=strColNameAgeTenureRatio,
                    boolVerbose=boolVerbose,
                ),
            ),
            (
                "balance_salary_ratio",
                t.Balance_Salary_Ratio(
                    strColNameBalance=strColNameBalance,
                    strColNameSalary=strColNameSalary,
                    strColNameBalanceSalaryRatio=strColNameBalanceSalaryRatio,
                    boolVerbose=boolVerbose,
                ),
            ),
            (
                "select",
                t.Select_Transformer(
                    lisstrColNames=selected_columns,
                    boolVerbose=boolVerbose,
                ),
            ),
        ]
    )


def build_model_pipeline(
    feature_columns: Sequence[str],
    *,
    estimator: BaseEstimator | None = None,
    boolVerbose: bool = False,
    dicOrderParams: dict | None = None,
    lisstrDisguisedNulls: list[str] | None = None,
    dicCoerce: dict | None = None,
    strMethod: str = "Frequency",
    boolAscending: bool = False,
    strColNameAge: str = "Age",
    strColNameTenure: str = "Tenure",
    strColNameBalance: str = "Balance",
    strColNameSalary: str = "EstimatedSalary",
    strColNameAgeTenureRatio: str = AGE_TENURE_RATIO_COL,
    strColNameBalanceSalaryRatio: str = BALANCE_SALARY_RATIO_COL,
) -> Pipeline:
    model = estimator or RandomForestClassifier(n_estimators=100, random_state=42)
    feature_pipeline = build_feature_pipeline(
        feature_columns=feature_columns,
        boolVerbose=boolVerbose,
        dicOrderParams=dicOrderParams,
        lisstrDisguisedNulls=lisstrDisguisedNulls,
        dicCoerce=dicCoerce,
        strMethod=strMethod,
        boolAscending=boolAscending,
        strColNameAge=strColNameAge,
        strColNameTenure=strColNameTenure,
        strColNameBalance=strColNameBalance,
        strColNameSalary=strColNameSalary,
        strColNameAgeTenureRatio=strColNameAgeTenureRatio,
        strColNameBalanceSalaryRatio=strColNameBalanceSalaryRatio,
    )

    return Pipeline(feature_pipeline.steps + [("model", model)])


__all__ = [
    "ROW_NUMBER_COL",
    "AGE_TENURE_RATIO_COL",
    "BALANCE_SALARY_RATIO_COL",
    "build_feature_pipeline",
    "build_model_pipeline",
]

def main(
        strPathTrainDataset = os.path.join('documents','train.csv'),
    ):
    #from ai_ml.feateng.pipeline import build_model_pipeline
    import pandas as pd
    from sklearn.model_selection import train_test_split
    tblRaw = pd.read_csv(
                strPathTrainDataset
            ).drop(
                'CustomerId', 
                axis='columns'
            )
    X,y = tblRaw[[strColName for strColName in tblRaw.columns if strColName != 'Exited']], tblRaw['Exited']
    X_train, X_test, y_train, y_test = train_test_split(
                X, 
                y, 
                test_size=0.2, 
                random_state=42
            )

    print(X_train.head(20))
    print(y_train.head(20))
    print(X_test.head(20))
    feature_columns = X_train.columns.tolist()  # raw feature columns only
    objBasePipeline = build_model_pipeline(
        feature_columns=feature_columns,
        dicOrderParams = {
            'Surname':"asc",
            'CreditScore':"asc"
        },
        boolVerbose=True,
    )

    objBasePipeline.fit(X_train, y_train)
    y_pred = objBasePipeline.predict(X_test)


if __name__ == "__main__":
    # python -m ai_ml.feateng.pipeline
    main()