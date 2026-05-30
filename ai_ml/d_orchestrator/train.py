from datetime import datetime
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, make_scorer, precision_score, recall_score
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline

from ai_ml import utils as u
from ai_ml.a_feateng import pipeline as a
from ai_ml.b_model import pipeline as b
from ai_ml.c_evaluator import transformers as c


MODEL_NAME_BY_ID = {
    1: "Linear Regression",
    2: "Logistic Regression",
    3: "Random Forest",
}
DEFAULT_MODEL_ORDER = [3, 1, 2]

dicConfigLinearRegression = {
    "model__fit_intercept": [True, False],
    "model__positive": [False, True],
}
dicConfigLogisticRegression = {
    "model__C": [0.1, 1, 10],
    "model__penalty": ["l2"],
    "model__solver": ["lbfgs"],
    "model__max_iter": [1000],
}
dicConfigRandomForest = {
    "model__n_estimators": [100, 200],
    "model__max_depth": [None, 10, 20],
    "model__min_samples_split": [2, 5],
    "model__min_samples_leaf": [1, 2],
}

dicMetric = {
    "1": "f1",
    "2": "accuracy",
    "3": "precision",
    "4": "recall",
}
METRIC_RESULT_KEY_BY_ID = {
    1: "fltF1",
    2: "fltAccuracy",
    3: "fltPrecision",
    4: "fltRecall",
}

dicTrainingConfigByModel = {
    1: dicConfigLinearRegression,
    2: dicConfigLogisticRegression,
    3: dicConfigRandomForest,
}


def _coerce_binary_predictions(arr_predictions) -> np.ndarray:
    np_predictions = np.asarray(arr_predictions)
    if np_predictions.dtype.kind not in {"i", "u", "b"}:
        np_predictions = (np_predictions >= 0.5).astype(int)
    else:
        np_predictions = np_predictions.astype(int, copy=False)
    return np_predictions


def _score_accuracy(y_true, y_pred) -> float:
    return float(accuracy_score(np.asarray(y_true, dtype=int), _coerce_binary_predictions(y_pred)))


def _score_precision(y_true, y_pred) -> float:
    return float(
        precision_score(
            np.asarray(y_true, dtype=int),
            _coerce_binary_predictions(y_pred),
            zero_division=0,
        )
    )


def _score_recall(y_true, y_pred) -> float:
    return float(
        recall_score(
            np.asarray(y_true, dtype=int),
            _coerce_binary_predictions(y_pred),
            zero_division=0,
        )
    )


def _score_f1(y_true, y_pred) -> float:
    return float(
        f1_score(
            np.asarray(y_true, dtype=int),
            _coerce_binary_predictions(y_pred),
            zero_division=0,
        )
    )


SCORER_BY_METRIC_ID = {
    1: make_scorer(_score_f1),
    2: make_scorer(_score_accuracy),
    3: make_scorer(_score_precision),
    4: make_scorer(_score_recall),
}


def _normalize_parameter_value(value):
    if isinstance(value, np.generic):
        return value.item()
    return value


def _normalize_parameter_dict(dic_params: dict) -> dict:
    return {
        str_key: _normalize_parameter_value(any_value)
        for str_key, any_value in dic_params.items()
    }


def _sample_background_matrix(obj_matrix, int_max_rows: int = 1000):
    if isinstance(obj_matrix, pd.DataFrame):
        if len(obj_matrix) > int_max_rows:
            return obj_matrix.sample(n=int_max_rows, random_state=42)
        return obj_matrix

    if hasattr(obj_matrix, "shape") and obj_matrix.shape[0] > int_max_rows:
        obj_random = np.random.default_rng(42)
        np_sample_index = obj_random.choice(
            obj_matrix.shape[0],
            size=int_max_rows,
            replace=False,
        )
        return obj_matrix[np_sample_index]

    return obj_matrix


def _validate_training_inputs(
        tblData: pd.DataFrame,
        intCv: int,
        fltTTSplit: float,
        intPrimaryMetric: int,
        intRandomState: int,
        strTargetColumn: str,
        lisintModels: list[int],
        classFeat,
        classModel,
        classEval,
    ) -> None:
    if not isinstance(tblData, pd.DataFrame):
        raise TypeError("[[build_pipeline_models_best]] Error tblData must be a pandas DataFrame.")
    if tblData.empty:
        raise ValueError("[[build_pipeline_models_best]] Error tblData must not be empty.")
    if not isinstance(strTargetColumn, str) or not strTargetColumn.strip():
        raise ValueError("[[build_pipeline_models_best]] Error strTargetColumn must be a non-empty string.")
    if strTargetColumn not in tblData.columns:
        raise ValueError(
            f"[[build_pipeline_models_best]] Error tblData must contain the `{strTargetColumn}` column."
        )

    objTarget = tblData[strTargetColumn]
    if objTarget.isna().any():
        raise ValueError(
            f"[[build_pipeline_models_best]] Error `{strTargetColumn}` must not contain null values."
        )
    if tblData.shape[1] <= 1:
        raise ValueError("[[build_pipeline_models_best]] Error tblData must contain at least one feature column.")
    if isinstance(intCv, bool) or not isinstance(intCv, int) or intCv < 2:
        raise ValueError("[[build_pipeline_models_best]] Error intCv must be an integer greater than or equal to 2.")
    if intCv > len(tblData):
        raise ValueError("[[build_pipeline_models_best]] Error intCv cannot be greater than the number of rows in tblData.")
    if not isinstance(fltTTSplit, (int, float)) or not 0 < float(fltTTSplit) < 1:
        raise ValueError("[[build_pipeline_models_best]] Error fltTTSplit must be a number greater than 0 and less than 1.")
    if isinstance(intPrimaryMetric, bool) or intPrimaryMetric not in (1, 2, 3, 4):
        raise ValueError("[[build_pipeline_models_best]] Error intPrimaryMetric must only be 1, 2, 3, or 4.")
    if isinstance(intRandomState, bool) or not isinstance(intRandomState, int):
        raise TypeError("[[build_pipeline_models_best]] Error intRandomState must be an integer.")
    if objTarget.nunique() < 2:
        raise ValueError("[[build_pipeline_models_best]] Error training requires at least two target classes.")
    intMinClassCount = int(objTarget.value_counts().min())
    if intCv > intMinClassCount:
        raise ValueError(
            "[[build_pipeline_models_best]] Error intCv cannot be greater than the smallest class count."
        )
    if not lisintModels:
        raise ValueError("[[build_pipeline_models_best]] Error at least one model must be requested.")
    for intModel in lisintModels:
        if isinstance(intModel, bool) or not isinstance(intModel, int):
            raise TypeError("[[build_pipeline_models_best]] Error every model id must be an integer.")
        if intModel not in dicTrainingConfigByModel:
            raise ValueError(
                f"[[build_pipeline_models_best]] Error intModel value: `{intModel}`. Must only be 1, 2, or 3."
            )
    if classFeat is not None and not callable(classFeat):
        raise TypeError("[[build_pipeline_models_best]] Error classFeat must be callable or None.")
    if not callable(classModel):
        raise TypeError("[[build_pipeline_models_best]] Error classModel must be callable.")
    if not callable(classEval):
        raise TypeError("[[build_pipeline_models_best]] Error classEval must be callable.")


def _train_single_model_with_grid_search(
        intModel: int,
        X_train: pd.DataFrame,
        X_test: pd.DataFrame,
        y_train: pd.Series,
        y_test: pd.Series,
        intCv: int,
        intPrimaryMetric: int,
        intRandomState: int,
        boolVerbose: bool,
        classFeat,
        classModel,
        classEval,
    ) -> dict:
    pipeModel = classModel(
        lisstrColNamesX=X_train.columns.tolist(),
        intModel=intModel,
        boolVerbose=False,
        pipeFeatEng=classFeat,
    )
    objGridSearch = GridSearchCV(
        estimator=pipeModel,
        param_grid=dicTrainingConfigByModel[intModel],
        cv=StratifiedKFold(n_splits=intCv, shuffle=True, random_state=intRandomState),
        n_jobs=-1,
        scoring=SCORER_BY_METRIC_ID[intPrimaryMetric],
        error_score="raise",
    )

    dtmGridStart = datetime.now()
    objGridSearch.fit(X_train, y_train)
    fltGridTimeTaken = (datetime.now() - dtmGridStart).total_seconds()

    if boolVerbose:
        print(f"[[build_pipeline_models_best]] {MODEL_NAME_BY_ID[intModel]} best params")
        print(objGridSearch.best_params_)
        print(f"[[build_pipeline_models_best]] {MODEL_NAME_BY_ID[intModel]} best score")
        print(objGridSearch.best_score_)

    dicResults = c.get_all_evals(
        X_train,
        X_test,
        y_train,
        y_test,
        pipeModel=objGridSearch.best_estimator_,
    )

    objBackground = objGridSearch.best_estimator_[:-1].transform(X_train)
    objBackground = _sample_background_matrix(objBackground)
    objSHAPTransformer = classEval(
        objEstimator=objGridSearch.best_estimator_.named_steps["model"],
        boolVerbose=False,
    )
    objSHAPTransformer.fit(objBackground, y_train)
    pipeScoringModel = Pipeline(
        objGridSearch.best_estimator_.steps[:-1] + [("shap", objSHAPTransformer)]
    )

    return {
        "intModel": intModel,
        "strModelName": MODEL_NAME_BY_ID[intModel],
        "objModel": pipeScoringModel,
        "dicResults": dicResults,
        "dicBestParams": _normalize_parameter_dict(objGridSearch.best_params_),
        "fltGridScore": float(objGridSearch.best_score_),
        "fltGridTimeTaken": fltGridTimeTaken,
    }


def build_pipeline_models_best(
        tblData: pd.DataFrame,
        lisintModels: list[int] | None = None,
        intCv: int = 5,
        fltTTSplit: float = 0.7,
        intPrimaryMetric: int = 1,
        intRandomState: int = 42,
        strTargetColumn: str = "Exited",
        boolVerbose: bool = True,
        classFeat=a.build_pipeline_feature,
        classModel=b.build_pipeline_model,
        classEval=c.SHAP_Transformer,
        boolPersistBestArtifact: bool = False,
    ) -> dict:
    lisintModels = lisintModels or DEFAULT_MODEL_ORDER.copy()
    lisintModelOrder = list(dict.fromkeys(lisintModels))

    _validate_training_inputs(
        tblData=tblData,
        intCv=intCv,
        fltTTSplit=fltTTSplit,
        intPrimaryMetric=intPrimaryMetric,
        intRandomState=intRandomState,
        strTargetColumn=strTargetColumn,
        lisintModels=lisintModelOrder,
        classFeat=classFeat,
        classModel=classModel,
        classEval=classEval,
    )

    X = tblData[[strColName for strColName in tblData.columns if strColName != strTargetColumn]]
    y = tblData[strTargetColumn]
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        train_size=fltTTSplit,
        random_state=intRandomState,
        stratify=y,
    )

    dtmTrainingStart = datetime.now()
    lisdicModelRuns = [
        _train_single_model_with_grid_search(
            intModel=intModel,
            X_train=X_train,
            X_test=X_test,
            y_train=y_train,
            y_test=y_test,
            intCv=intCv,
            intPrimaryMetric=intPrimaryMetric,
            intRandomState=intRandomState,
            boolVerbose=boolVerbose,
            classFeat=classFeat,
            classModel=classModel,
            classEval=classEval,
        )
        for intModel in lisintModelOrder
    ]
    fltTimeTaken = (datetime.now() - dtmTrainingStart).total_seconds()

    strPrimaryMetricKey = METRIC_RESULT_KEY_BY_ID[intPrimaryMetric]
    lisdicModelRuns = sorted(
        lisdicModelRuns,
        key=lambda dicRun: (
            float(dicRun["dicResults"][strPrimaryMetricKey]),
            float(dicRun["dicResults"]["fltAccuracy"]),
            float(dicRun["fltGridScore"]),
        ),
        reverse=True,
    )

    dicBestRun = lisdicModelRuns[0]
    if boolPersistBestArtifact:
        joblib.dump(dicBestRun["objModel"], u.create_artifact_name("churn_model"))

    return {
        "objBestModel": dicBestRun["objModel"],
        "strBestModelName": dicBestRun["strModelName"],
        "dicBestResults": dicBestRun["dicResults"],
        "lisdicModelRuns": lisdicModelRuns,
        "fltTimeTaken": fltTimeTaken,
    }


def build_pipeline_model_best(
        intModel: int,
        tblData: pd.DataFrame,
        intCv: int = 5,
        fltTTSplit: float = 0.7,
        intPrimaryMetric: int = 1,
        intTopFeats: int = 5,
        intRandomState: int = 42,
        strTargetColumn: str = "Exited",
        boolVerbose: bool = True,
        classFeat=a.build_pipeline_feature,
        classModel=b.build_pipeline_model,
        classEval=c.SHAP_Transformer,
        boolPersistArtifact: bool = True,
    ) -> Pipeline:
    dicTrainingRun = build_pipeline_models_best(
        tblData=tblData,
        lisintModels=[intModel],
        intCv=intCv,
        fltTTSplit=fltTTSplit,
        intPrimaryMetric=intPrimaryMetric,
        intRandomState=intRandomState,
        strTargetColumn=strTargetColumn,
        boolVerbose=boolVerbose,
        classFeat=classFeat,
        classModel=classModel,
        classEval=classEval,
        boolPersistBestArtifact=boolPersistArtifact,
    )
    return dicTrainingRun["objBestModel"], dicTrainingRun["dicBestResults"]


def main(
    strPathTrainDataset=os.path.join("documents", "train.csv")
    ):

    tblRaw = pd.read_csv(
        strPathTrainDataset
    ).drop(
        "CustomerId",
        axis="columns"
    )

    build_pipeline_models_best(
        tblData=tblRaw,
        lisintModels=DEFAULT_MODEL_ORDER,
        intCv=5,
        boolVerbose=True,
        boolPersistBestArtifact=True,
    )


if __name__ == "__main__":
    main()
