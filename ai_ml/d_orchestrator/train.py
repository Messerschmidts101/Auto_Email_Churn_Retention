from datetime import datetime
import os

import joblib
import pandas as pd
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline

from ai_ml import utils as u
from ai_ml.a_feateng import pipeline as a
from ai_ml.b_model import pipeline as b
from ai_ml.c_evaluator import transformers as c


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

dicTrainingConfigByModel = {
    1: dicConfigLinearRegression,
    2: dicConfigLogisticRegression,
    3: dicConfigRandomForest,
}


def _validate_build_pipeline_model_best_inputs(
        intModel: int,
        tblData: pd.DataFrame,
        intCv: int,
        fltTTSplit: float,
        intPrimaryMetric: int,
        intRandomState: int,
        strTargetColumn: str,
        classFeat,
        classModel,
        classEval,
    ) -> None:
    if isinstance(intModel, bool) or not isinstance(intModel, int):
        raise TypeError("[[build_pipeline_model_best]] Error intModel must be an integer.")
    if intModel not in dicTrainingConfigByModel:
        raise ValueError(
            f"[[build_pipeline_model_best]] Error intModel value: `{intModel}`. Must only be 1, 2, or 3."
        )
    if not isinstance(tblData, pd.DataFrame):
        raise TypeError("[[build_pipeline_model_best]] Error tblData must be a pandas DataFrame.")
    if tblData.empty:
        raise ValueError("[[build_pipeline_model_best]] Error tblData must not be empty.")
    if not isinstance(strTargetColumn, str) or not strTargetColumn.strip():
        raise ValueError("[[build_pipeline_model_best]] Error strTargetColumn must be a non-empty string.")
    if strTargetColumn not in tblData.columns:
        raise ValueError(
            f"[[build_pipeline_model_best]] Error tblData must contain the `{strTargetColumn}` column."
        )

    objTarget = tblData[strTargetColumn]
    if objTarget.isna().any():
        raise ValueError(
            f"[[build_pipeline_model_best]] Error `{strTargetColumn}` must not contain null values."
        )
    if tblData.shape[1] <= 1:
        raise ValueError("[[build_pipeline_model_best]] Error tblData must contain at least one feature column.")
    if isinstance(intCv, bool) or not isinstance(intCv, int) or intCv < 2:
        raise ValueError("[[build_pipeline_model_best]] Error intCv must be an integer greater than or equal to 2.")
    if intCv > len(tblData):
        raise ValueError("[[build_pipeline_model_best]] Error intCv cannot be greater than the number of rows in tblData.")
    if not isinstance(fltTTSplit, (int, float)) or not 0 < float(fltTTSplit) < 1:
        raise ValueError("[[build_pipeline_model_best]] Error fltTTSplit must be a number greater than 0 and less than 1.")
    if isinstance(intPrimaryMetric, bool) or intPrimaryMetric not in (1, 2, 3, 4):
        raise ValueError("[[build_pipeline_model_best]] Error intPrimaryMetric must only be 1, 2, 3, or 4.")
    if isinstance(intRandomState, bool) or not isinstance(intRandomState, int):
        raise TypeError("[[build_pipeline_model_best]] Error intRandomState must be an integer.")
    if intModel in (2, 3):
        if objTarget.nunique() < 2:
            raise ValueError("[[build_pipeline_model_best]] Error classification models require at least two target classes.")
        intMinClassCount = int(objTarget.value_counts().min())
        if intCv > intMinClassCount:
            raise ValueError(
                "[[build_pipeline_model_best]] Error intCv cannot be greater than the smallest class count for classification models."
            )
    if classFeat is not None and not callable(classFeat):
        raise TypeError("[[build_pipeline_model_best]] Error classFeat must be callable or None.")
    if not callable(classModel):
        raise TypeError("[[build_pipeline_model_best]] Error classModel must be callable.")
    if not callable(classEval):
        raise TypeError("[[build_pipeline_model_best]] Error classEval must be callable.")


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
        classEval=c.SHAP_Transformer
    ) -> Pipeline:
    """
    Creates the best model with grid search.
    """

    ########################################################
    #######                                          #######
    #######          Step 1: Load Everything         #######
    #######                                          #######
    ########################################################
    _validate_build_pipeline_model_best_inputs(
        intModel=intModel,
        tblData=tblData,
        intCv=intCv,
        fltTTSplit=fltTTSplit,
        intPrimaryMetric=intPrimaryMetric,
        intRandomState=intRandomState,
        strTargetColumn=strTargetColumn,
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
    dicTrainingConfig = dicTrainingConfigByModel[intModel]
    strMetric = dicMetric[f"{intPrimaryMetric}"]

    ########################################################
    #######                                          #######
    #######         Step 2: Commence Training        #######
    #######                  Estimator               #######
    #######                                          #######
    ########################################################
    pipeModel = classModel(
        lisstrColNamesX=X.columns.tolist(),
        intModel=intModel,
        boolVerbose=False,
        pipeFeatEng=classFeat,
    )

    ########################################################
    #######                                          #######
    #######         Step 3: Commence Training        #######
    #######                 Gridsearch               #######
    #######                                          #######
    ########################################################
    objGridSearch = GridSearchCV(
        estimator=pipeModel,
        param_grid=dicTrainingConfig,
        cv=intCv,
        n_jobs=-1,
        scoring=strMetric,
    )
    timeStart = datetime.now()
    objGridSearch.fit(X_train, y_train)
    timeEnd = datetime.now()
    print(f"[[build_pipeline_model_best]] Time taken grid search: {timeEnd - timeStart}")

    if boolVerbose:
        print("[[build_pipeline_model_best]] Keys")
        print(sorted(objGridSearch.cv_results_.keys()))
        print("[[build_pipeline_model_best]] Best Params")
        print(objGridSearch.best_params_)
        print("[[build_pipeline_model_best]] Best Score")
        print(objGridSearch.best_score_)
        print("[[build_pipeline_model_best]] Best Estimator")
        print(objGridSearch.best_estimator_)

    ########################################################
    #######                                          #######
    #######             Step 3: Get Evals            #######
    #######                                          #######
    ########################################################
    dicResults = c.get_all_evals(
        X_train,
        X_test,
        y_train,
        y_test,
        pipeModel=objGridSearch.best_estimator_,
    )

    ########################################################
    #######                                          #######
    #######             Step 4: Add Shap             #######
    #######                and refit                 #######
    #######                                          #######
    ########################################################
    pipeBestModel = objGridSearch.best_estimator_
    objSHAPTransformer = classEval(
        objEstimator=pipeBestModel.named_steps["model"],
        boolVerbose=False,
    )
    objSHAPTransformer.fit(None, y)
    pipeBestModel = Pipeline(
        pipeBestModel.steps[:-1] + [("shap", objSHAPTransformer)]
    )

    if boolVerbose:
        print("[[build_pipeline_model_best]] Final Model Generated")
        print(sorted(objGridSearch.cv_results_.keys()))

    strFileName = u.create_artifact_name("churn_model")
    joblib.dump(pipeBestModel, strFileName)

    ########################################################
    #######                                          #######
    #######          Step 4: Return Results          #######
    #######                                          #######
    ########################################################
    return pipeBestModel, dicResults


def main(
    strPathTrainDataset=os.path.join("documents", "train.csv")
    ):

    tblRaw = pd.read_csv(
        strPathTrainDataset
    ).drop(
        "CustomerId",
        axis="columns"
    )

    build_pipeline_model_best(
        intModel=3,
        tblData=tblRaw,
        intCv=5,
        boolVerbose=True,
    )


if __name__ == "__main__":
    main()
