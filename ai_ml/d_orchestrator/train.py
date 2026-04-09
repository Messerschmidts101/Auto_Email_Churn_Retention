from ai_ml.a_feateng import pipeline as a
from ai_ml.b_model import pipeline as b
from ai_ml.c_evaluator import transformers as c
from ai_ml import utils as u
from sklearn.pipeline import Pipeline
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
from sklearn.model_selection import GridSearchCV
from datetime import datetime
import joblib

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

def train_test_split_time_based(tblData: pd.DataFrame):
    X, y = tblData[[strColName for strColName in tblData.columns if strColName != "Exited"]], tblData["Exited"]
    return X, y

dicTrainingConfigByModel = {
    1: dicConfigLinearRegression,
    2: dicConfigLogisticRegression,
    3: dicConfigRandomForest,
}

def _validate_build_pipeline_model_best_inputs(
        intModel: int,
        tblData: pd.DataFrame,
        intCv: int,
        classFeat,
        classModel,
        classEval,
    ) -> None:
    if isinstance(intModel, bool) or not isinstance(intModel, int):
        raise TypeError("[[build_pipeline_model_best]] Error intModel must be an integer.")
    if intModel not in dicTrainingConfigByModel:
        raise ValueError(f"[[build_pipeline_model_best]] Error intModel value: `{intModel}`. Must only be 1, 2, or 3.")
    if not isinstance(tblData, pd.DataFrame):
        raise TypeError("[[build_pipeline_model_best]] Error tblData must be a pandas DataFrame.")
    if tblData.empty:
        raise ValueError("[[build_pipeline_model_best]] Error tblData must not be empty.")
    if "Exited" not in tblData.columns:
        raise ValueError("[[build_pipeline_model_best]] Error tblData must contain the `Exited` column.")

    objTarget = tblData["Exited"]
    if objTarget.isna().any():
        raise ValueError("[[build_pipeline_model_best]] Error `Exited` must not contain null values.")
    if tblData.shape[1] <= 1:
        raise ValueError("[[build_pipeline_model_best]] Error tblData must contain at least one feature column.")
    if isinstance(intCv, bool) or not isinstance(intCv, int) or intCv < 2:
        raise ValueError("[[build_pipeline_model_best]] Error intCv must be an integer greater than or equal to 2.")
    if intCv > len(tblData):
        raise ValueError("[[build_pipeline_model_best]] Error intCv cannot be greater than the number of rows in tblData.")
    if intModel in (2, 3):
        if objTarget.nunique() < 2:
            raise ValueError("[[build_pipeline_model_best]] Error classification models require at least two target classes.")
        intMinClassCount = int(objTarget.value_counts().min())
        if intCv > intMinClassCount:
            raise ValueError("[[build_pipeline_model_best]] Error intCv cannot be greater than the smallest class count for classification models.")
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
        boolVerbose: bool = True,
        classFeat = a.build_pipeline_feature,
        classModel = b.build_pipeline_model,
        classEval = c.SHAP_Transformer
    ) -> Pipeline:

    ########################################################
    #######                                          #######
    #######          Step 1: Load Everything         #######
    #######                                          #######
    ########################################################
    # Step 1.1.: Load config of model
    _validate_build_pipeline_model_best_inputs(
        intModel=intModel,
        tblData=tblData,
        intCv=intCv,
        classFeat=classFeat,
        classModel=classModel,
        classEval=classEval,
    )
    dicTrainingConfig = dicTrainingConfigByModel[intModel]
    # Step 1.2.: Load dataset
    X, y = train_test_split_time_based(
        tblData=tblData
    )
    # Step 1.3.: Load model blueprint
    pipeModel = classModel(
        lisstrColNamesX=X.columns.tolist(),
        intModel=intModel,
        boolVerbose=False, # so many logs will appear as we train many times
        pipeFeatEng=classFeat
    )
    # Step 1.4.: Load grid training blueprint
    objGridSearch = GridSearchCV(
        estimator=pipeModel,
        param_grid=dicTrainingConfig,
        cv=intCv,
        n_jobs=-1,
    )
    ########################################################
    #######                                          #######
    #######         Step 2: Commence Training        #######
    #######                                          #######
    ########################################################
    timeStart = datetime.now()
    objGridSearch.fit(X, y)
    timeEnd = datetime.now()
    print(f"[[build_pipeline_model_best]] Time taken grid search: {timeEnd - timeStart}")

    if boolVerbose:
        print("[[build_pipeline_model_best]] 🪵 Keys")
        print(sorted(objGridSearch.cv_results_.keys()))
        print("[[build_pipeline_model_best]] 🪵 Best Params")
        print(objGridSearch.best_params_)
        print("[[build_pipeline_model_best]] 🪵 Best Score")
        print(objGridSearch.best_score_)
        print("[[build_pipeline_model_best]] 🪵 Best Estimator")
        print(objGridSearch.best_estimator_)

    ########################################################
    #######                                          #######
    #######             Step 3: Get Evals            #######
    #######                                          #######
    ########################################################
    dicResults = c.get_all_evals(
        X,
        y,
        pipeModel=objGridSearch.best_estimator_,
    )
    
    ########################################################
    #######                                          #######
    #######             Step 4: Add Shap             #######
    #######                                          #######
    ########################################################
    pipeBestModel = objGridSearch.best_estimator_
    objSHAPTransformer = classEval(
        objEstimator=pipeBestModel.named_steps["model"],
        boolVerbose=False,
    )
    objSHAPTransformer.fit(None, y)
    pipeBestModel = Pipeline(
        pipeBestModel.steps[:-1] + [("shap", objSHAPTransformer)] # replaces the rf with the rf+shap
    )
    
    if boolVerbose:
        print("[[build_pipeline_model_best]] 🪵 Final Model Generated")
        print(sorted(objGridSearch.cv_results_.keys()))
    
    strFileName = u.create_artifact_name('churn_model')
    joblib.dump(pipeBestModel,strFileName)

    ########################################################
    #######                                          #######
    #######          Step 4: Return Results          #######
    #######                                          #######
    ########################################################
    '''
    XTrain, XTest, yTrain, yTest = train_test_split(
        X,
        y,
        test_size=0.2,
        shuffle=False,
    )

    npYTest = np.asarray(yTest, dtype=int)
    npYPred = np.asarray(objGridSearch.best_estimator_.predict(XTest))

    if npYPred.dtype.kind not in {"i", "u", "b"}:
        npYPred = (npYPred >= 0.5).astype(int)
    else:
        npYPred = npYPred.astype(int, copy=False)

    objConfusionMatrix = confusion_matrix(
        npYTest,
        npYPred,
        labels=[0, 1],
    )
    dicResults = {
        "intCountTrainPositiveClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 1)),
        "intCountTrainNegativeClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 0)),
        "intCountTestPositiveClass": int(np.count_nonzero(npYTest == 1)),
        "intCountTestNegativeClass": int(np.count_nonzero(npYTest == 0)),
        "fltAccuracy": float(accuracy_score(npYTest, npYPred)),
        "fltPrecision": float(precision_score(npYTest, npYPred, zero_division=0)),
        "fltRecall": float(recall_score(npYTest, npYPred, zero_division=0)),
        "fltF1": float(f1_score(npYTest, npYPred, zero_division=0)),
        "objConfusionMatrix": objConfusionMatrix,
    }'''
    return pipeBestModel, dicResults

def main(
    strPathTrainDataset = os.path.join('documents','train.csv')
    ):
    
    tblRaw = pd.read_csv(
        strPathTrainDataset
    ).drop(
        'CustomerId', 
        axis='columns'
    )

    build_pipeline_model_best(
        intModel = 3,
        tblData = tblRaw,
        intCv = 5,
        boolVerbose = True,
    )

if __name__ == "__main__":
    main()
