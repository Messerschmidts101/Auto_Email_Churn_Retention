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

dicMetric = {
    "1" : "f1",
    "2" : "accuracy",
    "3" : "precision",
    "4" : "recall"
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
        classFeat,
        classModel,
        classEval,
    ) -> None:
    if isinstance(intModel, bool) or not isinstance(intModel, int):
        raise TypeError("[[build_pipeline_model_best]] 😱 Error intModel must be an integer.")
    if intModel not in dicTrainingConfigByModel:
        raise ValueError(f"[[build_pipeline_model_best]] 😱 Error intModel value: `{intModel}`. Must only be 1, 2, or 3.")
    if not isinstance(tblData, pd.DataFrame):
        raise TypeError("[[build_pipeline_model_best]] 😱 Error tblData must be a pandas DataFrame.")
    if tblData.empty:
        raise ValueError("[[build_pipeline_model_best]] 😱 Error tblData must not be empty.")
    if "Exited" not in tblData.columns:
        raise ValueError("[[build_pipeline_model_best]] 😱 Error tblData must contain the `Exited` column.")

    objTarget = tblData["Exited"]
    if objTarget.isna().any():
        raise ValueError("[[build_pipeline_model_best]] 😱 Error `Exited` must not contain null values.")
    if tblData.shape[1] <= 1:
        raise ValueError("[[build_pipeline_model_best]] 😱 Error tblData must contain at least one feature column.")
    if isinstance(intCv, bool) or not isinstance(intCv, int) or intCv < 2:
        raise ValueError("[[build_pipeline_model_best]] 😱 Error intCv must be an integer greater than or equal to 2.")
    if intCv > len(tblData):
        raise ValueError("[[build_pipeline_model_best]] 😱 Error intCv cannot be greater than the number of rows in tblData.")
    if intModel in (2, 3):
        if objTarget.nunique() < 2:
            raise ValueError("[[build_pipeline_model_best]] 😱 Error classification models require at least two target classes.")
        intMinClassCount = int(objTarget.value_counts().min())
        if intCv > intMinClassCount:
            raise ValueError("[[build_pipeline_model_best]] 😱 Error intCv cannot be greater than the smallest class count for classification models.")
    if classFeat is not None and not callable(classFeat):
        raise TypeError("[[build_pipeline_model_best]] 😱 Error classFeat must be callable or None.")
    if not callable(classModel):
        raise TypeError("[[build_pipeline_model_best]] 😱 Error classModel must be callable.")
    if not callable(classEval):
        raise TypeError("[[build_pipeline_model_best]] 😱 Error classEval must be callable.")

def build_pipeline_model_best(
        intModel: int,
        tblData: pd.DataFrame,
        intCv: int = 5,
        fltTTSplit: float = .7,
        intPrimaryMetric: int = 1,
        intTopFeats: int = 5,
        boolVerbose: bool = True,
        classFeat = a.build_pipeline_feature,
        classModel = b.build_pipeline_model,
        classEval = c.SHAP_Transformer
    ) -> Pipeline:
    """
    Creates the best model with gridsearch.
    # Inputs:
    1. intModel: int. The model type, only one of the following:
        - `1`: Linear Regression
        - `2`: Logistic Regression
        - `3`: Random Forest
    2. tblData: pd.DataFrame. The input dataframe, containing enough data for both training and testing.
    3. intCv: int = 5. Count of folds for cross validation.
    4. fltTTSplit: float = .7. The ratio of train test split. `.7` indicates that 70% of data will be alotted for training. `.6` indicates that 60% of data will be alloted for testing.
    5. intPrimaryMetric: int = 1. The main metric to determine best model, only one of the following:
        - `1`: f1
        - `2`: accuracy
        - `3`: precision
        - `4`: recall
    6. intTopFeats: int = 5. The amount of top feats to be displayed for scoring. `5` means scoring will display top 5 feats of churning. `4` means scoring will display top 4 feats of churning.
    7. boolVerbose: bool = True. If `True`, display logs in terminal.
    8. classFeat = a.build_pipeline_feature. 
    9. classModel = b.build_pipeline_model.
    10. classEval = c.SHAP_Transformer.

    # Process
    1. Creates a learned model already.
    2. From that learned model, identify its best hyperparameters using gridsearch.
    3. The best model is only then given a shap transformer.

    # Output
    1. Returns the best model object
    2. Returns the performance accuracy metrics of the best model as dictionary
    (TODO: implement this soon) 3. Returns the rest of the models.
    
    """

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
    # Step 1.2.: Load dataset
    X, y = tblData[[strColName for strColName in tblData.columns if strColName != "Exited"]], tblData["Exited"]
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        train_size=fltTTSplit,
        random_state=42,
        stratify=y
    )
    # Step 1.3.: Load gridsearch config
    dicTrainingConfig = dicTrainingConfigByModel[intModel]
    strMetric = dicMetric[f"{intPrimaryMetric}"]


    ########################################################
    #######                                          #######
    #######         Step 2: Commence Training        #######
    #######                  Estimator               #######
    #######                                          #######
    ########################################################
    # Step 2: Load model blueprint, then, creates a learned model
    pipeModel = classModel( # this already produces a trained model
        lisstrColNamesX=X.columns.tolist(),
        intModel=intModel,
        boolVerbose=False, # so many logs will appear as we train many times
        pipeFeatEng=classFeat
    )
    ########################################################
    #######                                          #######
    #######         Step 3: Commence Training        #######
    #######                 Gridsearch               #######
    #######                                          #######
    ########################################################
    # Step 3.1: Load grid training blueprint
    # TODO: we can optimize this by having X,y already transformed, then just load an untrained estimator
    objGridSearch = GridSearchCV(
        estimator = pipeModel,
        param_grid = dicTrainingConfig, # set of mix matches oh hyperparams
        cv = intCv,
        n_jobs = -1, # -1 means we will use all cpu processors,
        scoring = strMetric
    )
    # Step 3.2: Start grid search
    timeStart = datetime.now()
    objGridSearch.fit(X_train, y_train)
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
