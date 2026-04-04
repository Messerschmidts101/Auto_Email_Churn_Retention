import pandas as pd
from sklearn.model_selection import GridSearchCV
from ai_ml.model import pipeline as p
from ai_ml import utils as u
import os
import joblib
from datetime import datetime
from sklearn.pipeline import Pipeline

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

# TODO: actual split maybe
def train_test_split_time_based(tblData: pd.DataFrame):
    X,y = tblData[[strColName for strColName in tblData.columns if strColName != 'Exited']], tblData['Exited']
    return X,y

def train_model_many(
    intModel: int,
    tblData: pd.DataFrame,
    intCv: int = 5,
    boolVerbose: bool = True
    ) -> Pipeline:
    """
    # Inputs:
    1. `intModel`: int. The chosen model, could only either be of the following:
        - `1` - Linear Regression
        = `2` - Logistic Regression
        = `3` - Random Forest
    2. `tblData`: pd.DataFrame,. The training dataframe. 
    3. `intCv`: Integer. Default `5`. Not yet functional! Intended to set paritions for cross validation.
    4. `boolVerbose`: Boolean. Default `True`

    # Process:
    1. Train test splits the `tblData`.
    2. Assembles the pipeline model. Combines pipeline for feature transformations, and adds the estimator (chosen `intModel`) as final step.
    3. Commences grid search for the possible hyperparameters of the `intModel`.

    # Outputs:
    1. Outputs only the best model of the grid search.
    """
    ########################################################
    #######                                          #######
    #######          Step 1: Load Everything         #######
    #######                                          #######
    ########################################################
    # Step 1.1.: Load config of model
    if intModel == 1:
        dicTrainingConfig = dicConfigLinearRegression
    elif intModel == 2:
        dicTrainingConfig = dicConfigLogisticRegression
    elif intModel == 3:
        dicTrainingConfig = dicConfigRandomForest
    else:
        raise Exception(f"[[train_model_many]] 😱 Error intModel value: `{intModel}`. Must only be 1, 2, or 3.")
    # Step 1.2.: Load dataset
    X,y = train_test_split_time_based(
        tblData = tblData
    )
    # Step 1.3.: Load model blueprint
    pipeModel = p.build_pipeline_model(
        lisstrColNamesX=X.columns.tolist(),
        intModel=intModel,
        boolVerbose=False, # so many logs will appear as we train many times
    )
    # Step 1.4.: Load grid training blueprint
    objGridSearch = GridSearchCV(
        estimator=pipeModel,
        param_grid=dicTrainingConfig,
    )

    ########################################################
    #######                                          #######
    #######         Step 2: Commence Training        #######
    #######                                          #######
    ########################################################
    timeStart = datetime.now()
    objGridSearch.fit(X,y)
    timeEnd = datetime.now()
    print(f"[[train_model_many]] 🪵 Time taken grid search: {timeEnd-timeStart}")
    ########################################################
    #######                                          #######
    #######          Step 3: Output Results          #######
    #######                                          #######
    ########################################################
    if boolVerbose:
        print(f"[[train_model_many]] 🪵 Keys")
        print(sorted(objGridSearch.cv_results_.keys()))
        print(f"[[train_model_many]] 🪵 Best Params")
        print(objGridSearch.best_params_)
        print(f"[[train_model_many]] 🪵 Best Score")
        print(objGridSearch.best_score_)
        print(f"[[train_model_many]] 🪵 Best Estimator")
        print(objGridSearch.best_estimator_)

    # this outputs the pipeline
    pipeBestModel = objGridSearch.best_estimator_
    strFileName = u.create_artifact_name('transformer')
    joblib.dump(pipeBestModel,strFileName)

    return pipeBestModel

def main(
    strPathTrainDataset = os.path.join('documents','train.csv')
):
    tblRaw = pd.read_csv(
        strPathTrainDataset
    ).drop(
        'CustomerId', 
        axis='columns'
    )
    
    train_model_many(
        intModel = 3,
        tblData = tblRaw,
    )


if __name__ == "__main__":
    main()
    