import pandas as pd
import joblib
import os
from pandas import DataFrame

def get_scores(pipeEstimator,tblInput,strPathSavePredictions) -> DataFrame:
    """
    # Inputs
    1. `pipeEstimator`: Pipeline. A fitted estimator containing the following:
        - Transformer: Feature Engineering Pipeline
        - Estimator: ML Pipeline
        - Evaluators: SHAP
    2. `tblInput`: DataFrame. Dataset we want to score.
    3. `strPathSavePredictions`: String. Location to store predicted as csv.

    # Process
    1. Loads `tblInput` as pandas DataFrame.
    2. Calls transform on `pipeEstimator`.
    3. Saves scored results as csv.

    # Outputs
    1. Returns scored results as pandas DataFrame.
    """
    ########################################################
    #######                                          #######
    #######         Step 1: Load Data Scoring        #######
    #######                                          #######
    ########################################################
    tblScoring = pd.read_csv(tblInput)
    ########################################################
    #######                                          #######
    #######              Step 2: Predict             #######
    #######                                          #######
    ########################################################
    tblPredictions = pipeEstimator.transform(tblScoring) #pandas df
    print('Check predictions here')
    print(tblPredictions)
    if strPathSavePredictions:
        tblPredictions.to_csv(strPathSavePredictions, index=False)
    return tblPredictions


def main(
        strPathEstimator = os.path.join("artifacts","churn_model_20260407_224905"),
        strPathScoreDataset = os.path.join('documents','scoring.csv'),
        strPathSavePredictions = os.path.join("documents","scored.csv"),
    ):
    get_scores(
        pipeEstimator = joblib.load(strPathEstimator),
        tblInput = strPathScoreDataset,
        strPathSavePredictions = strPathSavePredictions
    )

if __name__ == "__main__":
    main()