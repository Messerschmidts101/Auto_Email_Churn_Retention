import pandas as pd
from sklearn.model_selection import GridSearchCV
from ai_ml.model import pipeline as p
import os

from datetime import datetime

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
    ):
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
    
    ########################################################
    #######                                          #######
    #######          Step 3: Output Results          #######
    #######                                          #######
    ########################################################
    print(f"[[train_model_many]] 🪵 Showing Grid Search Result. Time taken {timeEnd-timeStart}")
    print(f"[[train_model_many]] 🪵 Keys")
    print(sorted(objGridSearch.cv_results_.keys()))
    print(f"[[train_model_many]] 🪵 Best Params")
    print(objGridSearch.best_params_)
    print(f"[[train_model_many]] 🪵 Best Score")
    print(objGridSearch.best_score_)
    print(f"[[train_model_many]] 🪵 Best Estimator")
    print(objGridSearch.best_estimator_)
    """
    [[train_model_many]] 🪵 Showing Grid Search Result. Time taken 0:02:52.028190
    [[train_model_many]] 🪵 Keys
    ['mean_fit_time', 'mean_score_time', 'mean_test_score', 'param_model__max_depth', 'param_model__min_samples_leaf', 'param_model__min_samples_split', 'param_model__n_estimators', 'params', 'rank_test_score', 'split0_test_score', 'split1_test_score', 'split2_test_score', 'split3_test_score', 'split4_test_score', 'std_fit_time', 'std_score_time', 'std_test_score']
    [[train_model_many]] 🪵 Best Params
    {'model__max_depth': None, 'model__min_samples_leaf': 1, 'model__min_samples_split': 5, 'model__n_estimators': 200}
    [[train_model_many]] 🪵 Best Score
    0.8794279021392578
    [[train_model_many]] 🪵 Best Estimator
    Pipeline(steps=[('Step_1_Fix_Order', Order_Transformer()),
                    ('Step_2_Fix_Disguised_Nulls',
                    Disguised_Nulls_Transformer(boolVerbose=True,
                                                lisstrDisguisedNulls=['_', '',
                                                                    ' '])),
                    ('Step_3_Fix_Data_Types',
                    Coerce_Type_Transformer(boolVerbose=True)),
                    ('Step_4_Fix_Nulls', Imputer_Transformer(boolVerbose=True)),
                    ('Step_5_Fix_Labels',
                    Encoder_Transformer(boolVerbose=Tru...
                                        strColNameSalary='EstimatedSalary')),
                    ('Step_8_Finalize',
                    Select_Transformer(boolVerbose=True,
                                        lisstrColNames=['Surname', 'CreditScore',
                                                        'Geography', 'Gender',
                                                        'Age', 'Tenure', 'Balance',
                                                        'NumOfProducts',
                                                        'HasCrCard',
                                                        'IsActiveMember',
                                                        'EstimatedSalary',
                                                        'RecentSatisfactionScore',
                                                        'Age_Tenure_Ratio',
                                                        'Balance_Salary_Ratio'])),
                    ('model',
                    RandomForestClassifier(min_samples_split=5,
                                            n_estimators=200))])
    """



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
    