from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
import os

def build_pipeline_model(lisstrColNamesX: list[str], intModel: int, boolVerbose:bool = False, pipeFeatEng = None):
    if intModel == 1:
        objModel = LinearRegression()
    elif intModel == 2:
        objModel = LogisticRegression()
    elif intModel == 3:
        objModel = RandomForestClassifier()
    else:
        raise Exception(f"[[build_pipeline_model]] 😱 Error intModel value: `{intModel}`. Must only be 1, 2, or 3.")
    
    if pipeFeatEng:
        pipeFeatEng = pipeFeatEng(
            lisstrColNamesX = lisstrColNamesX,
            boolVerbose = True,
            strColNameAge = 'Age', 
            strColNameTenure = 'Tenure',
            strColNameAgeTenureRatio = 'Age_Tenure_Ratio',
            strColNameBalance = 'Balance',
            strColNameSalary = 'EstimatedSalary',
            strColNameBalanceSalaryRatio = 'Balance_Salary_Ratio',
        )
        pipeModel = Pipeline(
            pipeFeatEng.steps + [("model", objModel)]
        )
        return pipeModel
    
    else:
        pipeModel = Pipeline([("model", objModel)])
        return pipeModel


def main(
        strPathTrainDataset = os.path.join('documents','train.csv')
    ):
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from ai_ml.a_feateng import pipeline as p 
    
    ########################################################
    #######                                          #######
    #######             Step 1: Load Data            #######
    #######                                          #######
    ########################################################
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
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Showing X Train")
    print(X_train.head(10))
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Showing y Train")
    print(y_train.head(10))
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Showing X Test")
    print(X_test.head(10))
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Showing y Test")
    print(y_test.head(10))
    
    ########################################################
    #######                                          #######
    #######            Step 2: Load Pipe             #######
    #######                                          #######
    ########################################################
    lisstrFeats = X_train.columns.tolist()  # raw feature columns only
    pipeModel = build_pipeline_model(
        lisstrColNamesX = lisstrFeats,
        intModel = 3, 
        pipeFeatEng = p.build_pipeline_feature,
        boolVerbose = True,
    )
    pipeModel.fit(X_train, y_train)
    #pipeModel.transform(X_train)
    
    ########################################################
    #######                                          #######
    #######           Step 3: Print Result           #######
    #######                                          #######
    ########################################################
    y_pred = pipeModel.predict(X_test)
    
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Print Results Y Actual")
    print(y_test.head(10))
    print(f"[[ai_ml.b_model.pipeline]] 🪵 Print Results Y Pred")
    print(y_pred[:10])

if __name__ == "__main__":
    # python -m ai_ml.feateng.pipeline
    main()