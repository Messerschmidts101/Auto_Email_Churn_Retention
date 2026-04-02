from collections.abc import Sequence

from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline

from ai_ml.feateng import transformers as t

import os

def build_pipeline_feature(
        lisstrColNamesX:list[str],
        boolVerbose:bool = True,
        strColNameAge = 'Age', 
        strColNameTenure = 'Tenure',
        strColNameAgeTenureRatio = 'Age_Tenure_Ratio',
        strColNameBalance = 'Balance',
        strColNameSalary = 'EstimatedSalary',
        strColNameBalanceSalaryRatio = 'Balance_Salary_Ratio',
    ) -> Pipeline:
    lisstrColNamesX = lisstrColNamesX + [strColNameAgeTenureRatio] + [strColNameBalanceSalaryRatio]
    return Pipeline(
        [
            (
                'Step_1_Fix_Order', 
                t.Order_Transformer()
            ),
            (
                'Step_2_Fix_Disguised_Nulls', 
                t.Disguised_Nulls_Transformer(
                    lisstrDisguisedNulls = ["_",""," "],
                    lisstrColNamesExclude = None,
                    boolVerbose = boolVerbose,
                )
            ),
            (
                'Step_3_Fix_Data_Types', 
                t.Coerce_Type_Transformer(
                    dicCoerce = None,
                    lisstrColNamesExclude = None,
                    boolVerbose = boolVerbose
                )
            ),
            (
                'Step_4_Fix_Nulls', 
                t.Imputer_Transformer(
                    lisstrColNamesExclude = None,
                    boolVerbose = boolVerbose
                )
            ),
            (
                'Step_5_Fix_Labels', 
                t.Encoder_Transformer(
                    strMethod = "Frequency",
                    lisstrColNamesExclude = None,
                    boolVerbose = boolVerbose
                )
            ),
            (
                'Step_6_Add_Ratio_Age_Tenure', 
                t.Age_Tenure_Ratio(
                    strColNameAge = strColNameAge, 
                    strColNameTenure = strColNameTenure,
                    strColNameAgeTenureRatio = strColNameAgeTenureRatio,
                    boolVerbose = boolVerbose
                )
            ),
            (
                'Step_7_Add_Ratio_Balance_Salary', 
                t.Balance_Salary_Ratio(
                    strColNameBalance = strColNameBalance,
                    strColNameSalary = strColNameSalary,
                    strColNameBalanceSalaryRatio = strColNameBalanceSalaryRatio,
                    boolVerbose = boolVerbose)
            ),
            (
                'Step_8_Finalize', 
                t.Select_Transformer(
                    lisstrColNames = lisstrColNamesX,
                    boolVerbose = boolVerbose
                    )
            ),
        ]
    )


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
    objBasePipeline = build_pipeline_feature(
        lisstrColNamesX= X_train.columns.tolist(),
        boolVerbose=True,
    )

    objBasePipeline.fit(X_train, y_train)
    #y_pred = objBasePipeline.predict(X_test)


if __name__ == "__main__":
    # python -m ai_ml.feateng.pipeline
    main()