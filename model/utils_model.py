import os
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np
import shap
import time
from pandas import DataFrame, Series
from pandas.api.types import is_numeric_dtype, is_string_dtype, is_bool_dtype
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
import joblib


########################################################
#######                                          #######
#######            Step 1: select_col            #######
#######                                          #######
########################################################
class Order_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
    def fit(self, X, y=None):
        return self
    def transform(self, X:DataFrame):
        X = X.copy()
        X['Row_Number'] = np.arange(len(X))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######      Step 2: fix_disguised_null_col      #######
#######                                          #######
########################################################
class Disguised_Nulls_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str],
                 lisstrDisguisedNulls:list[str]=['_','',' '], 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str]=[]
                 ):
        self.lisstrColNames = lisstrColNames
        self.lisstrDisguisedNulls = lisstrDisguisedNulls
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X, y=None):
        return self
    
    def transform(self, X:DataFrame):
        
        X = X.copy()
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                X[strColName] = X[strColName].apply(
                    lambda cell_value: 
                        None if cell_value in self.lisstrDisguisedNulls 
                        else cell_value
                )
        if self.boolVerbose:
            print('finished step 2 Disguised_Nulls_Transformer()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######         Step 3: coerce_col_type          #######
#######                                          #######
########################################################
class Coerce_Type_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], 
                 boolVerbose:bool = False,
                 lisstrColNamesExclude:list[str]=[], 
                 dicCoerce:dict={}):
        """
        # Input
        1. lisstrColNames : list of str. List of column names to attempt type coercion on.
        2. boolVerbose : bool, optional (default=False). If True, prints detailed progress information during transformation.
        3. lisstrColNamesExclude : list of str, optional (default=None). List of column names to exclude from coercion, even if present in `lisstrColNames`.
        4. dicCoerce : dict of {str: str}, optional (default=None). Mapping of column names to desired target data types. Supported type values:
            - `'object'`
            - `'string'`
            - `'int64'`
            - `'float64'`
            - `'datetime64'`
            - `'timedelta64'`
            - `'bool'`
        """
        self.lisstrColNames = lisstrColNames
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude
        self.dicCoerce = dicCoerce

    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame):
        
        X = X.copy()
        lisstrColNamesExcludeLocal = self.lisstrColNamesExclude
        if not self.dicCoerce == {}:
            lisstrColNamesExcludeLocal.extend([strColName for strColName in self.dicCoerce.keys()])

        # Step 1: Attempt to make everything numerical
        for strColName in self.lisstrColNames:
            if strColName not in lisstrColNamesExcludeLocal:
                X[strColName] = pd.to_numeric(
                    X[strColName],
                    errors='ignore'
                )
        # Step 2: Apply user-defined coercion
        for strColName, strDataType in self.dicCoerce.items():
            X[strColName] = X[strColName].astype(strDataType)
        
        if self.boolVerbose:
            print('finished step 3 coerce_col_type()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
        
########################################################
#######                                          #######
#######           Step 4: impute_col             #######
#######                                          #######
########################################################
class Imputer_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False, lisstrColNamesExclude:list[str]=[]):
        self.lisstrColNames = lisstrColNames
        self.dicImpute = {}
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X: DataFrame, y=None):
        # generate dictionary of column name and impute value
        X = X.copy()
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                # Step 1: Get Mode
                if is_string_dtype(X[strColName]) or is_bool_dtype(X[strColName]):
                    anyImputeValue = X[strColName].mode(dropna=True)
                    anyImputeValue = anyImputeValue[0] if not anyImputeValue.empty else None
                # Step 1: Get Mean
                elif is_numeric_dtype(X[strColName]):
                    anyImputeValue = X[strColName].mean()
                # Step 2: Update Impute Dictionary
                self.dicImpute.update({strColName:anyImputeValue})
        return self
    
    def transform(self, X:DataFrame):
        X = X.copy()

        # Step 1: Force None if NaN
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                X[strColName] = X[strColName].apply(
                    lambda cell_value: None if pd.isna(cell_value)
                    else cell_value
                )
        # Step 2: Impute
        X = X.fillna(self.dicImpute)
        
        if self.boolVerbose:
            print('finished step 4 Imputer_Transformer()')
            print(self.dicImpute)
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######            Step 5: encode_col            #######
#######                                          #######
########################################################
class Encoder_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str]=[],
                 strMethod:str = 'Frequency',
                 boolAscending:bool = False
        ):
        """
        # Input
        1. lisstrColNames : list of str. List of column names to encode.
        2. boolVerbose : bool, optional (default=False). If True, prints detailed progress information during fitting and transformation.
        3. lisstrColNamesExclude : list of str, optional (default=[]). List of column names to exclude from encoding, even if present in `lisstrColNames`.
        4. strMethod : str, optional (default='Frequency'). Defines the method used for encoding categorical values. Supported methods:
            - `'Frequency'` : Assigns indices based on frequency of category occurrence.
            - `'Alphabetical'` : Assigns indices based on alphabetical order of category names.
        5. boolAscending : str, optional (default='False'). Defines the order of index assignment. Supported values:
            - `'True'` : Lowest rank or alphabetical category gets smallest index (0).
            - `'False'` : Highest rank or last alphabetical category gets smallest index (0).
        """
        self.lisstrColNames = lisstrColNames
        self.dicMaps = {}
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude
        self.strMethod = strMethod
        self.boolAscending = boolAscending

    def fit(self, X:DataFrame, y=None):
        # generate dictionary of column name and mapping values
        X = X.copy()
        X['Row_Number_Temporary'] = X['Row_Number']
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                if is_string_dtype(X[strColName]):
                    if self.strMethod.lower() == 'frequency':
                        tblMappingValues = X.groupby(
                            strColName
                        ).agg(
                            {'Row_Number_Temporary':'count'}
                        ).sort_values(
                            by = 'Row_Number_Temporary',
                            ascending = self.boolAscending
                        ).reset_index()
                        tblMappingValues['Label'] = np.arange(len(tblMappingValues))
                    elif self.strMethod.lower() == 'alphabetical':
                        tblMappingValues = X.groupby(
                            strColName
                        ).agg(
                            {'Row_Number_Temporary':'count'}
                        ).sort_values(
                            by = strColName,
                            ascending = self.boolAscending
                        )
                        tblMappingValues['Label'] = np.arange(len(tblMappingValues))
                        tblMappingValues = tblMappingValues.drop(
                            'Row_Number_Temporary',
                            axis = 1
                        ).reset_index()
                    self.dicMaps.update({strColName:tblMappingValues})
        return self

    def transform(self, X:DataFrame):
        X = X.copy()
        for strColName,tblMap in self.dicMaps.items():
            if strColName not in self.lisstrColNamesExclude:
                X = pd.merge(
                    X,
                    tblMap,
                    on = strColName,
                    how = 'left'
                )
                # After merging, if your test set has a category unseen during training, it will appear as NaN in 'Label'. So we have to fillna with -1
                X[strColName] = X['Label'].fillna(-1).astype(int)
                X = X.drop(
                    'Label',
                    axis = 1
                )

        if self.boolVerbose:
            print('finished step 5 Encoder_Transformer()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######         Step 6: Age_Tenure_Ratio         #######
#######                                          #######
########################################################
class Age_Tenure_Ratio(BaseEstimator, TransformerMixin):
    def __init__(self, strColNameAge, strColNameTenure, strColNameAgeTenureRatio, boolVerbose:bool = False):
        self.strColNameAge = strColNameAge
        self.strColNameTenure = strColNameTenure
        self.strColNameAgeTenureRatio = strColNameAgeTenureRatio
        self.boolVerbose = boolVerbose
    
    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame):
        X = X.copy()
        X[self.strColNameAgeTenureRatio] = np.where(
            X[self.strColNameTenure] == 0,                      # condition
            0,                                                  # output this value if True
            X[self.strColNameAge] / X[self.strColNameTenure]    # output this value if False
        )
        if self.boolVerbose:
            print('finished step 6 Age_Tenure_Ratio()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######       Step 7: Balance_Salary_Ratio       #######
#######                                          #######
########################################################
class Balance_Salary_Ratio(BaseEstimator, TransformerMixin):
    def __init__(self, strColNameBalance, strColNameSalary, strColNameBalanceSalaryRatio, boolVerbose:bool = False):
        self.strColNameBalance = strColNameBalance
        self.strColNameSalary = strColNameSalary
        self.strColNameBalanceSalaryRatio = strColNameBalanceSalaryRatio
        self.boolVerbose = boolVerbose
    
    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame):
        X = X.copy()
        X[self.strColNameBalanceSalaryRatio] = np.where(
            X[self.strColNameSalary] == 0,                          # condition
            0,                                                      # output this value if True
            X[self.strColNameBalance] / X[self.strColNameSalary]    # output this value if False
        )
        if self.boolVerbose:
            print('finished step 7 Balance_Salary_Ratio()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######            Step 8: select_col            #######
#######                                          #######
########################################################
class Select_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False):
        self.lisstrColNames = lisstrColNames
        self.boolVerbose = boolVerbose
    
    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame):
        X = X.copy()[self.lisstrColNames]
        if self.boolVerbose:
            print('finished step 8 select_col()')
            print(X.head(100))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).drop(
            'Row_Number',
            axis=1
        )

########################################################
#######                                          #######
#######           Step 9: SHAP Explainer         #######
#######                                          #######
########################################################
class SHAPExplanationTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, objModel:RandomForestClassifier, intTopFeatCount=5, boolVerbose = True):
        self.objModel = objModel
        self.intTopFeatCount = intTopFeatCount
        self.objExplainer = shap.TreeExplainer(self.objModel)
        self.boolVerbose = boolVerbose
    
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # np3DShapValues look like this:
        # [
        #   [ # line item 1
        #     [0.000105, -0.000105] # feature 1
        #     [0.003062, -0.003062] # feature 2
        #     [0.011599, -0.011599] # feature 3
        #     [0.005208, -0.005208] # feature 4
        #   ]
        #   [ # line item 2
        #     [0.000105, -0.000105] # feature 1
        #     [0.003062, -0.003062] # feature 2
        #     [0.011599, -0.011599] # feature 3
        #     [0.005208, -0.005208] # feature 4
        #   ]
        # ]
        
        np3DShapValues = self.objExplainer.shap_values(X) 
        np2DPredProba = self.objModel.predict_proba(X) 
        np1DPredictions = self.objModel.predict(X) 
        lisNewPredictionRow = []

        for intIndexRow in range(len(X)):
            dicNewPredictionRow = {
                "Prediction": int(np1DPredictions[intIndexRow]),
                "Churn_Probability": float(np2DPredProba[intIndexRow][1]) # get only proba of class 1
            }
            ########################################################
            #######                                          #######
            #######       Step 1: Get SHAP Of Line Item      #######
            #######                                          #######
            ########################################################
            np2DSHAPSpecific = np3DShapValues[intIndexRow] 

            ########################################################
            #######                                          #######
            #######        Step 2: Attach Index To SHAP      #######
            #######                                          #######
            ########################################################
            # create index 
            np1DIndex = np.arange( # arrIndices -> np1DIndex
                np2DSHAPSpecific.shape[0]
            ).reshape(
                -1,
                1
            )
            # attach index to shap
            np2DSHAPSpecific = np.hstack(
                (
                    np1DIndex, 
                    np2DSHAPSpecific
                )
            )
            if self.boolVerbose:
                print('check SHAP array here:')
                for arrArray in np2DSHAPSpecific:
                    print('-----')
                    print(arrArray)
            ########################################################
            #######                                          #######
            #######    Step 3: Get top 5 features by SHAP    #######
            #######                                          #######
            ########################################################
            # Step 1: order the shap values by descending order of value on positive class
            np2DSHAPSpecific = np2DSHAPSpecific[np.argsort(-np2DSHAPSpecific[:, 2])] # why index 2 instead of 1? remember we attached the index on it so now the list has 3 elements instead of 2  
            # Step 2: add top features
            for intIndexFeature in range(self.intTopFeatCount):
                intIndexFeatureForX = int(np2DSHAPSpecific[intIndexFeature][0])
                strFeatureName = X.columns[intIndexFeatureForX]
                dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat"] = strFeatureName
                dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat_Value"] =  X.iloc[intIndexRow][strFeatureName]
                dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat_Score"] = np2DSHAPSpecific[intIndexFeature][2]  

            ########################################################
            #######                                          #######
            #######              Step 4: Compile             #######
            #######                                          #######
            ########################################################
            lisNewPredictionRow.append(dicNewPredictionRow)
            
        return pd.DataFrame(lisNewPredictionRow)


def main(
        strPathTrainDataset = os.path.join('documents','train.csv'),
    ):
        start = time.time()
        ########################################################
        #######                                          #######
        #######        Step 1: Load Data Training        #######
        #######                                          #######
        ########################################################
        # Learnings:
        # 1. Customer Id doesnt have duplicates
        # 2. Surname, Geo, & Gender can have duplicates
        # 3. Exited 0: 7960
        # 4. Exited 1: 2034
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
        for tblTbl in [X_train, X_test]:
            tblTbl['Row_Number'] = np.arange(len(tblTbl))

        ########################################################
        #######                                          #######
        #######      Step 2: Assemble Base Pipeline      #######
        #######                                          #######
        ########################################################
        lisstrColNamesX = X_train.columns.tolist()
        lisstrColNamesXFinal = lisstrColNamesX + ['Age_Tenure_Ratio','Balance_Salary_Ratio']
        objPipeline = Pipeline([
            ('Order', Order_Transformer()),
            ('Diguised_Nulls', Disguised_Nulls_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
            ('Coerce_Type', Coerce_Type_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
            ('Imputer', Imputer_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
            ('Encoder', Encoder_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
            ('Age_Tenure_Ratio', Age_Tenure_Ratio('Age','Tenure','Age_Tenure_Ratio', boolVerbose=True)),
            ('Balance_Salary_Ratio', Balance_Salary_Ratio('Balance','EstimatedSalary','Balance_Salary_Ratio', boolVerbose=True)),
            ('Selecter', Select_Transformer(lisstrColNamesXFinal, boolVerbose=True)),
            ('Random_Forest', RandomForestClassifier(n_estimators=100, random_state=42))
        ])

        ########################################################
        #######                                          #######
        #######         Step 3: Fit Base Pipeline        #######
        #######                                          #######
        ########################################################
        objPipeline.fit(X_train, y_train)

        ########################################################
        #######                                          #######
        #######                Step 4: Test              #######
        #######                                          #######
        ########################################################
        y_pred = objPipeline.predict(X_test)

        # Calculate metrics
        acc = accuracy_score(y_test, y_pred)
        y_test = y_test.astype(int)
        y_pred = y_pred.astype(int)

        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        cm = confusion_matrix(y_test, y_pred)

        # Store metrics in object
        print('AAAAA: Dependent Variables')
        print(X_test[:10])
        print('BBBBB: Dependent Variables')
        print(y_pred[:10])
        
        print('CCCCC: Metrics')
        print(f"Accuracy: {acc:.4f}")
        print(f"Precision: {prec:.4f}")
        print(f"Recall: {rec:.4f}")
        print(f"F1 Score: {f1:.4f}")
        print("Confusion Matrix:")
        print(cm)

        
        end = time.time()
        print(f"Time taken: {end - start:.2f} seconds")

if __name__ == "__main__":
    main()