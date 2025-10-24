import os
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np
import shap
import time
from pandas import DataFrame
objSpark = SparkSession.builder.getOrCreate()

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
        X['Row_Number'] = np.arange(len(X))
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                X[strColName] = X[strColName].apply(
                    lambda cell_value: 
                        None if cell_value in self.lisstrDisguisedNulls 
                        else cell_value
                )
        if self.boolVerbose:
            print('finished step 2 Disguised_Nulls_Transformer()')
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                objType = tblInputData.schema[strColName].dataType
                if isinstance(objType, StringType):
                    anyImputeValue = tblInputData.filter(
                        F.col(strColName).isNotNull()
                    ).groupBy(
                        strColName
                    ).agg(
                        F.count("*").alias(f"temp_count_{strColName}")
                    ).orderBy(
                        F.desc(f"temp_count_{strColName}")
                    ).first()[strColName]

                if isinstance(objType, DoubleType):
                    anyImputeValue = tblInputData.filter(
                        ~((F.isnan(strColName)) | (F.col(strColName).isNull()))
                    ).agg(
                        F.round(F.mean(strColName),2).alias(f"temp_mean_{strColName}")
                    ).first()[f"temp_mean_{strColName}"]
                self.dicImpute.update({strColName:anyImputeValue})
        return self
    
    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                tblInputData = tblInputData.withColumn(
                    strColName,
                    F.when(
                        (F.isnan(strColName)) | (F.col(strColName).isNull()),
                        F.lit(None)
                    ).otherwise(
                        F.col(strColName)
                    )
                )
        tblInputData = tblInputData.na.fill(self.dicImpute)
        if self.boolVerbose:
            print('finished step 4 Imputer_Transformer()')
            print(self.dicImpute)
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
        )
    
########################################################
#######                                          #######
#######            Step 5: encode_col            #######
#######                                          #######
########################################################
class Encoder_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False, lisstrColNamesExclude:list[str]=[]):
        self.lisstrColNames = lisstrColNames
        self.dicMaps = {}
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X:DataFrame, y=None):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
            if strColName not in self.lisstrColNamesExclude:
                objType = tblInputData.schema[strColName].dataType
                if isinstance(objType, (StringType)):
                    objIndexer = StringIndexer(inputCol=strColName, outputCol=f"{strColName}_map")
                    objIndexerModel = objIndexer.fit(tblInputData)
                    tblResult = objIndexerModel.transform(tblInputData) 
                    tblResultMap = tblResult.groupBy(
                        strColName,
                        f'{strColName}_map'
                    ).agg(
                        F.count('*').alias('count')
                    ).orderBy(
                        F.asc(strColName)
                    )
                    self.dicMaps.update({strColName:tblResultMap.toPandas()}) # need to pandas cause it cannot be saved by pipeline
        return self

    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        for strColName,tblMap in self.dicMaps.items():
            if strColName not in self.lisstrColNamesExclude:
                tblInputData = tblInputData.join(
                    objSpark.createDataFrame(tblMap),
                    on = strColName,
                    how = 'left'
                ).withColumn(
                    strColName,
                    F.col(f'{strColName}_map')
                ).drop(
                    f'{strColName}_map',
                    'count'
                )

        if self.boolVerbose:
            print('finished step 5 Encoder_Transformer()')
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
        tblInputData = objSpark.createDataFrame(X)
        tblInputData = tblInputData.withColumn(
            self.strColNameAgeTenureRatio,
            F.when(
                F.col(self.strColNameTenure) == 0,
                F.lit(0)
            ).otherwise(
                F.col(self.strColNameAge) / F.col(self.strColNameTenure)
            )
        )

        if self.boolVerbose:
            print('finished step 6 Age_Tenure_Ratio()')
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
        tblInputData = objSpark.createDataFrame(X)
        tblInputData = tblInputData.withColumn(
            self.strColNameBalanceSalaryRatio,
            F.when(
                F.col(self.strColNameSalary) == 0,
                F.lit(0)
            ).otherwise(
                F.col(self.strColNameBalance) / F.col(self.strColNameSalary)
            )
        )
        if self.boolVerbose:
            print('finished step 7 Balance_Salary_Ratio()')
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
        tblInputData = objSpark.createDataFrame(X)
        tblInputData = tblInputData.select(*self.lisstrColNames)
        if self.boolVerbose:
            print('finished step 8 select_col()')
            print(X.head())
        return X.sort_values(
            by = 'Row_Number',
            ascending = True
        ).sort_values(
            axis = 1
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
    