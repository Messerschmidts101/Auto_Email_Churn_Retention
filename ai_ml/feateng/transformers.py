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


########################################################
#######                                          #######
#######            Step 1: select_col            #######
#######                                          #######
########################################################
class Order_Transformer(BaseEstimator, TransformerMixin):
    """
    Adds row numbers to each data point. This is for accurate processing.    
    """
    def __init__(self, dicOrderParams:dict=None):
        self.dicOrderParams:dict = dicOrderParams
    def fit(self, X, y=None):
        if self.dicOrderParams == {}:
            self.dicOrderParams = None
        elif self.dicOrderParams:
            lisstrColNames = self.dicOrderParams.keys()
            lisstrOrders = self.dicOrderParams.values()
            lisstrOrders = [
                True if strOrder.lower() == 'asc' or strOrder == True else False
                for strOrder in lisstrOrders
            ]
            self.dicOrderParams = dict(
                zip(
                    lisstrColNames, 
                    lisstrOrders
                )
            )
        return self
    def transform(self, X:DataFrame):
        X = X.copy()
        # Capture original load order before any temporary sorting.
        X['Das_Row_Number'] = np.arange(len(X))
        if self.dicOrderParams:
            X = X.sort_values(
                by = list(self.dicOrderParams.keys()),
                ascending = list(self.dicOrderParams.values())
            ).copy()
        return X.sort_values(
            by = 'Das_Row_Number',
            ascending = True
        )

########################################################
#######                                          #######
#######      Step 2: fix_disguised_null_col      #######
#######                                          #######
########################################################
class Disguised_Nulls_Transformer(BaseEstimator, TransformerMixin):
    """
    Coerces disguised nulls as actual None.
    
    """
    def __init__(self, lisstrColNames:list[str] = None,
                 lisstrDisguisedNulls:list[str] = ['_','',' '], 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str] = None
                ):
        self.lisstrColNames = lisstrColNames
        self.lisstrDisguisedNulls = lisstrDisguisedNulls
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X, y=None):
        if self.lisstrColNames == []:
            self.lisstrColNames = None
        if self.lisstrColNamesExclude == []:
            self.lisstrColNamesExclude = None
        return self
    
    def transform(self, X:DataFrame):
        X = X.copy()

        if self.lisstrColNames == None:
            lisstrColNames = X.columns
        else:
            lisstrColNames = self.lisstrColNames

        if self.lisstrColNamesExclude == None:
            lisstrColNamesExclude = []
        else:
            lisstrColNamesExclude = self.lisstrColNamesExclude

        if self.lisstrDisguisedNulls:
            for strColName in lisstrColNames:
                if strColName not in lisstrColNamesExclude:
                    X[strColName] = X[strColName].apply(
                        lambda cell_value: 
                            None if cell_value in self.lisstrDisguisedNulls 
                            else cell_value
                    )
        if self.boolVerbose:
            print('finished step 2 Disguised_Nulls_Transformer()')
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
            ascending = True
        )
    
########################################################
#######                                          #######
#######         Step 3: coerce_col_type          #######
#######                                          #######
########################################################
class Coerce_Type_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str] = None, 
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
        if self.lisstrColNames == []:
            self.lisstrColNames = None
        return self

    def transform(self, X:DataFrame):
        if self.lisstrColNames == None:
            self.lisstrColNames = X.columns

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
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
            ascending = True
        )

########################################################
#######                                          #######
#######           Step 4: impute_col             #######
#######                                          #######
########################################################
class Imputer_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, 
                 lisstrColNames:list[str] = None, 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str]=[]):
        self.lisstrColNames = lisstrColNames
        self.dicImpute = {}
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X: DataFrame, y=None):
        # generate dictionary of column name and impute value
        if self.lisstrColNames == [] or self.lisstrColNames == None:
            self.lisstrColNames = X.columns

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
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
            ascending = True
        )

########################################################
#######                                          #######
#######            Step 5: encode_col            #######
#######                                          #######
########################################################
class Encoder_Transformer(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        lisstrColNames: list[str] = None,
        boolVerbose: bool = False,
        lisstrColNamesExclude: list[str] = None,
        strMethod: str = 'Frequency',
        boolAscending: bool = False
    ):
        
        """
        # Input
        1. lisstrColNames : list of str. List of column names to encode.
        2. boolVerbose : bool, optional (default=False). If True, prints detailed progress information during fitting and transformation.
        3. lisstrColNamesExclude : list of str, optional (default=[]). List of column names to exclude from encoding, even if present in `lisstrColNames`.
        4. strMethod : str, optional (default='Frequency'). Defines the method used for encoding categorical values. Supported methods:
            - `'Frequency'` : Assigns frequency value based on frequency of category occurrence.
            - `'Alphabetical'` : Assigns indices based on alphabetical order of category names.
        5. boolAscending : str, optional (default='False'). Defines the order of index assignment. Supported values:
            - `'True'` : Lowest rank or alphabetical category gets smallest index (0).
            - `'False'` : Highest rank or last alphabetical category gets smallest index (0).
        """
        self.lisstrColNames = lisstrColNames
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude or []
        self.strMethod = strMethod.lower()
        self.boolAscending = boolAscending
        self.dicMaps = {}

    def fit(self, X: DataFrame, y=None):
        if self.lisstrColNames == None or self.lisstrColNames == []:
            self.lisstrColNames = X.columns

        X = X.copy()

        for strColName in self.lisstrColNames:
            if strColName in self.lisstrColNamesExclude:
                continue

            if not is_string_dtype(X[strColName]):
                continue

            if self.strMethod == 'frequency':
                X[strColName] = X[strColName].fillna(0)
                dictMap = X[strColName].value_counts(
                    ascending=self.boolAscending,
                    dropna=True
                ).to_dict()

            elif self.strMethod == 'alphabetical':
                X[strColName] = X[strColName].fillna(-1).astype(int)
                uniques = sorted(X[strColName].dropna().unique(), reverse=not self.boolAscending)
                dictMap = {k: i for i, k in enumerate(uniques)}

            else:
                raise ValueError(f"Unsupported method: {self.strMethod}")

            self.dicMaps[strColName] = dictMap

        return self

    def transform(self, X: DataFrame):
        X = X.copy()

        for strColName, dictMap in self.dicMaps.items():
            if strColName in self.lisstrColNamesExclude:
                continue

            # map instead of merge (faster + safe)
            X[strColName] = X[strColName].map(dictMap).fillna(-1).astype(int)

        if self.boolVerbose:
            print('finished Encoder_Transformer()')
            print(X.head(20))

        return X

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
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
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
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
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
            print(X.head(20))
        return X.sort_values(
            by = 'Das_Row_Number',
            ascending = True
        ).drop(
            'Das_Row_Number',
            axis=1
        )
