from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype, is_string_dtype, is_bool_dtype
from sklearn.utils.validation import check_is_fitted


def _resolve_columns(X: DataFrame, lisstrColNames: list[str] = None) -> list[str]:
    if lisstrColNames is None or lisstrColNames == []:
        return list(X.columns)
    return list(lisstrColNames)

def _resolve_excluded_columns(lisstrColNamesExclude: list[str] = None) -> set[str]:
    if lisstrColNamesExclude is None or lisstrColNamesExclude == []:
        return set()
    return set(lisstrColNamesExclude)

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
        dicOrderParams = self.dicOrderParams
        if dicOrderParams == {}:
            dicOrderParams = None
        elif dicOrderParams:
            lisstrColNames = dicOrderParams.keys()
            lisstrOrders = dicOrderParams.values()
            lisstrOrders = [
                True if strOrder.lower() == 'asc' or strOrder == True else False
                for strOrder in lisstrOrders
            ]
            dicOrderParams = dict(
                zip(
                    lisstrColNames, 
                    lisstrOrders
                )
            )
        self.order_params_ = dicOrderParams
        return self
    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["order_params_"])
        X = X.copy()
        # Capture original load order before any temporary sorting.
        X['Das_Row_Number'] = np.arange(len(X))
        if self.order_params_:
            X = X.sort_values(
                by = list(self.order_params_.keys()),
                ascending = list(self.order_params_.values())
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
                 lisstrDisguisedNulls:list[str] = None, 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str] = None
                ):
        self.lisstrColNames = lisstrColNames
        self.lisstrDisguisedNulls = ['_','',' '] if lisstrDisguisedNulls is None else lisstrDisguisedNulls
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X, y=None):
        self.columns_ = _resolve_columns(X, self.lisstrColNames)
        self.excluded_columns_ = _resolve_excluded_columns(self.lisstrColNamesExclude)
        self.disguised_nulls_ = tuple(self.lisstrDisguisedNulls)
        return self
    
    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["columns_", "excluded_columns_", "disguised_nulls_"])
        X = X.copy()

        if self.disguised_nulls_:
            for strColName in self.columns_:
                if strColName not in self.excluded_columns_:
                    X[strColName] = X[strColName].replace(list(self.disguised_nulls_), None)
        if self.boolVerbose:
            print('finished step 2 Disguised_Nulls_Transformer()')
            print(X.head(20))
        return X
    
########################################################
#######                                          #######
#######         Step 3: coerce_col_type          #######
#######                                          #######
########################################################
class Coerce_Type_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str] = None, 
                 boolVerbose:bool = False,
                 lisstrColNamesExclude:list[str]=None, 
                 dicCoerce:dict=None):
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
        self.columns_ = _resolve_columns(X, self.lisstrColNames)
        self.coerce_map_ = {} if self.dicCoerce in [None, {}] else dict(self.dicCoerce)
        self.excluded_columns_ = _resolve_excluded_columns(self.lisstrColNamesExclude).union(self.coerce_map_.keys())
        return self

    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["columns_", "coerce_map_", "excluded_columns_"])
        X = X.copy()

        # Step 1: Attempt to make everything numerical
        for strColName in self.columns_:
            if strColName in self.excluded_columns_:
                continue
            try:
                X[strColName] = pd.to_numeric(X[strColName])
            except (ValueError, TypeError):
                continue
        # Step 2: Apply user-defined coercion
        if self.coerce_map_:
            for strColName, strDataType in self.coerce_map_.items():
                X[strColName] = X[strColName].astype(strDataType)
        
        if self.boolVerbose:
            print('finished step 3 coerce_col_type()')
            print(X.head(20))
        return X

########################################################
#######                                          #######
#######           Step 4: impute_col             #######
#######                                          #######
########################################################
class Imputer_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, 
                 lisstrColNames:list[str] = None, 
                 boolVerbose:bool = False, 
                 lisstrColNamesExclude:list[str]=None):
        self.lisstrColNames = lisstrColNames
        self.dicImpute = {}
        self.boolVerbose = boolVerbose
        self.lisstrColNamesExclude = lisstrColNamesExclude

    def fit(self, X: DataFrame, y=None):
        # generate dictionary of column name and impute value
        self.columns_ = _resolve_columns(X, self.lisstrColNames)
        self.excluded_columns_ = _resolve_excluded_columns(self.lisstrColNamesExclude)
        X = X.copy()
        self.dicImpute_ = {}
        for strColName in self.columns_:
            if strColName not in self.excluded_columns_:
                # Step 1: Get Mode
                if is_string_dtype(X[strColName]) or is_bool_dtype(X[strColName]):
                    anyImputeValue = X[strColName].mode(dropna=True)
                    anyImputeValue = anyImputeValue[0] if not anyImputeValue.empty else None
                # Step 1: Get Mean
                elif is_numeric_dtype(X[strColName]):
                    anyImputeValue = X[strColName].mean()
                else:
                    anyImputeValue = X[strColName].mode(dropna=True)
                    anyImputeValue = anyImputeValue[0] if not anyImputeValue.empty else None
                # Step 2: Update Impute Dictionary
                self.dicImpute_[strColName] = anyImputeValue
        return self
    
    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["columns_", "excluded_columns_", "dicImpute_"])
        X = X.copy()
        # Step 2: Impute
        X = X.fillna(self.dicImpute_)
        
        if self.boolVerbose:
            print('finished step 4 Imputer_Transformer()')
            print(self.dicImpute_)
            print(X.head(20))
        return X

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
        strMethod: str = 'frequency', 
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
        self.lisstrColNamesExclude = lisstrColNamesExclude
        self.strMethod = strMethod 
        self.boolAscending = boolAscending
        self.dicMaps = {}

    def fit(self, X: DataFrame, y=None):
        self.strMethod = self.strMethod.lower()
        if self.lisstrColNamesExclude is None:
            self.lisstrColNamesExclude = [] 

        self.columns_ = _resolve_columns(X, self.lisstrColNames)
        self.excluded_columns_ = _resolve_excluded_columns(self.lisstrColNamesExclude)
        self.dicMaps_ = {}
        X = X.copy()

        for strColName in self.columns_:
            if strColName in self.excluded_columns_:
                continue

            if not is_string_dtype(X[strColName]):
                continue

            if self.strMethod == 'frequency':
                dictMap = X[strColName].dropna().value_counts(
                    ascending=self.boolAscending,
                    dropna=True
                ).to_dict()

            elif self.strMethod == 'alphabetical':
                uniques = sorted(
                    X[strColName].dropna().astype(str).unique(),
                    reverse=not self.boolAscending
                )
                dictMap = {k: i for i, k in enumerate(uniques)}

            else:
                raise ValueError(f"Unsupported method: {self.strMethod}")

            self.dicMaps_[strColName] = dictMap

        return self

    def transform(self, X: DataFrame):
        check_is_fitted(self, attributes=["columns_", "excluded_columns_", "dicMaps_"])
        X = X.copy()

        for strColName, dictMap in self.dicMaps_.items():
            if strColName in self.excluded_columns_:
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
        self.output_column_ = self.strColNameAgeTenureRatio
        return self

    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["output_column_"])
        X = X.copy()
        X[self.strColNameAgeTenureRatio] = np.where(
            X[self.strColNameTenure] == 0,                      # condition
            0,                                                  # output this value if True
            X[self.strColNameAge] / X[self.strColNameTenure]    # output this value if False
        )
        if self.boolVerbose:
            print('finished step 6 Age_Tenure_Ratio()')
            print(X.head(20))
        return X
    
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
        self.output_column_ = self.strColNameBalanceSalaryRatio
        return self

    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["output_column_"])
        X = X.copy()
        X[self.strColNameBalanceSalaryRatio] = np.where(
            X[self.strColNameSalary] == 0,                          # condition
            0,                                                      # output this value if True
            X[self.strColNameBalance] / X[self.strColNameSalary]    # output this value if False
        )
        if self.boolVerbose:
            print('finished step 7 Balance_Salary_Ratio()')
            print(X.head(20))
        return X
    
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
        self.selected_columns_ = list(self.lisstrColNames) # useless, exist only so that pipeline/grid search knows the transformer was fitted...
        return self

    def transform(self, X:DataFrame):
        check_is_fitted(self, attributes=["selected_columns_"])
        X = X.copy()
        if 'Das_Row_Number' in X.columns:
            X = X.sort_values(by='Das_Row_Number', ascending=True)
        X = X[self.lisstrColNames]
        if self.boolVerbose:
            print('finished step 8 select_col()')
            print(X.head(20))
        return X
