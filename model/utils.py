import os
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DoubleType
import pyspark.sql.functions as F
import pyspark.sql.window as W
from sklearn.base import BaseEstimator, TransformerMixin
objSpark = SparkSession.builder.getOrCreate()

########################################################
#######                                          #######
#######      Step 1: fix_disguised_null_col      #######
#######                                          #######
########################################################
class Disguised_Nulls_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str],lisstrDisguisedNulls:list[str]=['_','',' '], boolVerbose:bool = False):
        self.lisstrColNames = lisstrColNames
        self.lisstrDisguisedNulls = lisstrDisguisedNulls
        self.boolVerbose = boolVerbose

    def fit(self, X, y=None):
        return self
    
    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
            tblInputData = tblInputData.withColumn(
                strColName,
                F.when(
                    F.col(strColName).isin(self.lisstrDisguisedNulls),
                    F.lit(None)
                ).otherwise(
                    F.col(strColName)
                )
            )
        if self.boolVerbose:
            print('finished step 1 Disguised_Nulls_Transformer()')
            tblInputData.show()
        return tblInputData.toPandas()
    
########################################################
#######                                          #######
#######         Step 2: coerce_col_type          #######
#######                                          #######
########################################################
class Coerce_Type_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False):
        self.lisstrColNames = lisstrColNames
        self.boolVerbose = boolVerbose
    
    def fit(self, X, y=None):
        return self

    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
            intBefore = tblInputData.filter(
                F.col(strColName).isNotNull()
            ).count()
            intAfter = tblInputData.filter(
                F.col(strColName).cast("int").isNotNull()
            ).count()
            if intBefore > intAfter:
                continue
            else: 
                tblInputData = tblInputData.withColumn(
                    strColName,
                    F.col(strColName).cast('double')
                )
        if self.boolVerbose:
            print('finished step 2 coerce_col_type()')
            tblInputData.show()
        return tblInputData.toPandas()
        
########################################################
#######                                          #######
#######           Step 3: impute_col             #######
#######                                          #######
########################################################
class Imputer_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False):
        self.lisstrColNames = lisstrColNames
        self.dicImpute = {}
        self.boolVerbose = boolVerbose

    def fit(self, X: DataFrame, y=None):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
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
            print('finished step 3 Imputer_Transformer()')
            print(self.dicImpute)
            tblInputData.show()
        return tblInputData.toPandas()
    
########################################################
#######                                          #######
#######            Step 4: encode_col            #######
#######                                          #######
########################################################
class Encoder_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str], boolVerbose:bool = False):
        self.lisstrColNames = lisstrColNames
        self.dicMaps = {}
        self.boolVerbose = boolVerbose

    def fit(self, X:DataFrame, y=None):
        tblInputData = objSpark.createDataFrame(X)
        for strColName in self.lisstrColNames:
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
                print('check before updating dic map')
                tblResultMap.show()
                self.dicMaps.update({strColName:tblResultMap.toPandas()}) # need to pandas cause it cannot be saved by pipeline
            else:
                pass
        return self

    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        for strColName,tblMap in self.dicMaps.items():
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
        print('check maps here: ')
        for strColName,tblMap in self.dicMaps.items():
            objSpark.createDataFrame(tblMap).show()

        if self.boolVerbose:
            print('finished step 4 Encoder_Transformer()')
            tblInputData.show()
        return tblInputData.toPandas()
    
########################################################
#######                                          #######
#######            Step 4: encode_col            #######
#######                                          #######
########################################################
