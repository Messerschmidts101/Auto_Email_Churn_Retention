import os
from pyspark.ml import Estimator, Model, Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable, MLReadable, MLWritable, MLReader, MLWriter
from pyspark.ml.param.shared import Param, Params
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType, TimestampType, StringType, DoubleType, NumericType
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
    def __init__(self, lisstrColNames:list[str],lisstrDisguisedNulls:list[str]=['_','',' ']):
        self.lisstrColNames = lisstrColNames
        self.lisstrDisguisedNulls = lisstrDisguisedNulls

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

        return tblInputData.toPandas()

########################################################
#######                                          #######
#######         Step 2: coerce_col_type          #######
#######                                          #######
########################################################

class Coerce_Type_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str]):
        self.lisstrColNames = lisstrColNames
    
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
        return tblInputData.toPandas()
        
########################################################
#######                                          #######
#######           Step 3: impute_col             #######
#######                                          #######
########################################################

class Imputer_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, lisstrColNames:list[str]):
        self.lisstrColNames = lisstrColNames
        self.dicImpute = {}

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

            if isinstance(objType, NumericType):
                anyImputeValue = tblInputData.filter(
                    F.col(strColName).isNotNull()
                ).agg(
                    F.mean(strColName).alias(f"temp_mean_{strColName}")
                ).first()[f"temp_mean_{strColName}"]

            self.dicImpute.update({strColName:anyImputeValue})
        return self
    
    def transform(self, X:DataFrame):
        tblInputData = objSpark.createDataFrame(X)
        return tblInputData.na.fill(self.dicImpute).toPandas()