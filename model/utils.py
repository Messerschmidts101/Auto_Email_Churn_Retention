from pyspark.sql.types import DateType, TimestampType, StringType, DoubleType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.window as W
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
import time
import os
objSpark = SparkSession.builder.getOrCreate()


class Preprocessing:
    def __init__(self, tblRaw:DataFrame = None):
        self.tblRaw = tblRaw
        self.tblResult = None
        self.dictblLabelMap = {
            # strColName: objDataFrame
        }
        self.strPathDataLabelMap = os.path.join('model','dataset','data_label_map')
        os.makedirs(
            os.path.join('model','dataset','data_label_map'),
            exist_ok=True
        )

    def fix_disguised_null_col(self,tblInput:DataFrame,strColName:str,lisstrDisguisedNulls:list[str]=[' ', '', '_']):
        tblResult = tblInput.withColumn(
            strColName,
            F.when(
                F.col(strColName).isin(lisstrDisguisedNulls),
                None
            ).otherwise(
                F.col(strColName)
            )
        )
        return tblResult

    def coerce_col_type(self,tblInput:DataFrame,strColName:str):
        objType = tblInput.schema[strColName].dataType
        if isinstance(objType, (DateType, TimestampType)):
            return tblInput
        
        # its better to ask for forgiveness than check
        # we are checking if the values are numerical
        # if the number of nulls have increased, this means this column is supposed to be alphanumeric
        # the nulls will increase because if F.sum() is worked on numeric, it silently fails by converting string to null.
        intInitialNullCount = tblInput.select(
            F.sum(
                F.when(F.col(strColName).isNull(), 1).otherwise(0)
            ).alias('temp_null_count')
        ).collect()[0]['temp_null_count']

        tblResult = tblInput.withColumn(
            strColName,
            F.col(strColName).cast('double')
        )

        intFinalNullCount = tblResult.select(
            F.sum(
                F.when(F.col(strColName).isNull(), 1).otherwise(0)
            ).alias('temp_null_count')
        ).collect()[0]['temp_null_count']

        if intInitialNullCount >= intFinalNullCount:
            return tblResult
        else:
            return tblInput.withColumn(
                strColName,
                F.col(strColName).cast('string')
            )

    def impute_col(self,tblInput,strColName):
        objType = tblInput.schema[strColName].dataType
        # use mode
        if isinstance(objType, StringType):
            strMode = tblInput.groupBy(
                strColName
            ).agg(
                F.count(strColName).alias(f'temp_count_{strColName}')
            ).orderBy(
                F.desc(f'temp_count_{strColName}')
            ).collect()[0][strColName]
            tblResult = tblInput.withColumn(
                strColName,
                F.when(
                    F.col(strColName).isNull(),
                    F.lit(strMode)
                ).otherwise(
                    F.col(strColName)
                )
            )
            return tblResult
        # use mean
        if isinstance(objType, DoubleType):
            strMean = tblInput.select(
                F.mean(strColName).alias(f'temp_mean_{strColName}')
            ).collect()[0][f'temp_mean_{strColName}']
            tblResult = tblInput.withColumn(
                strColName,
                F.when(
                    F.col(strColName).isNull(),
                    F.lit(strMean)
                ).otherwise(
                    F.col(strColName)
                )
            )
            return tblResult

    def encode_col(self,tblInput,strColName):
        objType = tblInput.schema[strColName].dataType
        if isinstance(objType, (StringType)):
            objIndexer = StringIndexer(inputCol=strColName, outputCol=f"{strColName}_map")
            tblResult = objIndexer.fit(tblInput).transform(tblInput) 
            tblResultMap = tblResult.groupBy(
                strColName,
                f'{strColName}_map'
            ).agg(
                F.count('*').alias('count')
            ).orderBy(
                F.asc(strColName)
            )

            tblResult = tblResult.withColumn(
                strColName,
                F.col(f"{strColName}_map")
            ).drop(
                f"{strColName}_map"
            )
            return tblResult, tblResultMap
        else:
            return tblInput, None

    def normalized_col(self, tblInput,strColName):
        pass

    def run_all(self, tblInput, strColTarget:str=None, 
                lisstrColExclude:list[str]=[], boolVerbose = False):
        lisstrColNames = [strColName for strColName in tblInput.columns if strColName not in lisstrColExclude]
        tblResult = tblInput

        
        # Step 1: Handle Disguised Null with fix_disguised_null_col()
        start_time = time.time()
        for strColName in lisstrColNames:
            tblResult = self.fix_disguised_null_col(
                tblInput=tblResult, 
                strColName=strColName
            )
        if boolVerbose:
            print(f'finished step 1 in {time.time() - start_time:.2f} seconds')
            tblResult.show()

        # Step 2. Coerce Data Type with coerce_col_type()
        start_time = time.time()
        for strColName in lisstrColNames:
            tblResult = self.coerce_col_type(
                tblInput=tblResult, 
                strColName=strColName
            )
        if boolVerbose:
            print(f'finished step 2 in {time.time() - start_time:.2f} seconds')
            tblResult.show()

        # Step 3. Impute Missing with impute_col()
        start_time = time.time()
        for strColName in lisstrColNames:
            tblResult = self.impute_col(
                tblInput=tblResult, 
                strColName=strColName
            )
        if boolVerbose:
            print(f'finished step 3 in {time.time() - start_time:.2f} seconds')
            tblResult.show()

        # Step 4. Encode Data with encode_col()
        start_time = time.time()
        for strColName in lisstrColNames:
            tblResult, tblMap = self.encode_col(
                tblInput=tblResult, 
                strColName=strColName
            )
            self.dictblLabelMap.update({strColName: tblMap})
            # Save mapping table
            if tblMap:
                strOutputPath = os.path.join(self.strPathDataLabelMap, f"{strColName}.csv")
                tblMap.toPandas().to_csv(
                    strOutputPath,
                    index=False
                )
        
        if boolVerbose:
            print(f'finished step 4 in {time.time() - start_time:.2f} seconds')
            tblResult.show()

        # Step 5. Normalize Data with normalized_col()
        self.tblResult = tblResult
        return tblResult