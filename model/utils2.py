import os
from pyspark.ml import Estimator, Model, Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable, MLReadable, MLWritable, MLReader, MLWriter
from pyspark.ml.param.shared import Param, Params
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DateType, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
import pyspark.sql.window as W

# fix_disguised_null_col
# coerce_col_type
# impute_col
# encode_col
# normalized_col
# predict
objSpark = SparkSession.builder.getOrCreate()
lisobjBaseEstimator = [Estimator, DefaultParamsReadable, DefaultParamsWritable, MLWritable, MLReadable]
lisobjBaseTransformer = [Transformer, DefaultParamsReadable, DefaultParamsWritable]
lisobjBaseModel = [Model, MLWritable, MLReadable]

########################################################
#######                                          #######
#######      Step 1: fix_disguised_null_col      #######
#######                                          #######
########################################################

class Disguised_Nulls_Transformer(*lisobjBaseTransformer):
    def __init__(self, strInputColName:str,lisstrDisguisedNulls:list[str]=['_','',' ']):
        super().__init__()
        self.strInputColName = strInputColName
        self.lisstrDisguisedNulls = lisstrDisguisedNulls

    def _transform(self, tblInputData:DataFrame):
        tblResult = tblInputData.withColumn(
            self.strInputColName,
            F.when(
                F.col(self.strInputColName).isin(self.lisstrDisguisedNulls),
                F.lit(None)
            ).otherwise(
                F.col(self.strInputColName)
            )
        )
        return tblResult
    
########################################################
#######                                          #######
#######         Step 2: coerce_col_type          #######
#######                                          #######
########################################################

class Coerce_Type_Transformer(*lisobjBaseTransformer):
    def __init__(self, strInputColName:str):
        super().__init__()
        self.strInputColName = strInputColName

    def _transform(self, tblInputData:DataFrame):
        intBefore = tblInputData.filter(
            F.col(self.strInputColName).isNotNull()
        ).count()
        intAfter = tblInputData.filter(
            F.col(self.strInputColName).cast("int").isNotNull()
        ).count()

        if intBefore > intAfter:
            return tblInputData
        else: 
            return tblInputData.withColumn(
                self.strInputColName,
                F.col(self.strInputColName).cast('double')
            )
        
########################################################
#######                                          #######
#######           Step 3: impute_col             #######
#######                                          #######
########################################################

class Imputer_Estimator(*lisobjBaseEstimator):
    def __init__(self, strInputColName: str):
        super().__init__()
        self.strInputColName = strInputColName
        self.anyImputeValue = None

    def _fit(self, tblInputData: DataFrame):
        objType = tblInputData.schema[self.strInputColName].dataType

        if isinstance(objType, StringType):
            self.anyImputeValue = tblInputData.filter(
                F.col(self.strInputColName).isNotNull()
            ).groupBy(
                self.strInputColName
            ).agg(
                F.count("*").alias(f"temp_count_{self.strInputColName}")
            ).orderBy(
                F.desc(f"temp_count_{self.strInputColName}")
            ).first()[self.strInputColName]

        if isinstance(objType, DoubleType):
            self.anyImputeValue = tblInputData.filter(
                F.col(self.strInputColName).isNotNull()
            ).agg(
                F.mean(self.strInputColName).alias(f"temp_mean_{self.strInputColName}")
            ).first()[f"temp_mean_{self.strInputColName}"]

        return Imputer_Model(self.anyImputeValue, self.strInputColName)

class Imputer_Model(*lisobjBaseModel):
    def __init__(self, anyImputeValue: any, strInputColName: str):
        super().__init__()
        self.anyImputeValue = anyImputeValue
        self.strInputColName = strInputColName

    def _transform(self, tblInputData: DataFrame):
        return tblInputData.na.fill({self.strInputColName: self.anyImputeValue})
    
    def write(self):
        print('check here starting of write...')
        return Impute_Model_Writer(self)

    @classmethod
    def read(cls):
        return Impute_Model_Loader()

    @classmethod
    def load(cls, path):
        return cls.read().load(path)
    
class Impute_Model_Writer(MLWriter):
    def __init__(self, objInstance):
        super().__init__()
        self.objInstance = objInstance
 
    '''def saveImpl(self, path):
        # Save mean or mode
        strPathImputeValue = os.path.join(path, "impute_value.txt")
        objSpark.sparkContext.parallelize(
            self.objInstance.anyImputeValue
        ).saveAsTextFile(
            strPathImputeValue
        )
        # Save metadata
        dicMetaData = {
            "strInputColName": self.objInstance.strInputColName
        }
        self._saveMetadata(self.objInstance, path, dicMetaData)'''

    def saveImpl(self, path):
        # Save mean or mode
        print(f'check this: path {path}')
        strPathImputeValue = os.path.join(path, "impute_value.txt")
        print(f'check this: write {self.objInstance.anyImputeValue}')
        with open(strPathImputeValue, "w") as f:
            f.write( self.objInstance.anyImputeValue)
        # Save metadata
        dicMetaData = {
            "strInputColName": self.objInstance.strInputColName
        }
        print('check this: meta')
        self._saveMetadata(self.objInstance, path, dicMetaData)
        print('check this: success')

class Impute_Model_Loader(MLReader):
    '''def load(self, path):
        dicMetaData = self._loadMetadata(path)
        strPathImputeValue = os.path.join(path, "impute_value.txt")
        anyImputeValue = objSpark.read.text(os.path.join(strPathImputeValue, "impute_value.txt")).collect()[0][0]
        return Imputer_Model(
            anyImputeValue = anyImputeValue, 
            strInputColName = dicMetaData['strInputColName']
        )'''
    
    def load(self, path):
        dicMetaData = self._loadMetadata(path)
        strPathImputeValue = os.path.join(path, "impute_value.txt")
        with open(strPathImputeValue, "r") as f:
            anyImputeValue = f.read() # float(f.read())
        return Imputer_Model(
            anyImputeValue = anyImputeValue, 
            strInputColName = dicMetaData['strInputColName']
        )
    