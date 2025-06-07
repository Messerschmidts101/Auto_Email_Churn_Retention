import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

import pyspark.sql.functions as F
import pyspark.sql.window as W
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
import utils2

objSpark = SparkSession.builder.getOrCreate()

# Define schema
schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Balance", FloatType(), True)
])

# Sample data with missing values
data = [
    (1, "Alice", 30, "Female", 1000.0),
    (2, "Bob", None, "Male", 1500.0),
    (3, None, 40, None, None),
    (4, "Mike", 35, "Male", 800.0),
    (5, "Eve", None, "Female", 1400.0)
]

# Create DataFrame
df = objSpark.createDataFrame(data, schema=schema)

# Create Pipeline
objPipeline = Pipeline(
    stages=[
        utils2.Disguised_Nulls_Transformer, 
        utils2.Coerce_Type_Transformer, 
        utils2.Imputer_Estimator # yes not Imputer_Model as estimator will return Imputer_Model
    ]
)

# Fit Pipeline
objModel = objPipeline.fit(df)

# Transform Pipeline
df = objModel.transform(df)

# Show
df.show()
