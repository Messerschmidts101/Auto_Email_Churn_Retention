#python model\Z_Test_Data_Prep.py
import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

from pyspark import SparkConf
import pyspark.sql.functions as F
import pyspark.sql.window as W
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType,BooleanType
from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import utils
import pickle

conf = SparkConf()
conf.set("spark.hadoop.io.native.lib.available", "false")
objSpark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()
# Define schema
schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Balance", FloatType(), True),
    StructField("Churn", BooleanType(), True)
])

# Sample data with missing values
data = [
    (1, "Alice", 30, "Female", 1000.0,True),
    (2, "Bob", None, "Male", 1500.0,True),
    (3, "_", 40, None, None,True),
    (4, "Mike", 35, "Male", 800.0,True),
    (5, "Eve", None, "Female", 1400.0,False)
]

# Create DataFrame
df = objSpark.createDataFrame(data, schema=schema).toPandas()
X = df[[strColName for strColName in df.columns if strColName != 'Churn']]
X_train, X_test, y_train, y_test = train_test_split(X, df['Churn'], test_size=0.2, random_state=42)

# Create Pipeline
col_names = X.columns.tolist()
pipeline = Pipeline([
    ('Diguised_Nulls', utils.Disguised_Nulls_Transformer(col_names, boolVerbose=True)),
    ('Coerce_Type', utils.Coerce_Type_Transformer(col_names, boolVerbose=True)),
    ('Imputer', utils.Imputer_Transformer(col_names, boolVerbose=True)),
    ('Encoder', utils.Encoder_Transformer(col_names, boolVerbose=True)),
])

# Fit Pipeline
pipeline.fit(df, y_train)

# Save the pipeline with pickle
with open('temp_pipeline_preprocess.pkl', 'wb') as f:
    pickle.dump(pipeline, f)

# Later or elsewhere: load the pipeline
with open('temp_pipeline_preprocess.pkl', 'rb') as f:
    loaded_pipeline = pickle.load(f)

print('finished saving')
# Use loaded pipeline to predict on test data
df = loaded_pipeline.transform(df)
df = objSpark.createDataFrame(df)
df.show()