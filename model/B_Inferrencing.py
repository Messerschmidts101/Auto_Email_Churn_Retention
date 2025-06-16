import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"
# Load the pipeline (if saved via pickle)

from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np
import pickle
import shap

objSpark = SparkSession.builder.getOrCreate()

########################################################
#######                                          #######
#######         Step 1: Load Data Scoring        #######
#######                                          #######
########################################################
tblScoring = objSpark.read.option(
    "header", 
    True
).csv(
    os.path.join('model','dataset','scoring.csv')
).drop(
    'CustomerId'
).toPandas()
X,y = tblScoring[[strColName for strColName in tblScoring.columns if strColName != 'Exited']], tblScoring['Exited']

########################################################
#######                                          #######
#######            Step 2: Load Model            #######
#######                                          #######
########################################################
with open('Churn_Pred_Model_With_SHAP.pkl', 'rb') as f:
    objPipeline = pickle.load(f)

########################################################
#######                                          #######
#######              Step 3: Predict             #######
#######                                          #######
########################################################
tblPredictions = objPipeline.transform(tblScoring)
print('Check predictions here')
print(tblPredictions)

print(tblPredictions.columns.tolist())
tblPredictions.to_csv('output.csv', index=False)