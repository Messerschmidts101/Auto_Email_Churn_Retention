import os
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np
import pickle
import shap
import os
import sys
import requests
import Modelling_Class
import joblib

'''objSpark = SparkSession.builder.getOrCreate()
objPipeline = joblib.load('New_Churn_Pred_Model_With_SHAP.pkl')
print(type(objPipeline.named_steps))
for key,value in objPipeline.named_steps['Preprocessor'].named_steps.items():
    print(f'{key} : {value}')
'''
########################################################
#######                                          #######
#######           Step 1: Train and Dump         #######
#######                                          #######
########################################################
objModellingClass = Modelling_Class.Modelling_Class(
        strPathTrainDataset = os.path.join(os.getcwd(),'model', 'dataset', 'train.csv')
    )
objModellingClass.run_training( 
    boolVerbose = True 
)
joblib.dump(objModellingClass,'churn_prediction_model.pkl')

########################################################
#######                                          #######
#######           Step 2: Test and Dump          #######
#######                                          #######
########################################################
objPipeline = joblib.load('churn_prediction_model.pkl')

tblScored = objPipeline.get_predictions(
    strPathScoring = os.path.join(os.getcwd(), "model", "dataset", "scoring.csv"),
    strPathSavePredictions = 'output.csv',
    boolVerbose = False,
)
print('Check scoring here:')
print(tblScored)
