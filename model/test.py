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

objSpark = SparkSession.builder.getOrCreate()

########################################################
#######                                          #######
#######           Step 1: Train and Dump         #######
#######                                          #######
########################################################
objModellingClass = Modelling_Class.Modelling_Class(
        strPathTrainDataset = os.path.join(os.getcwd(),'model', 'dataset', 'train.csv')
    )
objModellingClass.run_training()

with open('churn_prediction_model.pkl', 'wb') as f:
    pickle.dump(objModellingClass, f) 


########################################################
#######                                          #######
#######           Step 2: Test and Dump          #######
#######                                          #######
########################################################
with open('churn_prediction_model.pkl', 'rb') as f:
    objPipeline = pickle.load(f)
scoring_path = os.path.join(os.getcwd(), "model", "dataset", "scoring.csv")

tblScored = objPipeline.get_predictions(strPathScoring=scoring_path)
print('Check scoring here:')
print(tblScored)
