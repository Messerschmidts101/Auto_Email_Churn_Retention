#flask

from flask import Flask, jsonify
from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np
import pickle
import shap
import os
import sys
utils_path = os.path.join(os.getcwd(), 'model')

if utils_path not in sys.path:
    sys.path.append(utils_path)
import utils

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Test App") \
    .master("local[*]") \
    .getOrCreate()

@app.route('/')
def hello():
    return 'PySpark Flask Server is running!'

@app.route('/run-model')
def run_model():
    exec(open(os.path.join(os.getcwd(), 'model', 'A_Modelling.py')).read(), globals())
    print('Model Created')
    return "Model script executed."

@app.route('/spark')
def test_spark():
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
        os.path.join(os.getcwd(), 'model','dataset','scoring.csv')
    ).drop(
        'CustomerId'
    ).limit(
        5
    ).toPandas()
    X,y = tblScoring[[strColName for strColName in tblScoring.columns if strColName != 'Exited']], tblScoring['Exited']

    ########################################################
    #######                                          #######
    #######            Step 2: Load Model            #######
    #######                                          #######
    ########################################################
    with open(os.path.join(os.getcwd(),'Churn_Pred_Model_With_SHAP.pkl'), 'rb') as f:
        objPipeline = pickle.load(f)

    ########################################################
    #######                                          #######
    #######              Step 3: Predict             #######
    #######                                          #######
    ########################################################
    tblPredictions = objPipeline.transform(tblScoring).to_dict(orient='records')
    print('Check predictions here')
    print(tblPredictions)

    return jsonify(tblPredictions)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
