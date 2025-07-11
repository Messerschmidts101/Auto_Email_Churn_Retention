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
import joblib

utils_path = os.path.join(os.getcwd(), 'model')
if utils_path not in sys.path:
    sys.path.append(utils_path)
import utils
import Modelling_Class

app = Flask(__name__,template_folder='Website',static_folder='Website/static')
objGlobalModellingClass = None


# complete 
@app.route('/')
def hello():
    return render_template('index.html')

# complete
@app.route('/upload_train', methods=['POST'])
def upload_train():
    objFile = request.files['file']
    objFile.save(os.path.join(os.getcwd(),'model', 'dataset', 'train.csv'))
    tblTraining = pd.read_csv(os.path.join(os.getcwd(),"model", "dataset", "train.csv"))
    return jsonify(tblTraining.to_dict(orient="records"))

# complete
@app.route('/upload_scoring', methods=['POST'])
def upload_scoring():
    objFile = request.files['file']
    objFile.save(os.path.join(os.getcwd(),'model', 'dataset', 'scoring.csv'))
    tblScoring = pd.read_csv(os.path.join(os.getcwd(),"model", "dataset", "scoring.csv"))
    return jsonify(tblScoring.to_dict(orient="records"))

# complete
@app.route('/train_model')
def train_model():
    ########################################################
    #######                                          #######
    #######           Step 1: Train and Dump         #######
    #######                                          #######
    ########################################################
    global objGlobalModellingClass
    objModellingClass = Modelling_Class.Modelling_Class(
        strPathTrainDataset = os.path.join(os.getcwd(),'model', 'dataset', 'train.csv')
    )
    objModellingClass.run_training()
    joblib.dump(objModellingClass,'churn_prediction_model.pkl')
    objGlobalModellingClass = objModellingClass # update available model server side

    ########################################################
    #######                                          #######
    #######          Step 2: Collect Metrics         #######
    #######                                          #######
    ########################################################
    dicMetrics = {
        "Accuracy": [objModellingClass.fltAccuracy],
        "Precision": [objModellingClass.fltPrecision],
        "Recall": [objModellingClass.fltRecall],
        "Number of Positive Class In Training": [objModellingClass.intCountTrainPositiveClass],
        "Number of Negative Class In Training": [objModellingClass.intCountTrainNegativeClass],
        "Number of Positive Class In Testing": [objModellingClass.intCountTestPositiveClass],
        "Number of Negative Class In Testing": [objModellingClass.intCountTestNegativeClass],
    }

    cm = objModellingClass.objConfusionMatrix
    dicMetrics["True Negative"] = [cm[0][0]]
    dicMetrics["False Positive"] = [cm[0][1]]
    dicMetrics["False Negative"] = [cm[1][0]]
    dicMetrics["True Positive"] = [cm[1][1]]

    tblMetrics = pd.DataFrame(dicMetrics)
    return jsonify(tblMetrics.to_dict(orient="records"))

# not being used, unsure of this api purpose
@app.route('/get_model')
def get_model():
    global objGlobalModellingClass
    strPathModelSHAP = objGlobalModellingClass.strPathModelSHAP
    with open(os.path.join(os.getcwd(), strPathModelSHAP), 'rb') as f:
        objPipeline = pickle.load(f)
    objPipeline

@app.route('/run_inference')
def get_prediction():
    
    ########################################################
    #######                                          #######
    #######             Step 1: Get Model            #######
    #######                                          #######
    ########################################################
    global objGlobalModellingClass
    objPipeline = objGlobalModellingClass
    # Load the saved model object if no model is trained during session or SHAP path is missing
    if not objPipeline:
        objPipeline = joblib.load('churn_prediction_model.pkl')

    ########################################################
    #######                                          #######
    #######           Step 2: Run Inference          #######
    #######                                          #######
    ########################################################
    strPathScoring = os.path.join(os.getcwd(), "model", "dataset", "scoring.csv")
    tblScored = objPipeline.get_predictions(strPathScoring=strPathScoring)

    return jsonify(tblScored.to_dict(orient="records"))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
