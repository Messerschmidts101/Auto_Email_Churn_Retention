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
    file = request.files['file']
    file.save(os.path.join(os.getcwd(),'model', 'dataset', 'train.csv'))
    df = pd.read_csv(os.path.join(os.getcwd(),"model", "dataset", "train.csv"))
    return jsonify(df.to_dict(orient="records"))

# complete
@app.route('/upload_scoring', methods=['POST'])
def upload_scoring():
    file = request.files['file']
    file.save(os.path.join(os.getcwd(),'model', 'dataset', 'scoring.csv'))
    df = pd.read_csv(os.path.join(os.getcwd(),"model", "dataset", "scoring.csv"))
    return jsonify(df.to_dict(orient="records"))


# complete but can be improved
@app.route('/train_model')
def train_model():
    #exec(open(os.path.join(os.getcwd(), 'model', 'A_Modelling.py')).read(), globals())
    global objGlobalModellingClass
    objModellingClass = Modelling_Class.Modelling_Class(
        strPathTrainDataset = os.path.join(os.getcwd(),'model', 'dataset', 'train.csv')
    )
    objModellingClass.run_training()
    objGlobalModellingClass = objModellingClass

    with open('test_objModellingClass.pkl', 'wb') as f:
        pickle.dump(objModellingClass, f) 

    # Collect metrics
    metrics = {
        "Accuracy": [objModellingClass.fltAccuracy],
        "Precision": [objModellingClass.fltPrecision],
        "Recall": [objModellingClass.fltRecall]
    }

    # Convert confusion matrix to separate fields
    cm = objModellingClass.objConfusionMatrix
    metrics["True Negative"] = [cm[0][0]]
    metrics["False Positive"] = [cm[0][1]]
    metrics["False Negative"] = [cm[1][0]]
    metrics["True Positive"] = [cm[1][1]]

    # Create DataFrame
    dfMetrics = pd.DataFrame(metrics)

    # Optional: jsonify for Flask
    return jsonify(dfMetrics.to_dict(orient="records"))


@app.route('/get_model')
def get_model():
    global objGlobalModellingClass
    strPathModelSHAP = objGlobalModellingClass.strPathModelSHAP
    with open(os.path.join(os.getcwd(), strPathModelSHAP), 'rb') as f:
        objPipeline = pickle.load(f)
    objPipeline

@app.route('/run_inference')
def get_prediction():
    # TODO: FIX ISSUE OF TRIGGERING TRANSFORM AND FIT DESPITE ONLY CALLING TRANSFORM()
    global objGlobalModellingClass
    objPipeline = objGlobalModellingClass

    # Load the saved model object if no model is trained during session or SHAP path is missing
    if not objPipeline or not objPipeline.strPathModelSHAP:
        with open(os.path.join(os.getcwd(), 'test_objModellingClass.pkl'), 'rb') as f:
            objPipeline = pickle.load(f)

    # Run predictions on the scoring dataset
    scoring_path = os.path.join(os.getcwd(), "model", "dataset", "scoring.csv")
    tblScored = objPipeline.get_predictions(strPathScoring=scoring_path)

    print('Check scoring here:')
    tblScored.show()

    return jsonify(tblScored.to_dict(orient="records"))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
