
########################################################
#######                                          #######
#######            Import Environment            #######
#######                                          #######
########################################################
import os
from dotenv import load_dotenv
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"
load_dotenv()
strAPIKey = os.getenv("GROQ_API_KEY")

########################################################
#######                                          #######
#######             Import Libraries             #######
#######                                          #######
########################################################
from flask import Flask, jsonify, render_template, request
import pandas as pd
import pickle
import sys
import joblib
import re
import time
import smtplib
from email.mime.text import MIMEText

########################################################
#######                                          #######
#######         Import Custom Libraries          #######
#######                                          #######
########################################################
utils_path = os.path.join(os.getcwd(), 'model')
if utils_path not in sys.path:
    sys.path.append(utils_path)
import server_web_config
import utils
import Modelling_Class
import llm.llm_class as llm


########################################################
#######                                          #######
#######             Server Constants             #######
#######                                          #######
########################################################
app = Flask(__name__,template_folder='Website',static_folder='Website/static')
with open(server_web_config.strPathPersonaLLM, "r", encoding="utf-8") as file:
    strTemplateContextResponse = file.read()
objGlobalModellingClass = None
objLLM = llm.LLM_Email(intLLMProvider = 1, 
    strIngestPath = server_web_config.strPathStorageLLM,
    strPromptTemplate = strTemplateContextResponse, 
    strAPIKey = strAPIKey, 
    fltTemperature = 0.1, 
    intRetrieverK = 5,
    intLLMAccessory = 4,
)

# complete 
@app.route('/')
def hello():
    return render_template('index.html')

# complete
@app.route('/upload_train', methods=['POST'])
def upload_train():
    objFile = request.files['file']
    objFile.save(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVTrain)) 
    tblTraining = pd.read_csv(os.path.join(server_web_config.strPathStorageML,server_web_config.strNameCSVTrain)) 
    return jsonify(tblTraining.to_dict(orient="records"))

# complete
@app.route('/upload_scoring', methods=['POST'])
def upload_scoring():
    objFile = request.files['file']
    objFile.save(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring)) 
    tblScoring = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring)) 
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
        strPathTrainDataset = os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVTrain),

    )
    timeStart = time.time()
    objModellingClass.run_training()
    joblib.dump(objModellingClass, os.path.join(server_web_config.strPathStorageML, server_web_config.strNameMLFinal)) 
    objGlobalModellingClass = objModellingClass # update available model server side

    ########################################################
    #######                                          #######
    #######          Step 2: Collect Metrics         #######
    #######                                          #######
    ########################################################
    
    dicSamples = {
        "Number of Positive Class In Training": [objModellingClass.intCountTrainPositiveClass],
        "Number of Negative Class In Training": [objModellingClass.intCountTrainNegativeClass],
        "Number of Positive Class In Testing": [objModellingClass.intCountTestPositiveClass],
        "Number of Negative Class In Testing": [objModellingClass.intCountTestNegativeClass],
    }
    dicMetrics = {
        "Accuracy": [objModellingClass.fltAccuracy],
        "Precision": [objModellingClass.fltPrecision],
        "Recall": [objModellingClass.fltRecall],
        "F1": [objModellingClass.fltF1]
    }
    cm = objModellingClass.objConfusionMatrix
    dicConfusionMatrix = {
        "True Negative": [cm[0][0]],
        "False Positive": [cm[0][1]],
        "False Negative": [cm[1][0]],
        "True Positive": [cm[1][1]],
    }

    tblMetrics = pd.DataFrame(dicMetrics)
    tblSamples = pd.DataFrame(dicSamples)
    tblConfusionMatrix = pd.DataFrame(dicConfusionMatrix)
    
    timeEnd = time.time()
    return jsonify({
        "samples": tblSamples.to_dict(orient="records"),
        "metrics": tblMetrics.to_dict(orient="records"),
        "confusion_matrix": tblConfusionMatrix.to_dict(orient="records"),
        'time': timeEnd-timeStart
    })

# complete
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
        objPipeline = joblib.load(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameMLFinal)) 


    ########################################################
    #######                                          #######
    #######           Step 2: Run Inference          #######
    #######                                          #######
    ########################################################
    strPathScoring = os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring)
    timeStart = time.time()
    tblScored = objPipeline.get_predictions(strPathScoring=strPathScoring)
    tblScored.to_csv(
        os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScored), 
        index = False
    )
    timeEnd = time.time()
    print(f'Time taken: {timeEnd-timeStart}')
    return jsonify(tblScored.to_dict(orient="records"))

# complete
@app.route('/create_emails')
def create_emails():
    def add_spaces_to_camel_case(name):
        return re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
    # Step 1: open scored table
    tblScored = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScored)) 
    tblScored = tblScored[tblScored['Prediction'] == 0]
    lisEmailsGenerate = []
    # Step 2: pass each item as argument to llm
    for intIndex, rowRow in tblScored.iterrows():
        dicResult = objLLM.generate_email(
            'John Doe',
            add_spaces_to_camel_case(rowRow['Top_1_Feat']),
            rowRow['Top_1_Feat_Value'],
            add_spaces_to_camel_case(rowRow['Top_2_Feat']),
            rowRow['Top_2_Feat_Value'],
            add_spaces_to_camel_case(rowRow['Top_3_Feat']),
            rowRow['Top_3_Feat_Value'],
        )
        print(f'Result of Email:\n{dicResult}')
        lisEmailsGenerate.append(dicResult['Response'])

    # Step 3: compile result as table
    tblScored['LLM_Response'] = lisEmailsGenerate
    # Step 4: return table
    return jsonify(tblScored.to_dict(orient="records"))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)


'''
# complete
@app.route('/send_emails')
def send_emails():
    msg = MIMEText("Hello from Python!")
    msg["Subject"] = "Test Email"
    msg["From"] = "you@gmail.com"
    msg["To"] = "recipient@example.com"

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login("you@gmail.com", "your_app_password")
        server.send_message(msg)'''