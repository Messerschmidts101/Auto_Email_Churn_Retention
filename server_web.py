
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
app = Flask(
    __name__,
    template_folder = 'Website',
    static_folder = os.path.join('Website','static')
)
with open(server_web_config.strPathPersonaLLM, "r", encoding="utf-8") as file:
    strTemplateContextResponse = file.read()
objGlobalModellingClass = None

objLLM = llm.LLM_Email(intLLMProvider = 1, 
    strIngestPath = server_web_config.strPathStorageLLM,
    strPromptTemplate = strTemplateContextResponse, 
    strAPIKey = server_web_config.strAPILLM, 
    fltTemperature = server_web_config.fltTemperature, 
    intRetrieverK = server_web_config.intRetrieverK,
    intLLMAccessory = server_web_config.intLLMAccessory,
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
        strPathToSaveModels = server_web_config.strPathStorageML
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

    ########################################################
    #######                                          #######
    #######      Step 3: Combine Results and PII     #######
    #######                                          #######
    ########################################################
    tblScoring = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring))[['CustomerId','Surname','Email']]
    tblScored = pd.concat([tblScoring, tblScored], axis = 1)
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
    
    ########################################################
    #######                                          #######
    #######              Step 1: Get Data            #######
    #######                                          #######
    ########################################################
    tblEmails = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScored)) 
    tblEmails = tblEmails[tblEmails['Prediction'] == 1]

    lisLLMResponses = []
    lisTop3Feats = []
    lisTop3Values = []

    ########################################################
    #######                                          #######
    #######          Step 2: Generate Email          #######
    #######                                          #######
    ########################################################

    for _, row in tblEmails.iterrows():
        # for csv output
        dicResult = objLLM.generate_email(
            row['Surname'],
            add_spaces_to_camel_case(row['Top_1_Feat']),
            row['Top_1_Feat_Value'],
            add_spaces_to_camel_case(row['Top_2_Feat']),
            row['Top_2_Feat_Value'],
            add_spaces_to_camel_case(row['Top_3_Feat']),
            row['Top_3_Feat_Value'],
        )
        lisLLMResponses.append(dicResult['Response'])

        # for web output
        lisTop3Feats.append([
            add_spaces_to_camel_case(row['Top_1_Feat']),
            add_spaces_to_camel_case(row['Top_2_Feat']),
            add_spaces_to_camel_case(row['Top_3_Feat'])
        ])
        lisTop3Values.append([
            row['Top_1_Feat_Value'],
            row['Top_2_Feat_Value'],
            row['Top_3_Feat_Value']
        ])

    ########################################################
    #######                                          #######
    #######          Step 3: Compile Results         #######
    #######                                          #######
    ########################################################

    # for csv output
    tblEmails['LLM_Response'] = lisLLMResponses
    tblEmails.to_csv(
        os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVEmails), 
        index=False
    )

    # for web output
    tblTopInfo = pd.DataFrame({
        'Top_3_Feats': lisTop3Feats,
        'Top_3_Values': lisTop3Values
    })
    tblEmails2 = pd.concat([
        tblEmails[[
            'CustomerId',
            'Surname',
            'Email',
            'Churn_Probability',
            'LLM_Response'
        ]],
        tblTopInfo
    ], axis=1)

    return jsonify(tblEmails2.to_dict(orient="records"))


# complete
@app.route('/send_emails')
def send_emails():    
    ########################################################
    #######                                          #######
    #######              Step 1: Get Data            #######
    #######                                          #######
    ########################################################
    tblEmails = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVEmails)) 

    ########################################################
    #######                                          #######
    #######         Step 2: Indivdual Sending        #######
    #######                                          #######
    ########################################################
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(
            server_web_config.strEmailUser, 
            server_web_config.strEmailPass
        )
        for intIndex, rowRow in tblEmails.iterrows():
            if (intIndex%100 == 0) and (intIndex!=0):
                print('[[send_emails()]] Email sending limit reached. Sleeping for 5 minutes...')
                time.sleep(300) 
            msg = MIMEText(rowRow['LLM_Response'])
            msg['Subject'] = server_web_config.strEmailSubject
            msg['From'] = server_web_config.strEmailFrom
            msg['To'] = rowRow['Email']
            server.send_message(msg)
            print('===== check this email sending: =====')
            print(f"To: {rowRow['Email']}")
            print(msg)
            time.sleep(1) 
    return "Finished"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
