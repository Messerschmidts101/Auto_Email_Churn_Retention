
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
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import sys
import joblib
import re
import time
import smtplib
from email.mime.text import MIMEText
from datetime import date
from uuid import uuid4

########################################################
#######                                          #######
#######         Import Custom Libraries          #######
#######                                          #######
########################################################
utils_path = os.path.join(os.getcwd(), 'model')
if utils_path not in sys.path:
    sys.path.append(utils_path)
from server_database import db, Latest_Training, Latest_Scoring, Latest_Scored, Latest_Emails, Historical_Models, Historical_Training, Historical_Scoring, Historical_Scored, Historical_Emails
import utils_models
import utils_server
import Modelling_Class
import server_web_config

########################################################
#######                                          #######
#######             Server Constants             #######
#######                                          #######
########################################################

# Create Server
app = Flask(
    __name__,
    template_folder = 'Website',
    static_folder = os.path.join('Website','static')
)

# Add Database To Server
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
with app.app_context():
    db.create_all()

# Add LLM To Server
objLLM = utils_server.create_llm()








################################################################################################################

                # Since we are upgrading to sql storage instead of csv storage,
                # We must replace our reliance on read and write with csv, to sql.

                




# complete 
@app.route('/')
def hello():
    return render_template('index.html')

# complete
@app.route('/upload_train', methods=['POST'])
def upload_train():
    # Step 1: Write and Read locally
    objFile = request.files['file']
    objFile.save(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVTrain)) 
    tblLatestTraining = pd.read_csv(
        os.path.join(server_web_config.strPathStorageML,server_web_config.strNameCSVTrain)
    )

    # Step 2: Overwrite on database latest table
    db.session.query(Latest_Training).delete()
    db.session.commit()
    db.session.bulk_insert_mappings(Latest_Training, tblLatestTraining.to_dict(orient="records"))
    db.session.commit()

    # Step 3: Append on database historical table
    dtNow = date.today()
    tblLatestTraining['meta_DateCreated'] = dtNow
    tblLatestTraining['meta_Id'] = [str(dtNow) + '_' + create_random_string() for _ in range(len(tblLatestTraining))]
    db.session.bulk_insert_mappings(Historical_Training, tblLatestTraining.to_dict(orient="records"))
    db.session.commit()

    # Step 4: Return latest table for view
    return jsonify(tblLatestTraining.drop(['meta_DateCreated','meta_Id'], axis=1, errors='ignore').to_dict(orient="records"))

# complete
@app.route('/upload_scoring', methods=['POST'])
def upload_scoring():
    # Step 1: Write and Read locally
    objFile = request.files['file']
    objFile.save(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring)) 
    tblLatestScoring = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring)) 
    
    # Step 2: Overwrite on database latest table
    db.session.query(Latest_Scoring).delete()
    db.session.commit()
    db.session.bulk_insert_mappings(Latest_Scoring, tblLatestScoring.to_dict(orient="records"))
    db.session.commit()

    # Step 3: Append on database historical table
    dtNow = date.today()
    tblLatestScoring['meta_DateCreated'] = dtNow
    tblLatestScoring['meta_Id'] = [str(dtNow) + '_' + create_random_string() for _ in range(len(tblLatestScoring))]
    db.session.bulk_insert_mappings(Historical_Scoring, tblLatestScoring.to_dict(orient="records"))
    db.session.commit()

    # Step 4: Return latest table for view
    return jsonify(tblLatestScoring.drop(['meta_DateCreated','meta_Id'], axis=1, errors='ignore').to_dict(orient="records"))

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

    ########################################################
    #######                                          #######
    #######            Step 3: Record Stats          #######
    #######                                          #######
    ########################################################
    dtNow = date.today()
    rowModel = Historical_Models(
        meta_Id = dtNow,
        meta_DateCreated = str(dtNow) + '_' + create_random_string(),
        Accuracy = objModellingClass.fltAccuracy,
        Precision = objModellingClass.fltPrecision,
        Recall = objModellingClass.fltRecall,
        F1 = objModellingClass.fltF1,
        CountTrueNegative = cm[0][0],
        CountFalsePositive = cm[0][1],
        CountFalseNegative = cm[1][0],
        CountTruePositiove = cm[1][1],
        CountTrainingPositiveClass = objModellingClass.intCountTrainPositiveClass,
        CountTrainingNegativeClass = objModellingClass.intCountTrainNegativeClass,
        CountTestPositiveClass = objModellingClass.intCountTestPositiveClass,
        CountTestNegativeClass = objModellingClass.intCountTestNegativeClass
    )
    db.session.add(rowModel)
    db.session.commit()

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
    # Step 1: Write and Read Locally
    tblScoring = pd.read_csv(os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScoring))[['CustomerId','Surname','Email']]
    tblScored = pd.concat([tblScoring, tblScored], axis = 1)
    tblScored.to_csv(
        os.path.join(server_web_config.strPathStorageML, server_web_config.strNameCSVScored), 
        index = False
    )
    timeEnd = time.time()
    print(f'Time taken: {timeEnd-timeStart}')

    # Step 2: Overwrite on database latest table
    db.session.query(Latest_Scored).delete()
    db.session.commit()
    db.session.bulk_insert_mappings(Latest_Scored, tblScored.to_dict(orient="records"))
    db.session.commit()

    # Step 3: Append on database historical table
    dtNow = date.today()
    tblScored['meta_DateCreated'] = dtNow
    tblScored['meta_Id'] = [str(dtNow) + '_' + create_random_string() for _ in range(len(tblScored))]
    db.session.bulk_insert_mappings(Historical_Scored, tblScored.to_dict(orient="records"))
    db.session.commit()

    return jsonify(tblScored.drop(['meta_DateCreated','meta_Id'], axis=1, errors='ignore').to_dict(orient="records"))

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

    ########################################################
    #######                                          #######
    #######          Step 4: Save To Database        #######
    #######                                          #######
    ########################################################
    # Step 1: Overwrite on database latest table
    db.session.query(Latest_Emails).delete()
    db.session.commit()
    db.session.bulk_insert_mappings(Latest_Emails, tblEmails2.to_dict(orient="records"))
    db.session.commit()

    # Step 2: Append on database historical table
    dtNow = date.today()
    tblEmails2['meta_DateCreated'] = dtNow
    tblEmails2['meta_Id'] = [str(dtNow) + '_' + create_random_string() for _ in range(len(tblEmails2))]
    db.session.bulk_insert_mappings(Historical_Emails, tblEmails2.to_dict(orient="records"))
    db.session.commit()

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

@app.route('/view_results',methods=['POST'])
def view_tables():
    data = request.get_json()
    strTableName = data.get('strTableVersion') + '__' + data.get('strTableName')
    with db.engine.connect() as conn:
        sql = text(f"SELECT * FROM \"{strTableName}\"")
        result = conn.execute(sql)
        tblResult = pd.DataFrame(result.mappings().all())
    return jsonify({
        "records": tblResult.to_dict(orient='records'),
        "row_count": len(tblResult)
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
