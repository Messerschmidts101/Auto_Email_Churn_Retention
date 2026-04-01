
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
import uuid

########################################################
#######                                          #######
#######         Import Custom Libraries          #######
#######                                          #######
########################################################
utils_path = os.path.join(os.getcwd(), 'model')
if utils_path not in sys.path:
    sys.path.append(utils_path)
from app.db.schema import db, Latest_Training, Latest_Scoring, Latest_Scored, Latest_Emails, Historical_Models, Historical_Training, Historical_Scoring, Historical_Scored, Historical_Emails
import ChurnPredictionModel
import utils_server
import app.config as config

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

# Add Model To Server
try:
    pass
    objGlobalModellingClass = joblib.load(os.path.join(config.strPathStorageML, config.strNameMLFinal))
except:
    objGlobalModellingClass = None
    pass

# complete 
@app.route('/')
def hello():
    return render_template('index.html')

@app.route('/upload_train', methods=['POST'])
def upload_train():
    # Step 1: Get dateset
    # we have to unforunately save still as csv as there is no way the modelling class can ingest a db file
    objFile = request.files['file']
    tblLatestTraining = pd.read_csv(objFile)
    tblLatestTraining.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVTrain),
        index=False
    )
    # Step 2: Overwrite on database latest table
    Latest_Training.overwrite_self(tblLatestTraining)
    # Step 3: Append on database historical table
    Latest_Training.append_historical(tblLatestTraining, Historical_Training)
    # Step 4: Return latest table for view
    return jsonify(Latest_Training.to_json())

# complete
@app.route('/upload_scoring', methods=['POST'])
def upload_scoring():
    # Step 1: Write and Read locally
    # we have to unforunately save still as csv as there is no way the modelling class can ingest a db file
    objFile = request.files['file']
    tblLatestScoring = pd.read_csv(objFile)
    tblLatestScoring.to_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVScoring),
        index=False
    )
    # Step 2: Overwrite on database latest table
    Latest_Scoring.overwrite_self(tblLatestScoring)
    # Step 3: Append on database historical table
    Latest_Scoring.append_historical(tblLatestScoring, Historical_Scoring)
    # Step 4: Return latest table for view
    return jsonify(Latest_Scoring.to_json())

# complete
@app.route('/train_model')
def train_model():
    # NOW TAKES ONLY 751.6286749839783 seconds
    # 0.8709 ACCURACY
    # 0.6149 F1
    # 0.7715 PRECISION
    # 0.5111 RECALL
    ########################################################
    #######                                          #######
    #######           Step 1: Train and Dump         #######
    #######                                          #######
    ########################################################

    global objGlobalModellingClass
    objModellingClass = ChurnPredictionModel.ChurnPredictionModel(
        strPathTrainDataset = os.path.join(config.strPathStorageML, config.strNameCSVTrain),
        strPathToSaveModels = config.strPathStorageML
    )
    timeStart = time.time()
    objModellingClass.run_training(boolVerbose = False)
    joblib.dump(
        objModellingClass, 
        os.path.join(config.strPathStorageML, config.strNameMLFinal)
    ) 
    objGlobalModellingClass = objModellingClass # update available model to server side

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
        meta_Id =  f"{dtNow}_{uuid.uuid4()}" ,
        meta_DateCreated = dtNow,
        Accuracy = objModellingClass.fltAccuracy,
        Precision = objModellingClass.fltPrecision,
        Recall = objModellingClass.fltRecall,
        F1 = objModellingClass.fltF1,
        CountTrueNegative = cm[0][0],
        CountFalsePositive = cm[0][1],
        CountFalseNegative = cm[1][0],
        CountTruePositive = cm[1][1],
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
        objPipeline = joblib.load(os.path.join(config.strPathStorageML, config.strNameMLFinal)) 
    
    ########################################################
    #######                                          #######
    #######           Step 2: Run Inference          #######
    #######                                          #######
    ########################################################
    timeStart = time.time()
    tblLatestScored = objPipeline.get_predictions(strPathScoring = os.path.join(config.strPathStorageML, config.strNameCSVScoring))

    ########################################################
    #######                                          #######
    #######      Step 3: Combine Results and PII     #######
    #######                                          #######
    ########################################################
    # Step 3.1. Get only customer data
    tblScoringCustomerDetails = pd.read_sql(f"SELECT * FROM {Latest_Scoring.__tablename__}", db.engine)[['CustomerId','Surname','Email']]
    # Step 3.2. Combine customer data to master
    tblLatestScored = pd.concat([tblScoringCustomerDetails, tblLatestScored], axis = 1)
    timeEnd = time.time()
    print(f'Time taken: {timeEnd-timeStart}')

    ########################################################
    #######                                          #######
    #######        Step 4: Store Results to DB       #######
    #######                                          #######
    ########################################################
    # Step 4.1: Overwrite on database latest table
    Latest_Scored.overwrite_self(tblLatestScored)
    # Step 4.2: Append on database historical table
    Latest_Scored.append_historical(tblLatestScored, Historical_Scored)
    # Step 4.3: Return latest table for view
    return jsonify(Latest_Scored.to_json())

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
    tblLatestScored = pd.read_sql(f"SELECT * FROM {Latest_Scored.__tablename__}", db.engine)
    tblLatestScored = tblLatestScored[tblLatestScored['Prediction'] == 1]

    lisLLMResponses = []

    ########################################################
    #######                                          #######
    #######          Step 2: Generate Email          #######
    #######                                          #######
    ########################################################

    for _, row in tblLatestScored.iterrows():
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

    ########################################################
    #######                                          #######
    #######          Step 3: Compile Results         #######
    #######                                          #######
    ########################################################
    # for csv output
    tblLatestScored['LLM_Response'] = lisLLMResponses
    tblLatestScored = tblLatestScored[[
        strColName for strColName in tblLatestScored.columns 
        if strColName not in [
            'Top_4_Feat','Top_4_Feat_Value','Top_4_Feat_Score',
            'Top_5_Feat','Top_5_Feat_Value','Top_5_Feat_Score'
        ]
    ]].reset_index(drop=True)

    ########################################################
    #######                                          #######
    #######          Step 4: Save To Database        #######
    #######                                          #######
    ########################################################
    # Step 4.1: Overwrite on database latest table
    Latest_Emails.overwrite_self(tblLatestScored) #TODO: solve this
    # Step 4.2: Append on database historical table
    Latest_Emails.append_historical(tblLatestScored, Historical_Emails)
    
    return jsonify(Latest_Emails.to_json())

# complete
@app.route('/send_emails')
def send_emails():    
    ########################################################
    #######                                          #######
    #######              Step 1: Get Data            #######
    #######                                          #######
    ########################################################
    tblEmails = pd.read_csv(
        os.path.join(config.strPathStorageML, config.strNameCSVEmails)
    ) 

    ########################################################
    #######                                          #######
    #######         Step 2: Indivdual Sending        #######
    #######                                          #######
    ########################################################
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(
            config.strEmailUser, 
            config.strEmailPass
        )
        for intIndex, rowRow in tblEmails.iterrows():
            if (intIndex%100 == 0) and (intIndex!=0):
                print('[[send_emails()]] Email sending limit reached. Sleeping for 5 minutes...')
                time.sleep(300) 
            msg = MIMEText(rowRow['LLM_Response'])
            msg['Subject'] = config.strEmailSubject
            msg['From'] = config.strEmailFrom
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
    strTableName = data.get('strTableVersion') + '_' + data.get('strTableName')
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
