# TODO: Ensure order of training by:
# 1. including ordering of pandas table with index column before train test split
# 2. including ordering of pandas table with index column every after step of transformation
# TODO: Create transformer that test and stores record of accuracy, confusion matrix, feat importance
# TODO: Feature Engineering

# python model/A_Data_Prep.py
import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.ensemble import RandomForestClassifier

import utils
import pickle
import numpy as np
objSpark = SparkSession.builder.getOrCreate()

########################################################
#######                                          #######
#######        Step 1: Load Data Training        #######
#######                                          #######
########################################################
tblRaw = objSpark.read.option(
    "header", 
    True
).csv(
    os.path.join('model','dataset','source.csv')
).drop(
    'CustomerId'
).toPandas()

X,y = tblRaw[[strColName for strColName in tblRaw.columns if strColName != 'Exited']], tblRaw['Exited']
# Learnings:
# 1. Customer Id doesnt have duplicates
# 2. Surname, Geo, & Gender can have duplicates
# 3. Exited 0: 7960
# 4. Exited 1: 2034
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
for tblTbl in [X_train, X_test]:
    tblTbl['Row_Number'] = np.arange(len(tblTbl))

########################################################
#######                                          #######
#######      Step 2: Assemble Base Pipeline      #######
#######                                          #######
########################################################
lisstrColNamesX = X_train.columns.tolist()
lisstrColNamesXFinal = lisstrColNamesX + ['Age_Tenure_Ratio','Balance_Salary_Ratio']
objPipeline = Pipeline([
    ('Order', utils.Order_Transformer()),
    ('Diguised_Nulls', utils.Disguised_Nulls_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
    ('Coerce_Type', utils.Coerce_Type_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
    ('Imputer', utils.Imputer_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
    ('Encoder', utils.Encoder_Transformer(lisstrColNamesX, boolVerbose=True, lisstrColNamesExclude = ['Row_Number'])),
    ('Age_Tenure_Ratio', utils.Age_Tenure_Ratio('Age','Tenure','Age_Tenure_Ratio', boolVerbose=True)),
    ('Balance_Salary_Ratio', utils.Balance_Salary_Ratio('Balance','EstimatedSalary','Balance_Salary_Ratio', boolVerbose=True)),
    ('Selecter', utils.Select_Transformer(lisstrColNamesXFinal, boolVerbose=True)),
    ('Random_Forest', RandomForestClassifier(n_estimators=100, random_state=42))
])

########################################################
#######                                          #######
#######         Step 3: Fit Base Pipeline        #######
#######                                          #######
########################################################
objPipeline.fit(X_train, y_train)
with open('Churn_Pred_Model.pkl', 'wb') as f:
    pickle.dump(objPipeline, f)

########################################################
#######                                          #######
#######                Step 4: Test              #######
#######                                          #######
########################################################
y_pred = objPipeline.predict(X_test)
acc = accuracy_score(y_test, y_pred)
cm = confusion_matrix(y_test, y_pred)

print(f"Accuracy: {acc:.4f}")
print("Confusion Matrix:")
print(cm)


########################################################
#######                                          #######
#######       Step 5: Attach SHAP Explainer      #######
#######                                          #######
########################################################
with open('Churn_Pred_Model.pkl', 'rb') as f:
    objPipeline = pickle.load(f)
    objPreprocessor = Pipeline(objPipeline.steps[:-1])  # everything except Random Forest
    objModel = objPipeline.named_steps["Random_Forest"]

super_pipeline = Pipeline([
    ('Preprocessor', objPreprocessor),
    ('Random_Forest_SHAP', utils.SHAPExplanationTransformer(objModel, intTopFeatCount=5))
])

with open('Churn_Pred_Model_With_SHAP.pkl', 'wb') as f:
    pickle.dump(super_pipeline, f)