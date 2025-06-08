# python model/A_Data_Prep.py
import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"

from pyspark.sql import SparkSession
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.ensemble import RandomForestClassifier

import utils
import pickle
objSpark = SparkSession.builder.getOrCreate()


########################################################
#######                                          #######
#######        Step 1: Load Data Training        #######
#######                                          #######
########################################################
tblRaw = objSpark.read.option("header", True).csv(os.path.join('model','dataset','source.csv')).drop('CustomerId').toPandas()
X,y = tblRaw[[strColName for strColName in tblRaw.columns if strColName != 'Exited']], tblRaw['Exited']
# Learnings:
# 1. Customer Id are always unique
# 2. Surname, Geo, & Gender can have duplicates
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


########################################################
#######                                          #######
#######         Step 2: Assemble Pipeline        #######
#######                                          #######
########################################################
lisstrColNamesX = X.columns.tolist()
pipeline = Pipeline([
    ('Diguised_Nulls', utils.Disguised_Nulls_Transformer(lisstrColNamesX, boolVerbose=True)),
    ('Coerce_Type', utils.Coerce_Type_Transformer(lisstrColNamesX, boolVerbose=True)),
    ('Imputer', utils.Imputer_Transformer(lisstrColNamesX, boolVerbose=True)),
    ('Encoder', utils.Encoder_Transformer(lisstrColNamesX, boolVerbose=True)),
    ('Random_Forest', RandomForestClassifier(n_estimators=100, random_state=42))
])

########################################################
#######                                          #######
#######            Step 3: Fit Pipeline          #######
#######                                          #######
########################################################
pipeline.fit(X_train, y_train)
with open('temp_pipeline_preprocess.pkl', 'wb') as f:
    pickle.dump(pipeline, f)

########################################################
#######                                          #######
#######                Step 4: Test              #######
#######                                          #######
########################################################
y_pred = pipeline.predict(X_test)
acc = accuracy_score(y_test, y_pred)
cm = confusion_matrix(y_test, y_pred)

print(f"Accuracy: {acc:.4f}")
print("Confusion Matrix:")
print(cm)
