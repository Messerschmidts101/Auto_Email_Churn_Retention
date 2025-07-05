'''import os
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"'''


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
from sklearn.ensemble import RandomForestClassifier
import os
import utils
import joblib
import numpy as np
objSpark = SparkSession.builder.getOrCreate()

class Modelling_Class():
    def __init__(self, strPathTrainDataset:str, strPathTrainedModel:str = None, strPathToSaveModels:str = None):
        """
        # Inputs
        1. strPathTrainDataset: string. Path to csv file to use for training in run_training().
        2. strPathTrainedModel: string. Path to trained super pipeline to use for get_predictions().
        3. strPathToSaveModels: string. Path of directory to save models in run_training().

        # Attributes
        1. 

        # Methods
        1. run_training(): train a churn prediction pipeline.
        2. get_predictions(): call a trained churn prediction pipeline to predict.

        """
        self.strPathTrainDataset = strPathTrainDataset 
        self.fltAccuracy = None
        self.objConfusionMatrix = None
        self.fltPrecision = None
        self.fltF1 = None
        self.fltRecall = None
        self.strPathModelBasic = None
        self.strPathModelSuper = strPathTrainedModel
        self.strPathToSaveModels = strPathToSaveModels
        # can never store pyspark dataframe because problem with pkl
    def run_training(self):
        ########################################################
        #######                                          #######
        #######        Step 1: Load Data Training        #######
        #######                                          #######
        ########################################################
        print('running training')
        tblRaw = objSpark.read.option(
            "header", 
            True
        ).csv(
            self.strPathTrainDataset
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
        joblib.dump(objPipeline,'New_Churn_Pred_Model_Basic.pkl')
        self.strPathModelBasic = os.path.join(os.getcwd(),'New_Churn_Pred_Model_Basic.pkl')

        ########################################################
        #######                                          #######
        #######                Step 4: Test              #######
        #######                                          #######
        ########################################################
        y_pred = objPipeline.predict(X_test)

        # Calculate metrics
        acc = accuracy_score(y_test, y_pred)
        y_test = y_test.astype(int)
        y_pred = y_pred.astype(int)

        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        cm = confusion_matrix(y_test, y_pred)

        # Store metrics in object
        self.fltAccuracy = acc
        self.fltPrecision = prec
        self.fltRecall = rec
        self.objConfusionMatrix = cm
        self.fltF1 = f1

        # Display
        print(f"Accuracy: {acc:.4f}")
        print(f"Precision: {prec:.4f}")
        print(f"Recall: {rec:.4f}")
        print(f"F1 Score: {f1:.4f}")
        print("Confusion Matrix:")
        print(cm)

        ########################################################
        #######                                          #######
        #######       Step 5: Attach SHAP Explainer      #######
        #######                                          #######
        ########################################################
        objPreprocessor = Pipeline(objPipeline.steps[:-1])  # everything except Random Forest
        objModel = objPipeline.named_steps["Random_Forest"]
        objSuperPipeline = Pipeline([
            ('Preprocessor', objPreprocessor),
            ('Random_Forest_SHAP', utils.SHAPExplanationTransformer(objModel, intTopFeatCount=5))
        ])
        joblib.dump(objSuperPipeline,'New_Churn_Pred_Model_With_SHAP.pkl')
        self.strPathModelSuper = os.path.join(os.getcwd(),'New_Churn_Pred_Model_With_SHAP.pkl')

    def get_predictions(self,strPathScoring, strPathModelSuper:str = None, boolSavePredictions:bool=False):
        """
        # Inputs
        1. strPathScoring: string. Path to csv file to predict.
        2. strPathModelSuper: string. Path to trained super pipeline. Can be left empty and use the trained model of itself.
        3. boolSavePredictions: boolean. If true, save the predictions as csv file. If false, doesnt save anything.

        # Process
        1. Loads csv file of scoring defined in `strPathScoring`.
        2. Loads super pipeline defined in either `strPathModelSuper` or `self.strPathModelSuper`. Prioritizes `strPathModelSuper`.
        3. Runs the super pipeline to predict if churn or not, with result of SHAP explainer.

        # Output
        1. Returns pandas dataframe containing the predictions and SHAP feature contributions.
        """

        if not strPathModelSuper:
            if self.strPathModelSuper:
                strPathModelSuper = self.strPathModelSuper
            else:
                raise Exception('Attempting to get predictions without trained model. Stopping process immediately.')
        ########################################################
        #######                                          #######
        #######         Step 1: Load Data Scoring        #######
        #######                                          #######
        ########################################################
        tblScoring = objSpark.read.option(
            "header", 
            True
        ).csv(
            strPathScoring
        ).drop(
            'CustomerId'
        ).toPandas()
        ########################################################
        #######                                          #######
        #######            Step 2: Load Model            #######
        #######                                          #######
        ########################################################
        objPipeline = joblib.load(self.strPathModelSuper)
        ########################################################
        #######                                          #######
        #######              Step 3: Predict             #######
        #######                                          #######
        ########################################################
        tblPredictions = objPipeline.transform(tblScoring) #pandas df
        print('Check predictions here')
        print(tblPredictions)
        # tblPredictions.to_csv(os.path.join(os.getcwd(),'output.csv'), index=False)
        return tblPredictions
        

