#  python -m model.ChurnPredictionModel

from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
from sklearn.ensemble import RandomForestClassifier
import os
from model import utils_model
import joblib
import numpy as np
import pandas as pd
import time

class ChurnPredictionModel():
    def __init__(self, strPathTrainDataset:str, strPathTrainedModel:str = None, strPathToSaveModels:str = ''):
        """
        # Inputs
        1. strPathTrainDataset: string. Path to csv file to use for training in run_training().
        2. strPathTrainedModel: string. Path to trained super pipeline to use for get_predictions().
        3. strPathToSaveModels: string. Path of directory to save models in run_training(). Default must be '' and not `None`, because error would occur if `None`.

        # Attributes
        1. strPathTrainDataset: string. Path to csv file to use for training in run_training().
        2. strPathTrainedModel: string. Path to trained super pipeline to use for get_predictions().
        3. strPathToSaveModels: string. Path of directory to save models in run_training(). Default must be '' and not `None`, because error would occur if `None`.
        4. fltAccuracy: float. The recent accuracy of trained model produced from run_training().
        5. fltPrecision: float. The recent precision of trained model produced from run_training().
        6. fltRecall: float. The recent recall of trained model produced from run_training().
        7. fltF1: float. The recent f1 score of trained model produced from run_training().
        8. objConfusionMatrix: float. The recent confusion matrix of trained model produced from run_training().

        # Methods
        1. run_training(): train a churn prediction pipeline.
        2. get_predictions(): call a trained churn prediction pipeline to predict.

        """
        self.strPathTrainDataset = strPathTrainDataset 
        self.strPathSHAPPipeline = strPathTrainedModel
        self.strPathToSaveModels = strPathToSaveModels
        self.intCountTrainPositiveClass = None
        self.intCountTrainNegativeClass = None
        self.intCountTestPositiveClass = None
        self.intCountTestNegativeClass = None
        self.fltAccuracy = None
        self.fltPrecision = None
        self.fltRecall = None
        self.fltF1 = None
        self.objConfusionMatrix = None
        self.strPathModelBasic = None
        # can never store pyspark dataframe because problem with pkl
    
    def run_training(self, boolVerbose = True):
        ########################################################
        #######                                          #######
        #######        Step 1: Load Data Training        #######
        #######                                          #######
        ########################################################
        # Learnings:
        # 1. Customer Id doesnt have duplicates
        # 2. Surname, Geo, & Gender can have duplicates
        # 3. Exited 0: 7960
        # 4. Exited 1: 2034
        tblRaw = pd.read_csv(
            self.strPathTrainDataset
        ).drop(
            'CustomerId', 
            axis='columns'
        )
        X,y = tblRaw[[strColName for strColName in tblRaw.columns if strColName != 'Exited']], tblRaw['Exited']
        X_train, X_test, y_train, y_test = train_test_split(
            X, 
            y, 
            test_size=0.2, 
            random_state=42
        )
        for tblTbl in [X_train, X_test]:
            tblTbl['Row_Number'] = np.arange(len(tblTbl))

        ########################################################
        #######                                          #######
        #######      Step 2: Assemble Base Pipeline      #######
        #######                                          #######
        ########################################################
        lisstrColNamesX = X_train.columns.tolist()
        lisstrColNamesXFinal = lisstrColNamesX + ['Age_Tenure_Ratio','Balance_Salary_Ratio']
        objBasePipeline = Pipeline([
            ('Order', utils_model.Order_Transformer()),
            ('Diguised_Nulls', utils_model.Disguised_Nulls_Transformer(lisstrColNamesX, boolVerbose=boolVerbose, lisstrColNamesExclude = ['Row_Number'])),
            ('Coerce_Type', utils_model.Coerce_Type_Transformer(lisstrColNamesX, boolVerbose=boolVerbose, lisstrColNamesExclude = ['Row_Number'])),
            ('Imputer', utils_model.Imputer_Transformer(lisstrColNamesX, boolVerbose=boolVerbose, lisstrColNamesExclude = ['Row_Number'])),
            ('Encoder', utils_model.Encoder_Transformer(lisstrColNamesX, boolVerbose=boolVerbose, lisstrColNamesExclude = ['Row_Number'])),
            ('Age_Tenure_Ratio', utils_model.Age_Tenure_Ratio('Age','Tenure','Age_Tenure_Ratio', boolVerbose=boolVerbose)),
            ('Balance_Salary_Ratio', utils_model.Balance_Salary_Ratio('Balance','EstimatedSalary','Balance_Salary_Ratio', boolVerbose=boolVerbose)),
            ('Selecter', utils_model.Select_Transformer(lisstrColNamesXFinal, boolVerbose=boolVerbose)),
            ('Random_Forest', RandomForestClassifier(n_estimators=100, random_state=42))
        ])

        ########################################################
        #######                                          #######
        #######         Step 3: Fit Base Pipeline        #######
        #######                                          #######
        ########################################################
        objBasePipeline.fit(X_train, y_train)

        ########################################################
        #######                                          #######
        #######                Step 4: Test              #######
        #######                                          #######
        ########################################################
        y_pred = objBasePipeline.predict(X_test)

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
        self.fltF1 = f1
        self.objConfusionMatrix = cm
        self.intCountTrainPositiveClass = np.bincount(y_train)[1]
        self.intCountTrainNegativeClass = np.bincount(y_train)[0]
        self.intCountTestPositiveClass = np.bincount(y_test)[1]
        self.intCountTestNegativeClass = np.bincount(y_test)[0]
        
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
        # Step 1: Save base model
        joblib.dump(
            objBasePipeline,
            os.path.join(self.strPathToSaveModels, 'RandomForest_Base_Model.pkl')
        )
        self.strPathBasePipeline = os.path.join(self.strPathToSaveModels, 'RandomForest_Base_Model.pkl')

        # Step 2: Save shap model
        objPreprocessor = Pipeline(objBasePipeline.steps[:-1])
        objModel = objBasePipeline.named_steps["Random_Forest"]
        objSuperPipeline = Pipeline([
            ('Preprocessor', objPreprocessor),
            ('Random_Forest_SHAP', utils_model.SHAPExplanationTransformer(objModel, intTopFeatCount=5))
        ])
        joblib.dump(
            objSuperPipeline,
            os.path.join(self.strPathToSaveModels, 'RandomForest_SHAP_Model.pkl')
        )
        self.strPathSHAPPipeline = os.path.join(self.strPathToSaveModels, 'RandomForest_SHAP_Model.pkl')

    def get_predictions(self,strPathScoring, strPathSHAPPipeline:str = None, strPathSavePredictions:str=None, boolVerbose = False):
        """
        # Inputs
        1. strPathScoring: string. Path to csv file to predict.
        2. strPathSHAPPipeline: string. Path to trained super pipeline. Can be left empty and use the trained model of itself.
        3. strPathSavePredictions: string. Path to save predictions as csv. Can be left empty and no save will be done. 
        4. boolVerbose: boolean. If true, display results of data transformation at each stage of the pipeline.

        # Process
        1. Loads csv file of scoring defined in `strPathScoring`.
        2. Loads super pipeline defined in either `strPathSHAPPipeline` or `self.strPathSHAPPipeline`. Prioritizes `strPathSHAPPipeline`.
        3. Runs the super pipeline to predict if churn or not, with result of SHAP explainer.

        # Output
        1. Returns pandas dataframe containing the predictions and SHAP feature contributions.
        2. If there is value in `strPathSavePredictions`, stores the prediction to csv.
        """

        if not strPathSHAPPipeline:
            if self.strPathSHAPPipeline:
                strPathSHAPPipeline = self.strPathSHAPPipeline
            else:
                raise Exception('Attempting to get predictions without trained model. Stopping process immediately.')
        
        ########################################################
        #######                                          #######
        #######         Step 1: Load Data Scoring        #######
        #######                                          #######
        ########################################################
        tblScoring = pd.read_csv(strPathScoring)
        ########################################################
        #######                                          #######
        #######            Step 2: Load Model            #######
        #######                                          #######
        ########################################################
        objPipeline = joblib.load(self.strPathSHAPPipeline)
        for strStepName,objTransformer in objPipeline.named_steps['Preprocessor'].named_steps.items():
            if hasattr(objTransformer,'boolVerbose'):
                objTransformer.boolVerbose = boolVerbose
            
        ########################################################
        #######                                          #######
        #######              Step 3: Predict             #######
        #######                                          #######
        ########################################################
        tblPredictions = objPipeline.transform(tblScoring) #pandas df
        print('Check predictions here')
        print(tblPredictions)
        if strPathSavePredictions:
            tblPredictions.to_csv(strPathSavePredictions, index=False)
        return tblPredictions
        
def main(
        strPathTrainDataset = os.path.join('documents','train.csv'),
        strPathStorageML = os.path.join('model'),
        strPathScoreDataset = os.path.join('documents','scoring.csv')
    ):
    ########################################################
    #######                                          #######
    #######           Step 1: Train and Dump         #######
    #######                                          #######
    ########################################################
    objModellingClass = ChurnPredictionModel(
        strPathTrainDataset = strPathTrainDataset,
        strPathToSaveModels = strPathStorageML
    )
    timeStart = time.time()
    objModellingClass.run_training(boolVerbose = False)
    joblib.dump(objModellingClass, os.path.join(strPathStorageML, 'New_Churn_Pred_Model_With_SHAP.pkl')) 
    
    ########################################################
    #######                                          #######
    #######               Step 2: Predict            #######
    #######                                          #######
    ########################################################
    
    objPipeline = joblib.load(os.path.join(strPathStorageML, 'New_Churn_Pred_Model_With_SHAP.pkl')) 
    tblLatestScored = objPipeline.get_predictions(strPathScoring = strPathScoreDataset)
    print('AAA: Check Predictions here')
    print(tblLatestScored.head())
    
    timeEnd = time.time()
    print(f"Time taken: {timeEnd - timeStart:.2f} seconds")


if __name__ == "__main__":
    main()