from sqlalchemy import Column, Integer, Boolean, String, Float, Date
from datetime import date
import uuid
import pandas as pd
from app.db.database import objBase

########################################################
#######                                          #######
#######           Step 1: Latest Tables          #######
#######                                          #######
########################################################
class Latest_Training(objBase):
    __tablename__ = 'Latest_Training'  # Required attribute to do SQL querying
    CustomerId = Column(Integer, primary_key=True)
    Surname = Column(String(50), nullable=True)
    CreditScore = Column(Integer, nullable=True)
    Geography = Column(String(100), nullable=True)
    Gender = Column(String(10), nullable=True)
    Age = Column(Integer, nullable=True)
    Tenure = Column(Integer, nullable=True)
    Balance = Column(Float, nullable=True)
    NumOfProducts = Column(Integer, nullable=True)
    HasCrCard = Column(Boolean, nullable=True)
    IsActiveMember = Column(Boolean, nullable=True)
    EstimatedSalary = Column(Float, nullable=True)
    Exited = Column(Boolean, nullable=True)
    RecentSatisfactionScore = Column(Float, nullable=True)

class Latest_Scoring(objBase):
    __tablename__ = 'Latest_Scoring' 
    CustomerId = Column(Integer, primary_key=True)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(50), nullable=False)
    CreditScore = Column(Integer, nullable=False)
    Geography = Column(String(100), nullable=False)
    Gender = Column(String(10), nullable=False)
    Age = Column(Integer, nullable=False)
    Tenure = Column(Integer, nullable=False)
    Balance = Column(Float, nullable=False)
    NumOfProducts = Column(Integer, nullable=False)
    HasCrCard = Column(Boolean, nullable=False)
    IsActiveMember = Column(Boolean, nullable=False)
    EstimatedSalary = Column(Float, nullable=False)
    Exited = Column(Boolean, nullable=False)
    RecentSatisfactionScore = Column(Float, nullable=False)

class Latest_Scored(objBase):
    __tablename__ = 'Latest_Scored' 
    CustomerId = Column(Integer, primary_key=True)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(100), nullable=False)
    Prediction = Column(Boolean, nullable=False)
    Churn_Probability = Column(Float, nullable=False)
    Top_1_Feat = Column(String(100), nullable=False)
    Top_1_Feat_Value = Column(String(100), nullable=False)
    Top_1_Feat_Score = Column(Float, nullable=False)
    Top_2_Feat = Column(String(100), nullable=False)
    Top_2_Feat_Value = Column(String(100), nullable=False)
    Top_2_Feat_Score = Column(Float, nullable=False)
    Top_3_Feat = Column(String(100), nullable=False)
    Top_3_Feat_Value = Column(String(100), nullable=False)
    Top_3_Feat_Score = Column(Float, nullable=False)
    Top_4_Feat = Column(String(100), nullable=False)
    Top_4_Feat_Value = Column(String(100), nullable=False)
    Top_4_Feat_Score = Column(Float, nullable=False)
    Top_5_Feat = Column(String(100), nullable=False)
    Top_5_Feat_Value = Column(String(100), nullable=False)
    Top_5_Feat_Score = Column(Float, nullable=False)
    
class Latest_Emails(objBase):
    __tablename__ = 'Latest_Emails' 
    CustomerId = Column(Integer, primary_key=True)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(100), nullable=False)
    Prediction = Column(Boolean, nullable=False)
    Churn_Probability = Column(Float, nullable=False)
    LLM_Response = Column(String(4000), nullable=False)
    Top_1_Feat = Column(String(100), nullable=False)
    Top_1_Feat_Value = Column(String(100), nullable=False)
    Top_1_Feat_Score = Column(Float, nullable=False)
    Top_2_Feat = Column(String(100), nullable=False)
    Top_2_Feat_Value = Column(String(100), nullable=False)
    Top_2_Feat_Score = Column(Float, nullable=False)
    Top_3_Feat = Column(String(100), nullable=False)
    Top_3_Feat_Value = Column(String(100), nullable=False)
    Top_3_Feat_Score = Column(Float, nullable=False)

########################################################
#######                                          #######
#######         Step 2: Historical Tables        #######
#######                                          #######
########################################################
class Historical_Models(objBase):
    __tablename__ = 'Historical_Models' 
    meta_Id = Column(String(50), primary_key=True)
    meta_DateCreated = Column(Date, nullable=False)
    Accuracy = Column(Float, nullable=False)
    Precision = Column(Float, nullable=False)
    Recall = Column(Float, nullable=False)
    F1 = Column(Float, nullable=False)
    CountTrueNegative = Column(Integer, nullable=False)
    CountFalsePositive = Column(Integer, nullable=False)
    CountFalseNegative = Column(Integer, nullable=False)
    CountTruePositive = Column(Integer, nullable=False)
    CountTrainingPositiveClass = Column(Integer, nullable=False)
    CountTrainingNegativeClass = Column(Integer, nullable=False)
    CountTestPositiveClass = Column(Integer, nullable=False)
    CountTestNegativeClass = Column(Integer, nullable=False)

class Historical_Training(objBase):
    __tablename__ = 'Historical_Training' 
    meta_Id = Column(String(50), primary_key=True)
    meta_DateCreated = Column(Date, nullable=False)
    CustomerId = Column(Integer, nullable=False)
    Surname = Column(String(50), nullable=True)
    CreditScore = Column(Integer, nullable=True)
    Geography = Column(String(100), nullable=True)
    Gender = Column(String(10), nullable=True)
    Age = Column(Integer, nullable=True)
    Tenure = Column(Integer, nullable=True)
    Balance = Column(Float, nullable=True)
    NumOfProducts = Column(Integer, nullable=True)
    HasCrCard = Column(Boolean, nullable=True)
    IsActiveMember = Column(Boolean, nullable=True)
    EstimatedSalary = Column(Float, nullable=True)
    Exited = Column(Boolean, nullable=True)
    RecentSatisfactionScore = Column(Float, nullable=True)

class Historical_Scoring(objBase):
    __tablename__ = 'Historical_Scoring' 
    meta_Id = Column(String(50), primary_key=True)
    meta_DateCreated = Column(Date, nullable=False)
    CustomerId = Column(Integer, nullable=False)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(50), nullable=False)
    CreditScore = Column(Integer, nullable=False)
    Geography = Column(String(100), nullable=False)
    Gender = Column(String(10), nullable=False)
    Age = Column(Integer, nullable=False)
    Tenure = Column(Integer, nullable=False)
    Balance = Column(Float, nullable=False)
    NumOfProducts = Column(Integer, nullable=False)
    HasCrCard = Column(Boolean, nullable=False)
    IsActiveMember = Column(Boolean, nullable=False)
    EstimatedSalary = Column(Float, nullable=False)
    Exited = Column(Boolean, nullable=False)
    RecentSatisfactionScore = Column(Float, nullable=False)

class Historical_Scored(objBase):
    __tablename__ = 'Historical_Scored' 
    meta_Id = Column(String(50), primary_key=True)
    meta_DateCreated = Column(Date, nullable=False)
    CustomerId = Column(Integer, nullable=False)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(100), nullable=False)
    Prediction = Column(Boolean, nullable=False)
    Churn_Probability = Column(Float, nullable=False)
    Top_1_Feat = Column(String(100), nullable=False)
    Top_1_Feat_Value = Column(String(100), nullable=False)
    Top_1_Feat_Score = Column(Float, nullable=False)
    Top_2_Feat = Column(String(100), nullable=False)
    Top_2_Feat_Value = Column(String(100), nullable=False)
    Top_2_Feat_Score = Column(Float, nullable=False)
    Top_3_Feat = Column(String(100), nullable=False)
    Top_3_Feat_Value = Column(String(100), nullable=False)
    Top_3_Feat_Score = Column(Float, nullable=False)
    Top_4_Feat = Column(String(100), nullable=False)
    Top_4_Feat_Value = Column(String(100), nullable=False)
    Top_4_Feat_Score = Column(Float, nullable=False)
    Top_5_Feat = Column(String(100), nullable=False)
    Top_5_Feat_Value = Column(String(100), nullable=False)
    Top_5_Feat_Score = Column(Float, nullable=False)

class Historical_Emails(objBase):
    __tablename__ = 'Historical_Emails' 
    meta_Id = Column(String(50), primary_key=True)
    meta_DateCreated = Column(Date, nullable=False)
    CustomerId = Column(Integer, nullable=False)
    Surname = Column(String(50), nullable=False)
    Email = Column(String(100), nullable=False)
    Prediction = Column(Boolean, nullable=False)
    Churn_Probability = Column(Float, nullable=False)
    LLM_Response = Column(String(4000), nullable=False)
    Top_1_Feat = Column(String(100), nullable=False)
    Top_1_Feat_Value = Column(String(100), nullable=False)
    Top_1_Feat_Score = Column(Float, nullable=False)
    Top_2_Feat = Column(String(100), nullable=False)
    Top_2_Feat_Value = Column(String(100), nullable=False)
    Top_2_Feat_Score = Column(Float, nullable=False)
    Top_3_Feat = Column(String(100), nullable=False)
    Top_3_Feat_Value = Column(String(100), nullable=False)
    Top_3_Feat_Score = Column(Float, nullable=False)
    
    