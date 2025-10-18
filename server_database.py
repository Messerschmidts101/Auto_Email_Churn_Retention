from flask_sqlalchemy import SQLAlchemy
from datetime import date
import uuid
import pandas as pd

db = SQLAlchemy()
########################################################
#######                                          #######
#######          Step 0: Table Functions         #######
#######                                          #######
########################################################
class Table_Functions:
    """Provides to_dict() and to_json() helpers for SQLAlchemy models. Because this helps us produce outputs easily"""

    def to_dict(self, lisstrColNamesExclude:list[str]=None):
        """Convert a single row to a dict"""
        lisstrColNamesExclude = set(lisstrColNamesExclude or [])
        return {
            c.name: getattr(self, c.name) 
            for c in self.__table__.columns
            if c.name not in lisstrColNamesExclude
        }
    
    @classmethod
    def to_json(cls, lisstrColNamesExclude:list[str]=None):
        """Return all rows as list of dicts"""
        lisstrColNamesExclude = set(lisstrColNamesExclude or [])
        rows = cls.query.all()
        return [
            row.to_dict(lisstrColNamesExclude = lisstrColNamesExclude) 
            for row in rows
        ]
    
    @classmethod
    def overwrite_self(cls,tblInput: pd.DataFrame):
        """
        Overwrites self with new records.

        # Input
        1. tblInput: pandas dataframe only. The new data to insert with.

        # Process
        1. Deletes existing records of the table.
        2. Bulk insert new data defined in `tblInput`

        # Output
        1. Changes all records of the original table with the values of `tblInput`.
        """
        # Step 1: Assert input format
        if not isinstance(tblInput, pd.DataFrame):
            raise TypeError("tblInput must be a pandas DataFrame")
        tblInput = tblInput.to_dict(orient="records")

        # Step 2: Delete existing records
        db.session.query(cls).delete()
        db.session.commit()

        # Step 3: Insert new records
        db.session.bulk_insert_mappings(cls, tblInput)
        db.session.commit()

        return True
    
    @classmethod
    def append_historical(cls,tblInput: pd.DataFrame, objHistoricalTable):
        """
        Append new records on historical data following a predefined format.

        # Input
        1. tblInput: pandas dataframe only. The new data to insert with.

        # Process
        1. Deletes existing records of the table.
        2. Bulk insert new data defined in `tblInput`

        # Output
        1. Changes all records of the original table with the values of `tblInput`.
        """
        # Step 1: Assert input format
        if not isinstance(tblInput, pd.DataFrame):
            raise TypeError("tblInput must be a pandas DataFrame")
        dtNow = date.today()
        tblInput['meta_DateCreated'] = dtNow
        tblInput['meta_Id'] = [
            f"{dtNow}_{uuid.uuid4()}" for _ in range(len(tblInput))
        ]
        tblInput = tblInput.to_dict(orient="records")
        
        # Step 2: Insert new records
        db.session.bulk_insert_mappings(objHistoricalTable,tblInput)
        db.session.commit()

        return True

    
########################################################
#######                                          #######
#######           Step 1: Latest Tables          #######
#######                                          #######
########################################################
class Latest_Training(db.Model,Table_Functions):
    __tablename__ = 'Latest_Training'  # Required attribute to do SQL querying
    CustomerId = db.Column(db.Integer, primary_key=True)
    Surname = db.Column(db.String(50), nullable=True)
    CreditScore = db.Column(db.Integer, nullable=True)
    Geography = db.Column(db.String(100), nullable=True)
    Gender = db.Column(db.String(10), nullable=True)
    Age = db.Column(db.Integer, nullable=True)
    Tenure = db.Column(db.Integer, nullable=True)
    Balance = db.Column(db.Float, nullable=True)
    NumOfProducts = db.Column(db.Integer, nullable=True)
    HasCrCard = db.Column(db.Boolean, nullable=True)
    IsActiveMember = db.Column(db.Boolean, nullable=True)
    EstimatedSalary = db.Column(db.Float, nullable=True)
    Exited = db.Column(db.Boolean, nullable=True)
    RecentSatisfactionScore = db.Column(db.Float, nullable=True)

class Latest_Scoring(db.Model,Table_Functions):
    __tablename__ = 'Latest_Scoring' 
    CustomerId = db.Column(db.Integer, primary_key=True)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(50), nullable=False)
    CreditScore = db.Column(db.Integer, nullable=False)
    Geography = db.Column(db.String(100), nullable=False)
    Gender = db.Column(db.String(10), nullable=False)
    Age = db.Column(db.Integer, nullable=False)
    Tenure = db.Column(db.Integer, nullable=False)
    Balance = db.Column(db.Float, nullable=False)
    NumOfProducts = db.Column(db.Integer, nullable=False)
    HasCrCard = db.Column(db.Boolean, nullable=False)
    IsActiveMember = db.Column(db.Boolean, nullable=False)
    EstimatedSalary = db.Column(db.Float, nullable=False)
    Exited = db.Column(db.Boolean, nullable=False)
    RecentSatisfactionScore = db.Column(db.Float, nullable=False)

class Latest_Scored(db.Model,Table_Functions):
    __tablename__ = 'Latest_Scored' 
    CustomerId = db.Column(db.Integer, primary_key=True)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(100), nullable=False)
    Prediction = db.Column(db.Boolean, nullable=False)
    Churn_Probability = db.Column(db.Float, nullable=False)
    Top_1_Feat = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Score = db.Column(db.Float, nullable=False)
    Top_2_Feat = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Score = db.Column(db.Float, nullable=False)
    Top_3_Feat = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Score = db.Column(db.Float, nullable=False)
    Top_4_Feat = db.Column(db.String(100), nullable=False)
    Top_4_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_4_Feat_Score = db.Column(db.Float, nullable=False)
    Top_5_Feat = db.Column(db.String(100), nullable=False)
    Top_5_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_5_Feat_Score = db.Column(db.Float, nullable=False)
    
class Latest_Emails(db.Model,Table_Functions):
    __tablename__ = 'Latest_Emails' 
    CustomerId = db.Column(db.Integer, primary_key=True)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(100), nullable=False)
    Prediction = db.Column(db.Boolean, nullable=False)
    Churn_Probability = db.Column(db.Float, nullable=False)
    LLM_Response = db.Column(db.String(4000), nullable=False)
    Top_1_Feat = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Score = db.Column(db.Float, nullable=False)
    Top_2_Feat = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Score = db.Column(db.Float, nullable=False)
    Top_3_Feat = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Score = db.Column(db.Float, nullable=False)

########################################################
#######                                          #######
#######         Step 2: Historical Tables        #######
#######                                          #######
########################################################
class Historical_Models(db.Model,Table_Functions):
    __tablename__ = 'Historical_Models' 
    meta_Id = db.Column(db.String(50), primary_key=True)
    meta_DateCreated = db.Column(db.Date, nullable=False)
    Accuracy = db.Column(db.Float, nullable=False)
    Precision = db.Column(db.Float, nullable=False)
    Recall = db.Column(db.Float, nullable=False)
    F1 = db.Column(db.Float, nullable=False)
    CountTrueNegative = db.Column(db.Integer, nullable=False)
    CountFalsePositive = db.Column(db.Integer, nullable=False)
    CountFalseNegative = db.Column(db.Integer, nullable=False)
    CountTruePositive = db.Column(db.Integer, nullable=False)
    CountTrainingPositiveClass = db.Column(db.Integer, nullable=False)
    CountTrainingNegativeClass = db.Column(db.Integer, nullable=False)
    CountTestPositiveClass = db.Column(db.Integer, nullable=False)
    CountTestNegativeClass = db.Column(db.Integer, nullable=False)

class Historical_Training(db.Model,Table_Functions):
    __tablename__ = 'Historical_Training' 
    meta_Id = db.Column(db.String(50), primary_key=True)
    meta_DateCreated = db.Column(db.Date, nullable=False)
    CustomerId = db.Column(db.Integer, nullable=False)
    Surname = db.Column(db.String(50), nullable=True)
    CreditScore = db.Column(db.Integer, nullable=True)
    Geography = db.Column(db.String(100), nullable=True)
    Gender = db.Column(db.String(10), nullable=True)
    Age = db.Column(db.Integer, nullable=True)
    Tenure = db.Column(db.Integer, nullable=True)
    Balance = db.Column(db.Float, nullable=True)
    NumOfProducts = db.Column(db.Integer, nullable=True)
    HasCrCard = db.Column(db.Boolean, nullable=True)
    IsActiveMember = db.Column(db.Boolean, nullable=True)
    EstimatedSalary = db.Column(db.Float, nullable=True)
    Exited = db.Column(db.Boolean, nullable=True)
    RecentSatisfactionScore = db.Column(db.Float, nullable=True)

class Historical_Scoring(db.Model,Table_Functions):
    __tablename__ = 'Historical_Scoring' 
    meta_Id = db.Column(db.String(50), primary_key=True)
    meta_DateCreated = db.Column(db.Date, nullable=False)
    CustomerId = db.Column(db.Integer, nullable=False)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(50), nullable=False)
    CreditScore = db.Column(db.Integer, nullable=False)
    Geography = db.Column(db.String(100), nullable=False)
    Gender = db.Column(db.String(10), nullable=False)
    Age = db.Column(db.Integer, nullable=False)
    Tenure = db.Column(db.Integer, nullable=False)
    Balance = db.Column(db.Float, nullable=False)
    NumOfProducts = db.Column(db.Integer, nullable=False)
    HasCrCard = db.Column(db.Boolean, nullable=False)
    IsActiveMember = db.Column(db.Boolean, nullable=False)
    EstimatedSalary = db.Column(db.Float, nullable=False)
    Exited = db.Column(db.Boolean, nullable=False)
    RecentSatisfactionScore = db.Column(db.Float, nullable=False)

class Historical_Scored(db.Model,Table_Functions):
    __tablename__ = 'Historical_Scored' 
    meta_Id = db.Column(db.String(50), primary_key=True)
    meta_DateCreated = db.Column(db.Date, nullable=False)
    CustomerId = db.Column(db.Integer, nullable=False)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(100), nullable=False)
    Prediction = db.Column(db.Boolean, nullable=False)
    Churn_Probability = db.Column(db.Float, nullable=False)
    Top_1_Feat = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Score = db.Column(db.Float, nullable=False)
    Top_2_Feat = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Score = db.Column(db.Float, nullable=False)
    Top_3_Feat = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Score = db.Column(db.Float, nullable=False)
    Top_4_Feat = db.Column(db.String(100), nullable=False)
    Top_4_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_4_Feat_Score = db.Column(db.Float, nullable=False)
    Top_5_Feat = db.Column(db.String(100), nullable=False)
    Top_5_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_5_Feat_Score = db.Column(db.Float, nullable=False)

class Historical_Emails(db.Model,Table_Functions):
    __tablename__ = 'Historical_Emails' 
    meta_Id = db.Column(db.String(50), primary_key=True)
    meta_DateCreated = db.Column(db.Date, nullable=False)
    CustomerId = db.Column(db.Integer, nullable=False)
    Surname = db.Column(db.String(50), nullable=False)
    Email = db.Column(db.String(100), nullable=False)
    Prediction = db.Column(db.Boolean, nullable=False)
    Churn_Probability = db.Column(db.Float, nullable=False)
    LLM_Response = db.Column(db.String(4000), nullable=False)
    Top_1_Feat = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_1_Feat_Score = db.Column(db.Float, nullable=False)
    Top_2_Feat = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_2_Feat_Score = db.Column(db.Float, nullable=False)
    Top_3_Feat = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Value = db.Column(db.String(100), nullable=False)
    Top_3_Feat_Score = db.Column(db.Float, nullable=False)
    
    