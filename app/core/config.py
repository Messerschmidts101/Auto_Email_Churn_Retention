import os
from dotenv import load_dotenv
load_dotenv()

########################################################
#######                                          #######
#######                A. File Paths             #######
#######                                          #######
########################################################
strPathStorageML = os.path.join(os.getcwd(),'website','Temporary_Files')
strPathStorageLLM = os.path.join(os.getcwd(),'website','Temporary_Files','Context')
strNameMLFinal = 'churn_prediction_model.pkl'
strNameCSVTrain = "train.csv"
strNameCSVScoring = "scoring.csv"
strNameCSVScored = "scored.csv"
strNameCSVEmails = "emails.csv"

########################################################
#######                                          #######
#######                B. Emailing               #######
#######                                          #######
########################################################
strEmailSubject = "Hello and We Miss You"
strEmailFrom = os.getenv("EMAIL_APP_EMAIL")
strEmailUser = os.getenv("EMAIL_APP_EMAIL") # yes intentionally same to strEmailFrom
strEmailPass = os.getenv("EMAIL_APP_PASSWORD")

########################################################
#######                                          #######
#######                  C. LLM                  #######
#######                                          #######
########################################################
strAPILLM = os.getenv("GROQ_API_KEY")
strPathPersonaLLM = os.path.join(os.getcwd(),'website','Temporary_Files','persona_1.txt')
fltTemperature = 0.1
intRetrieverK = 5
intLLMAccessory = 4

########################################################
#######                                          #######
#######                   D. ML                  #######
#######                                          #######
########################################################

lisstrFeatsDefault = [
    #"CustomerId",
    #"Surname",
    "CreditScore",
    "Geography",
    "Gender",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary",
    "Exited",
    "RecentSatisfactionScore",
]
intModelDefault = 3
intCountFeatsScoring = 5