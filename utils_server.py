
import string
import random
import llm.GenAIModel as GenAIModel
import app.config as config


# complete
def create_llm() -> GenAIModel:
    """
    # Input
    1. None

    # Process
    1. Creates object of our proprietary LLM class.

    # Output
    1. Returns our proprietary LLM object.
    """
    # Step 1: Get Persona
    with open(config.strPathPersonaLLM, "r", encoding="utf-8") as file:
        strTemplateContextResponse = file.read()
    # Step 2: Create LLM
    objLLM = GenAIModel.LLM_Email(intLLMProvider = 1, 
        strIngestPath = config.strPathStorageLLM,
        strPromptTemplate = strTemplateContextResponse, 
        strAPIKey = config.strAPILLM, 
        fltTemperature = config.fltTemperature, 
        intRetrieverK = config.intRetrieverK,
        intLLMAccessory = config.intLLMAccessory,
    )
    # Step 3: Return LLM
    return objLLM
