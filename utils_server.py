
import string
import random
import llm.GenAIModel as GenAIModel
import server_web_config


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
    with open(server_web_config.strPathPersonaLLM, "r", encoding="utf-8") as file:
        strTemplateContextResponse = file.read()
    # Step 2: Create LLM
    objLLM = llm.LLM_Email(intLLMProvider = 1, 
        strIngestPath = server_web_config.strPathStorageLLM,
        strPromptTemplate = strTemplateContextResponse, 
        strAPIKey = server_web_config.strAPILLM, 
        fltTemperature = server_web_config.fltTemperature, 
        intRetrieverK = server_web_config.intRetrieverK,
        intLLMAccessory = server_web_config.intLLMAccessory,
    )
    # Step 3: Return LLM
    return objLLM
