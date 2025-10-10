
import string
import random
import llm.llm_class as llm
import server_web_config


# complete
def create_llm() -> llm:
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

# complete
def create_random_string(intLength:int=12, strCharactersForRandomString = string.ascii_letters + string.digits):
    """
    # Input
    1. intLength: integer. Length of random string to generate.
    2. strCharactersForRandomString: string. Characters to be included for random string generation.
    # Process
    1. Generates a random string fom pool of characters defined by `strCharactersForRandomString`. The length of the random string depends on `intLength`.
    # Output
    1. Returns a random string. This purpose is commonly used to create unique id.
    """
    return ''.join(
        random.choices(
            strCharactersForRandomString,
            k = intLength
        )
    )
