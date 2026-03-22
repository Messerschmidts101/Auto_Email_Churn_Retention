# For embedding of database
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from langchain_community.document_loaders import TextLoader
from langchain_community.document_loaders import DirectoryLoader
from langchain_community.vectorstores import Chroma

# For LLM chain
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import PromptTemplate

#For LLM
from langchain_groq import ChatGroq

import os
import uuid
from operator import itemgetter
from datetime import datetime
import time

class GenAIModel:
    def __init__(self, intLLMProvider:int, 
                 strIngestPath:str, 
                 strPromptTemplate:str, 
                 strAPIKey:str = None, 
                 fltTemperature:float = 0.1, 
                 intRetrieverK :int = 5,
                 intLLMAccessory:int = None,
                 objEmbeddingModel = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")):
        """
        # Inputs
        1. intLLMProvider: integer. The preset provider for LLM. For now we just limited to '1' as we will only use Groq provider.
        2. strIngestPath: string. The folder path of context.
        3. strPromptTemplate: string. The template of the prompt.
        4. strAPIKey: string. The API key for LLM.
        5. fltTemperature: float. The temperature setting for LLM.
        6. intRetrieverK: integer. The number of retrieved items for embeddings.
        7. intLLMAccessory: int. The preset accessories for LLM. The values are either of the following:
            a. '1': Pass this value to indicate that the RAG-LLM will just have: Context Retriever, and; LLM.
            b. '2': Pass this value to indicate that the RAG-LLM will just have: Chat History Retriever, and; LLM.
            c. '3': Pass this value to indicate that the RAG-LLM will just have: Context Retriever; Chat History Retriever, and; LLM.
            d. '4': Pass this value to indicate that the RAG-LLM will just be on email writing mode (special mode).
        8. objEmbeddingModel: object. The embedding model to be used as retriever for RAG chain.

        # Outputs
        1. Creates a RAG enabled LLM model.
        """
        #=== Placeholder attributes initialized to None or empty list ===#
        # These will be populated later by other methods like ingest_context(), ingest_chat_history(), etc.
        self.objLLM = None
        self.objPromptTemplate = None
        self.objRetrieverContext = None
        self.objRetrieverChatHistory = None
        self.objChain = None
        self.objChainEmailComposer = None
        self.lisChatHistory = []

        #=== Core Configuration Parameters ===#
        # These attributes are needed by downstream components like retrievers and chains.
        self.strAPIKey = strAPIKey
        self.strIngestPath = strIngestPath
        self.objEmbeddingModel = objEmbeddingModel

        # Initialize the LLM and supporting components
        self.initialize_llm(
            intLLMProvider=intLLMProvider,
            strAPIKey=strAPIKey,
            fltTemperature=fltTemperature,
            intRetrieverK=intRetrieverK,
            intLLMAccessory=intLLMAccessory,
            strPromptTemplate=strPromptTemplate
        )

        #=== Preserved Inputs for Rebuilding Chain When Adding New Data ===#
        self.intLLMProvider = intLLMProvider
        self.intRetrieverK = intRetrieverK
        self.intLLMAccessory = intLLMAccessory
        self.strPromptTemplate = strPromptTemplate

    # complete and vetted
    def initialize_llm(self, intLLMProvider, 
                       strAPIKey,
                       fltTemperature, 
                       intRetrieverK,
                       intLLMAccessory,
                       strPromptTemplate):
        """
        # Inputs
        1. intLLMProvider: integer. The preset provider for LLM. For now we just limited to '1' as we will only use Groq provider.
        2. strAPIKey: string. The API key for LLM.
        3. fltTemperature: float. The temperature setting for LLM.
        4. intRetrieverK: integer. The number of retrieved items for embeddings.
        5. intLLMAccessory: int. The preset accessories for LLM. The values are either of the following:
            a. '1': Pass this value to indicate that the RAG-LLM will just have: Context Retriever, and; LLM.
            b. '2': Pass this value to indicate that the RAG-LLM will just have: Chat History Retriever, and; LLM.
            c. '3': Pass this value to indicate that the RAG-LLM will just have: Context Retriever; Chat History Retriever, and; LLM.
            d. '4': Pass this value to indicate that the RAG-LLM will just be on email writing mode (special mode).
        6. strPromptTemplate: string. The template of the prompt.

        # Process
        1. Connects to appropriate LLM provider.

        # Outputs
        1. None, calls self.create_llm() and it is there where objLLM is created. 
        """
        ########################################################
        #######                                          #######
        #######               Step 1: Get LLM            #######
        #######                                          #######
        ########################################################
        dicLLMProvider = {
            1: "meta-llama/llama-4-scout-17b-16e-instruct",
            2: "llama3-70b-8192",
            3: "mistralai/Mistral-7B-Instruct-v0.2",
            4: "gpt-4o-mini",
            5: "meta-llama/llama-4-scout-17b-16e-instruct",
        }
        strModelName = dicLLMProvider.get(intLLMProvider, None)
        if not strModelName:
            raise ValueError(f"Invalid LLM setting: {intLLMProvider}")
        self.create_llm(intLLMProvider = intLLMProvider,
                        strAPIKey = strAPIKey,
                        fltTemperature = fltTemperature,
                        strModelName = strModelName)
        
        ########################################################
        #######                                          #######
        #######          Step 2: Create RAG Chain        #######
        #######                                          #######
        ########################################################
        self.create_chain(intLLMAccessory = intLLMAccessory,
                          intRetrieverK = intRetrieverK,
                          strPromptTemplate = strPromptTemplate)
    
    # complete and vetted
    def create_llm(self,intLLMProvider,strAPIKey,fltTemperature,strModelName):
        """
        # Inputs
        1. intLLMProvider: integer. The preset provider for LLM. For now we just limited to '1' as we will only use Groq provider.
        2. strAPIKey: string. The API key for LLM.
        3. fltTemperature: float. The temperature setting for LLM.
        4. strModelName: string. The specific name of LLM available in the LLM provider.

        # Process
        1. Connects to appropriate LLM provider.

        # Outputs
        1. Creates LLM, ready to be used in RAG chain.
        """
        if intLLMProvider in [1, 2, 5, 6]: # Groq LLM
            if not self.strAPIKey:
                raise ValueError(f"Requires API Key, set value of 'strAPIKey'")
            self.objLLM = ChatGroq(temperature = fltTemperature, 
                                   model_name = strModelName, 
                                   groq_api_key = strAPIKey)
        '''elif intLLMProvider == 3:  # HuggingFace LLM
            self.objLLM = HuggingFaceEndpoint(repo_id = strModelName, 
                                              temperature = fltTemperature, 
                                              token = strAPIKey)
        elif intLLMProvider == 4: # OpenAI LLM
            self.objLLM = AzureChatOpenAI(
                api_key = strAPIKey,
                deployment_name = strModelName,  # Use your specific deployment name
                model = strModelName,  # Or another model you have deployed
                temperature = fltTemperature,
                api_version = "2024-02-01"
            )'''

    # complete and vetted
    def create_chain(self,intLLMAccessory,intRetrieverK,strPromptTemplate):
        """
        # Inputs
        1. intLLMProvider: integer. The preset provider for LLM. For now we just limited to '1' as we will only use Groq provider.
        2. strAPIKey: string. The API key for LLM.
        3. fltTemperature: float. The temperature setting for LLM.
        4. intRetrieverK: integer. The number of retrieved items for embeddings.
        5. intLLMAccessory: int. The preset accessories for LLM. The values are either of the following:
            a. '1': Pass this value to indicate that the RAG-LLM will just have: Context Retriever, and; LLM.
            b. '2': Pass this value to indicate that the RAG-LLM will just have: Chat History Retriever, and; LLM.
            c. '3': Pass this value to indicate that the RAG-LLM will just have: Context Retriever; Chat History Retriever, and; LLM.
            d. '4': Pass this value to indicate that the RAG-LLM will just be on email writing mode (special mode).
        6. strPromptTemplate: string. The template of the prompt.

        # Process
        1. Connects to appropriate LLM provider.

        # Outputs
        1. None, calls self.create_llm() and it is there where objLLM is created. 
        """
        if intLLMAccessory > 0:
            if intLLMAccessory == 1:
                #just context on RAG
                self.objPromptTemplate = PromptTemplate(template = strPromptTemplate, 
                                                        input_variables = ["context", "question"])
                self.objRetrieverContext = self.ingest_context().as_retriever(search_kwargs={"k": intRetrieverK})
                self.objChain = ({"context": self.objRetrieverContext | self.combine_docs, 
                                  "question": RunnablePassthrough()} | 
                                  self.objPromptTemplate | 
                                  self.objLLM)
            elif intLLMAccessory == 2:
                raise ValueError(f"To be added soon Chat History only LLM accessory: {intLLMAccessory}")
            elif intLLMAccessory == 3:
                # Default both context and chat history in RAG
                self.objPromptTemplate = PromptTemplate(
                    template = strPromptTemplate, 
                    input_variables=["context", "question", "chat_history"]
                )
                self.objRetrieverContext = self.ingest_context().as_retriever(search_kwargs={"k": intRetrieverK})
                self.objRetrieverChatHistory = self.ingest_chat_history().as_retriever(search_kwargs={"k": intRetrieverK})
                # Use the updated combine_docs_chat_history function to provide chat history
                self.objChain = ({"context": self.objRetrieverContext | self.combine_docs, 
                                  "chat_history": self.objRetrieverChatHistory | self.combine_docs,  
                                  "question": RunnablePassthrough()} | 
                                  self.objPromptTemplate | 
                                  self.objLLM)
            elif intLLMAccessory == 4:
                # Special chain for email writing
                self.objPromptTemplateEmail = PromptTemplate(
                    template=strPromptTemplate,
                    input_variables=[
                        "strCustomerName",
                        "strFeat1", "strFeatValue1",
                        "strFeat2", "strFeatValue2",
                        "strFeat3", "strFeatValue3",
                        "strEmailTemplate",
                        "strOfferGuide"
                    ]
                )
                self.objRetrieverContext = self.ingest_context().as_retriever(search_kwargs={"k": intRetrieverK})
                # Use the updated combine_docs_chat_history function to provide chat history
                self.objChainEmailComposer = ({"strCustomerName": RunnablePassthrough(),
                                               "strFeat1": RunnablePassthrough(),
                                               "strFeatValue1": RunnablePassthrough(),
                                               "strFeat2": RunnablePassthrough(),
                                               "strFeatValue2": RunnablePassthrough(),
                                               "strFeat3": RunnablePassthrough(),
                                               "strFeatValue3": RunnablePassthrough(),
                                               # Focus retrieval for email template only
                                               "strEmailTemplate": lambda _: self.combine_docs(self.objRetrieverContext.invoke("email template for churn response")),
                                               # Focus retrieval for rention offers only
                                               "strOfferGuide": lambda _: self.combine_docs(self.objRetrieverContext.invoke("retention offer guide for low activity users"))} | 
                                               self.objPromptTemplateEmail | 
                                               self.objLLM
                                            )
            else:
                raise ValueError(f"Invalid LLM Additions: {intLLMAccessory}")
        else:
            self.objPromptTemplate = PromptTemplate(template = strPromptTemplate, 
                                                    input_variables=["question"])
            self.objChain = ({"question": RunnablePassthrough()} | 
                            self.objPromptTemplate | 
                            self.objLLM)
    
    def check_validity_of_settings(self,intLLMAccessory,strPromptTemplate):
        if 'chat_history' in strPromptTemplate and intLLMAccessory in [2,3,4,5]:
            return True
        elif 'chat_history' not in strPromptTemplate and intLLMAccessory not in [2,3,4,5]:
            return True
        else:
            return False

    def combine_docs(self, docs):
        '''
        This method is a sub process for ingesting database for context only RAG chains, triggered by ingest_context()
        '''
        return "\n\n".join(doc.page_content for doc in docs)

    def add_chat_history(self, strUserInput, strLLMOutput):
        '''
        This method does actually adds chat to the history for this LLM, however the chain is regenerated because the history retriever needs to be updated back to the chain.
        '''
        # Update chat history with both User input and System output in the same dictionary
        self.lisChatHistory.append({"Message Index":len(self.lisChatHistory)+1,
                                    "Timestamp": datetime.now(), 
                                    "User": strUserInput, 
                                    "System": strLLMOutput})
        self.create_chain(self.intLLMAccessory,self.intRetrieverK,self.strPromptTemplate)
    
    def add_context(self):
        '''
        This method does actually adds context for this LLM, however the chain is regenerated because the context retriever needs to be updated back to the chain.
        '''
        self.create_chain(self.intLLMAccessory,self.intRetrieverK,self.strPromptTemplate)
  
    def ingest_chat_history(self):
        ########################################################
        #######                                          #######
        #######     Step 1: Write conversation as txt    #######
        #######                                          #######
        ########################################################
        strChatHistoryRawDirectory = os.path.join(self.strIngestPath, 'chat_folder')
        os.makedirs(strChatHistoryRawDirectory, exist_ok=True)
        strChatHistoryFile = os.path.join(strChatHistoryRawDirectory,'chat_history.txt')
        with open(strChatHistoryFile, 'w') as file:
            file.write('Conversation History')
            for dicItem in self.lisChatHistory:
                file.write(f"-----\nMessage Index: {dicItem['Message Index']}; Time: {dicItem['Timestamp']}\nUser: {dicItem['User']}:\n")
                file.write(f"-----\nMessage Index: {dicItem['Message Index']}; Time: {dicItem['Timestamp']}\nSystem: {dicItem['System']}:\n")

        ########################################################
        #######                                          #######
        #######     Step 2: Convert chroma embeddings    #######
        #######                                          #######
        ########################################################
        strChatHistoryKnowledgeDirectory = os.path.join(strChatHistoryRawDirectory, 'chroma_embeddings')
        objLoader = DirectoryLoader(strChatHistoryRawDirectory, glob="**/*.txt", loader_cls=TextLoader, show_progress=False)
        raw_documents = objLoader.load()
        text_splitter = RecursiveCharacterTextSplitter(chunk_size = 1000, chunk_overlap=100)
        documents = text_splitter.split_documents(raw_documents)
        return Chroma.from_documents(documents, self.objEmbeddingModel, persist_directory=strChatHistoryKnowledgeDirectory)

    def ingest_context(self):
        ########################################################
        #######                                          #######
        #######     Step 1: Convert chroma embeddings    #######
        #######                                          #######
        ########################################################
        strContextKnowledgeDirectory = os.path.join(self.strIngestPath,'chroma_embeddings')
        objLoader = DirectoryLoader(self.strIngestPath, glob="**/*.txt", loader_cls=TextLoader, show_progress=False)
        raw_documents = objLoader.load()
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
        documents = text_splitter.split_documents(raw_documents)
        return Chroma.from_documents(documents, self.objEmbeddingModel, persist_directory = strContextKnowledgeDirectory)
         
    def get_response(self, strQuestion,  
                     boolShowSource = False,
                     boolSaveChat = True,):
        """
        # Inputs
            1. strQuestion = a string that is the question you want to pass to the LLM.
            2. boolShowSource = a boolean that is to show the retrieved context or no.
            3. boolSaveChat = a boolean that indicates if the conversation will be recorded for chat history.
        # Process
            1. Asks the RAG Chain the `strQuestion`. 
            2. Creates a dictionary to compile the results.
        # Outputs
            1. Returns a dictionary containing the following:
                1. 'Response': response of llm.
                2. 'Context Used': the context retrieved.
                3. 'Chat History Used': the chat history retrieved.
        """
        ########################################################
        #######                                          #######
        #######            Step 1: Get Response          #######
        #######                                          #######
        ########################################################
        timeStart = time.time()
        strResponse = self.objChain.invoke(strQuestion).content
        ########################################################
        #######                                          #######
        #######         Step 2: Get Contexts Used        #######
        #######                                          #######
        ########################################################
        if boolShowSource:
            strContexts = self.objRetrieverContext.get_relevant_documents(strQuestion)
            strChatHistories = self.objRetrieverChatHistory.get_relevant_documents(strQuestion)
        else:
            strContexts = None
            strChatHistories = None
        if boolSaveChat:
            self.add_chat_history(strQuestion,strResponse)

        timeEnd = time.time()
        return {
            'Response': strResponse,
            'Context Used': strContexts,
            'Chat History Used': strChatHistories,
            'Time To Respond': timeEnd - timeStart
        }

class LLM_Email(GenAIModel): # inherit llm class
    def __init__(self, intLLMProvider:int, 
                 strIngestPath:str, 
                 strPromptTemplate:str, 
                 strAPIKey:str = None, 
                 fltTemperature:float = 0.1, 
                 intRetrieverK :int = 5,
                 intLLMAccessory:int = None,
                 objEmbeddingModel = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")):
        super().__init__(intLLMProvider, strIngestPath, strPromptTemplate, strAPIKey, fltTemperature, intRetrieverK, intLLMAccessory, objEmbeddingModel)
    
    def generate_email(self,
                       strCustomerName:str=None,
                       strFeat1:str=None,
                       fltFeatValue1:float=None,
                       strFeat2:str=None,
                       fltFeatValue2:float=None,
                       strFeat3:str=None,
                       fltFeatValue3:float=None,
                       dicContents=None):
        """
        # Inputs
            1. strCustomerName: string. Name of the customer.
            2. strFeat1: string. Name of first churn-related feature.
            3. fltFeatValue1: float. Value of the first feature.
            4. strFeat2: string. Name of second churn-related feature.
            5. fltFeatValue2: float. Value of the second feature.
            6. strFeat3: string. Name of third churn-related feature.
            7. fltFeatValue3: float. Value of the third feature.
            8. dicContents: dicionary. Preconstructed input dictionary for the chain. Overrides individual parameters if provided.
        # Process
            1. Generate a personalized HTML email based on customer churn features using an LLM with retrieval-augmented generation.
            2. Builds an input dictionary from feature values (if not passed directly).
            3. Passes the dictionary to a custom RAG chain for email generation.
            4. Retrieves the specific context used for email template and retention strategy.
        # Outputs
            1. Returns a dictionary containing the following:
                1. 'Response': The generated HTML email from the LLM.
                2. 'Template_Used': Retrieved documents for the email template.
                3. 'Retention_Used': Retrieved documents for the retention guide.
                4. 'Time_To_Respond': Time it took to generate the response.
        """
        # dicContents should look like this below
        
        timeStart = time.time()
        if strFeat1:
            dicContents = {
                "strCustomerName": strCustomerName,
                "strFeat1": strFeat1,
                "strFeatValue1": fltFeatValue1,
                "strFeat2": strFeat2,
                "strFeatValue2": fltFeatValue2,
                "strFeat3": strFeat3,
                "strFeatValue3": fltFeatValue3,
            }
        strResponse = self.objChainEmailComposer.invoke(dicContents).content
        strEmailTemplate = self.objRetrieverContext.get_relevant_documents("email template for churn response")
        strRetentionOffer = self.objRetrieverContext.get_relevant_documents("retention offer guide for low activity users")
        timeEnd = time.time()
        return {
            'Response': strResponse,
            'Template_Used': strEmailTemplate,
            'Retention_Used': strRetentionOffer,
            'Time_To_Respond': timeEnd - timeStart
        }