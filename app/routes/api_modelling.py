from fastapi import APIRouter
#from schemas_response.schemas_common import objChatMessage, objChatConversation
#from agents.agent_chat import LLM_Chat
#from routes.api_admin import ingest_kb
router = APIRouter(
    prefix="/train",
    tags=["train", "modelling"]
)

@router.post(
    "/upload",
    summary="Step 1: Upload training data",
    description=(
        " "
    ),
)
def run():
    pass

@router.post(
    "/model",
    summary="Step 2: Start modelling",
    description=(
        " "
    ),
)
def run():
    pass