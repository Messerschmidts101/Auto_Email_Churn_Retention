import os
from datetime import datetime

ARTIFACTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
)

def create_artifact_name(strArtifactType: str):
    """
    Unifies/standardized the naming convention of artifacts.

    # Input
    1. `strArtifactType`: String. The type of the artifact being saved. Onluy accepts 2 values:
        - `1` or `Transformer`: indicates the artifact is a feateng pipeline.
        - `2` or `Estimator`: indicates the artifact is a model.
    # Output
    1. Returns a string name to use for joblib.dump()
    """

    dateCreated = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifactType = strArtifactType.lower() if isinstance(strArtifactType, str) else strArtifactType
    if artifactType == 1 or artifactType == "transformer":
        strFileName = "transformer"
    elif artifactType == 2 or artifactType == "estimator":
        strFileName = "estimator"
    elif artifactType == 3 or artifactType == "churn_model":
        strFileName = "churn_model"
    else:
        raise Exception(f"[[create_artifact_name]] 😱 Error strArtifactType value: `{strArtifactType}`. Must only be 1 or 2, or 'transformer' or 'estimator'.")

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    strFileName = os.path.join(ARTIFACTS_DIR, strFileName + "_" + str(dateCreated))
    return strFileName
