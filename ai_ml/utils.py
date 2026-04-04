from sklearn.pipeline import Pipeline
from datetime import datetime
import joblib

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
    strArtifactType = strArtifactType.lower()
    if strArtifactType == 1 or strArtifactType == "transformer":
        strFileName = "transformer"
    elif strArtifactType == 2 or strArtifactType == "estimator":
        strFileName = "estimator"
    else:
        raise Exception(f"[[create_artifact_name]] 😱 Error strArtifactType value: `{strArtifactType}`. Must only be 1 or 2, or 'transformer' or 'estimator'.")

    strFileName = strFileName + "_" + str(dateCreated)
    return strFileName
