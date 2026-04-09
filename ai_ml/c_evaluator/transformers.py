from sklearn.base import BaseEstimator, TransformerMixin
import shap
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
from sklearn.pipeline import Pipeline

class SHAP_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, objEstimator, intTopFeats:int = 5, boolVerbose: bool = True):
        self.objEstimator = objEstimator
        self.intTopFeats = intTopFeats
        self.boolVerbose = boolVerbose
        self.objExplainer = None

    def fit(self, X, y=None):
        if isinstance(self.objEstimator, LinearRegression):
            self.objExplainer = shap.LinearExplainer(self.objEstimator)
        elif isinstance(self.objEstimator, LogisticRegression):
            self.objExplainer = shap.LinearExplainer(self.objEstimator)
        elif isinstance(self.objEstimator, RandomForestClassifier):
            self.objExplainer = shap.TreeExplainer(self.objEstimator)
        else:
            raise Exception(f"[[SHAP_Transformer]] 😱 Error self.objEstimator model type: `{self.objEstimator}`. Must only be LinearRegression, LogisticRegression, RandomForestClassifier.")
        if self.boolVerbose:
            print(f"[[SHAP_Transformer]] 🪵 Successfully loaded SHAP explainer.")
        return self

    def _get_positive_shap_values(self, X):
        objShapValues = self.objExplainer.shap_values(X)

        if isinstance(objShapValues, list):
            np2DPositiveShapValues = np.asarray(objShapValues[-1])
        else:
            np2DPositiveShapValues = np.asarray(objShapValues)

            if np2DPositiveShapValues.ndim == 3:
                if np2DPositiveShapValues.shape[0] == len(X):
                    np2DPositiveShapValues = np2DPositiveShapValues[:, :, -1]
                else:
                    np2DPositiveShapValues = np2DPositiveShapValues[-1]

        if np2DPositiveShapValues.ndim == 1:
            np2DPositiveShapValues = np2DPositiveShapValues.reshape(1, -1)

        if np2DPositiveShapValues.ndim != 2:
            raise ValueError(
                f"[[SHAP_Transformer]] Unexpected SHAP output shape: `{np2DPositiveShapValues.shape}`."
            )

        return np2DPositiveShapValues

    def transform(self, X):
        # np3DShapValues look like this:
        # [
        #   [ # line item 1
        #     [0.000105, -0.000105] # feature 1
        #     [0.003062, -0.003062] # feature 2
        #     [0.011599, -0.011599] # feature 3
        #     [0.005208, -0.005208] # feature 4
        #   ]
        #   [ # line item 2
        #     [0.000105, -0.000105] # feature 1
        #     [0.003062, -0.003062] # feature 2
        #     [0.011599, -0.011599] # feature 3
        #     [0.005208, -0.005208] # feature 4
        #   ]
        # ]
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X)

        np2DPositiveShapValues = self._get_positive_shap_values(X)
        np1DPredictions = np.asarray(self.objEstimator.predict(X))
        np2DPredProba = (
            self.objEstimator.predict_proba(X)
            if hasattr(self.objEstimator, "predict_proba")
            else None
        )
        arrFeatureNames = X.columns.to_numpy()
        np2DXValues = X.to_numpy()
        np1DFeatureIndex = np.arange(X.shape[1])
        intTopFeatCount = min(self.intTopFeats, X.shape[1])
        lisNewPredictionRow = []

        for intIndexRow in range(len(X)):
            objPrediction = np1DPredictions[intIndexRow]
            dicNewPredictionRow = {
                "Prediction": objPrediction.item() if hasattr(objPrediction, "item") else objPrediction
            }

            if np2DPredProba is not None:
                dicNewPredictionRow["Churn_Probability"] = float(np2DPredProba[intIndexRow][1])

            np1DShapSpecific = np2DPositiveShapValues[intIndexRow]
            np2DSHAPSpecific = np.column_stack(
                (
                    np1DFeatureIndex,
                    -np1DShapSpecific,
                    np1DShapSpecific,
                )
            )

            if self.boolVerbose:
                print('check SHAP array here:')
                for arrArray in np2DSHAPSpecific:
                    print('-----')
                    print(arrArray)

            if intTopFeatCount > 0:
                np1DTopFeatureIndices = np.argpartition(
                    -np1DShapSpecific,
                    intTopFeatCount - 1
                )[:intTopFeatCount]
                np1DTopFeatureIndices = np1DTopFeatureIndices[
                    np.argsort(-np1DShapSpecific[np1DTopFeatureIndices])
                ]

                for intIndexFeature, intIndexFeatureForX in enumerate(np1DTopFeatureIndices):
                    strFeatureName = arrFeatureNames[intIndexFeatureForX]
                    dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat"] = strFeatureName
                    dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat_Value"] = np2DXValues[intIndexRow, intIndexFeatureForX]
                    dicNewPredictionRow[f"Top_{intIndexFeature+1}_Feat_Score"] = np1DShapSpecific[intIndexFeatureForX]

            lisNewPredictionRow.append(dicNewPredictionRow)

        return pd.DataFrame(lisNewPredictionRow)

def get_top_feats(X, pipeModel: Pipeline):
    """
    # Inputs
    1. X: DataFrame. Feature set.
    2. pipeModel: Pipeline. Trained pipeline (transformer + model)

    # Output
    1. Dict of feature importance (sorted descending)
    """

    # 1. Split pipeline
    pipeTransformer = pipeModel[:-1]
    pipeEstimator   = pipeModel.named_steps["model"]

    # 2. Transform input
    X_t = pipeTransformer.transform(X)

    if isinstance(X_t, pd.DataFrame):
        if len(X_t) > 1000:
            X_t = X_t.sample(n=1000, random_state=42)
    elif X_t.shape[0] > 1000:
        objRandom = np.random.default_rng(42)
        npSampleIndex = objRandom.choice(X_t.shape[0], size=1000, replace=False)
        X_t = X_t[npSampleIndex]

    # 3. Get feature names from the transformed frame when available.
    if isinstance(X_t, pd.DataFrame):
        feature_names = [str(strFeatureName) for strFeatureName in X_t.columns]
    else:
        try:
            feature_names = [
                str(strFeatureName)
                for strFeatureName in pipeTransformer.get_feature_names_out()
            ]
        except Exception:
            feature_names = [f"feat_{i}" for i in range(X_t.shape[1])]

    # 4. SHAP
    objExplainer = shap.Explainer(pipeEstimator, X_t)
    shap_values = objExplainer(X_t)

    # 5. Handle shape (classifier vs regressor)
    values = shap_values.values
    if values.ndim == 3:  # (n_samples, n_features, n_classes)
        values = values[..., 1]  # take positive class

    # 6. Global importance
    importance = np.abs(values).mean(axis=0)

    # 7. Return sorted dict
    return dict(sorted(
        zip(feature_names, importance),
        key=lambda x: x[1],
        reverse=True
    ))

def get_all_evals(X,y,pipeModel):
    """
    # Inputs
    1. `X`: Dataframe. Feature set.
    2. `y`: Dataframe. Target set.
    3. `pipeModel`: Pipeline. A trained pipeline containing both transformers and estimators.

    # Process
    1. Splits train and test sets.
    2. Gets best features using SHAP
    3. Gets other eval metrics:
        - Accuracy
        - Precision
        - Recall
        - F1
        - Confusion Matrix

    # Output
    1. Returns a dictionary:
        {
            "intCountTrainPositiveClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 1)),
            "intCountTrainNegativeClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 0)),
            "intCountTestPositiveClass": int(np.count_nonzero(npYTest == 1)),
            "intCountTestNegativeClass": int(np.count_nonzero(npYTest == 0)),
            "fltAccuracy": float(accuracy_score(npYTest, npYPred)),
            "fltPrecision": float(precision_score(npYTest, npYPred, zero_division=0)),
            "fltRecall": float(recall_score(npYTest, npYPred, zero_division=0)),
            "fltF1": float(f1_score(npYTest, npYPred, zero_division=0)),
            "objConfusionMatrix": objConfusionMatrix,
            "top_feats": dicTopFeats
        }
    """

    # Step 1: Get splits
    XTrain, XTest, yTrain, yTest = train_test_split(
        X,
        y,
        test_size=0.2,
        shuffle=False,
    )

    # Step 2: Get Top Feats
    dicFeats = get_top_feats(
        X = X,
        pipeModel = pipeModel
    )

    # Step 3: Get Evals
    npYTest = np.asarray(yTest, dtype=int)
    npYPred = np.asarray(pipeModel.predict(XTest))

    if npYPred.dtype.kind not in {"i", "u", "b"}:
        npYPred = (npYPred >= 0.5).astype(int)
    else:
        npYPred = npYPred.astype(int, copy=False)

    objConfusionMatrix = confusion_matrix(
        npYTest,
        npYPred,
        labels=[0, 1],
    )

    dicResults = {
        "intCountTrainPositiveClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 1)),
        "intCountTrainNegativeClass": int(np.count_nonzero(np.asarray(yTrain, dtype=int) == 0)),
        "intCountTestPositiveClass": int(np.count_nonzero(npYTest == 1)),
        "intCountTestNegativeClass": int(np.count_nonzero(npYTest == 0)),
        "fltAccuracy": float(accuracy_score(npYTest, npYPred)),
        "fltPrecision": float(precision_score(npYTest, npYPred, zero_division=0)),
        "fltRecall": float(recall_score(npYTest, npYPred, zero_division=0)),
        "fltF1": float(f1_score(npYTest, npYPred, zero_division=0)),
        "objConfusionMatrix": objConfusionMatrix,
        "dicFeats": dicFeats
    }
    return dicResults
