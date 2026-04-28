import numpy as np
import pandas as pd
import shap
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.pipeline import Pipeline


class SHAP_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self, objEstimator, intTopFeats: int = 5, boolVerbose: bool = True):
        self.objEstimator = objEstimator
        self.intTopFeats = intTopFeats
        self.boolVerbose = boolVerbose
        self.objExplainer = None

    def fit(self, X, y=None):
        if isinstance(X, pd.DataFrame) and len(X) > 1000:
            X = X.sample(n=1000, random_state=42)
        elif X is not None and hasattr(X, "shape") and X.shape[0] > 1000:
            objRandom = np.random.default_rng(42)
            npSampleIndex = objRandom.choice(X.shape[0], size=1000, replace=False)
            X = X[npSampleIndex]

        if isinstance(self.objEstimator, LinearRegression):
            if X is None:
                raise ValueError("[[SHAP_Transformer]] LinearRegression requires background data for SHAP.")
            self.objExplainer = shap.LinearExplainer(self.objEstimator, X)
        elif isinstance(self.objEstimator, LogisticRegression):
            if X is None:
                raise ValueError("[[SHAP_Transformer]] LogisticRegression requires background data for SHAP.")
            self.objExplainer = shap.LinearExplainer(self.objEstimator, X)
        elif isinstance(self.objEstimator, RandomForestClassifier):
            self.objExplainer = shap.TreeExplainer(self.objEstimator)
        else:
            raise TypeError(
                "[[SHAP_Transformer]] Unsupported estimator type. "
                "Must be LinearRegression, LogisticRegression, or RandomForestClassifier."
            )

        if self.boolVerbose:
            print("[[SHAP_Transformer]] Successfully loaded SHAP explainer.")
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
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X)

        np2DPositiveShapValues = self._get_positive_shap_values(X)
        np1DRawPredictions = np.asarray(self.objEstimator.predict(X))
        np2DPredProba = (
            self.objEstimator.predict_proba(X)
            if hasattr(self.objEstimator, "predict_proba")
            else None
        )
        if np2DPredProba is not None:
            np1DProbability = np.asarray(np2DPredProba[:, 1], dtype=float)
        else:
            np1DProbability = np.clip(np.asarray(np1DRawPredictions, dtype=float), 0.0, 1.0)

        if np1DRawPredictions.dtype.kind not in {"i", "u", "b"}:
            np1DPredictions = (np1DRawPredictions >= 0.5).astype(int)
        else:
            np1DPredictions = np1DRawPredictions.astype(int, copy=False)

        arrFeatureNames = X.columns.to_numpy()
        np2DXValues = X.to_numpy()
        np1DFeatureIndex = np.arange(X.shape[1])
        intTopFeatCount = min(self.intTopFeats, X.shape[1])
        lisNewPredictionRow = []

        for intIndexRow in range(len(X)):
            objPrediction = np1DPredictions[intIndexRow]
            dicNewPredictionRow = {
                "Prediction": objPrediction.item() if hasattr(objPrediction, "item") else objPrediction,
                "Churn_Probability": float(np1DProbability[intIndexRow]),
            }

            np1DShapSpecific = np2DPositiveShapValues[intIndexRow]

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

    pipeTransformer = pipeModel[:-1]
    pipeEstimator = pipeModel.named_steps["model"]

    X_train = pipeTransformer.transform(X)

    if isinstance(X_train, pd.DataFrame):
        if len(X_train) > 1000:
            X_train = X_train.sample(n=1000, random_state=42)
    elif X_train.shape[0] > 1000:
        objRandom = np.random.default_rng(42)
        npSampleIndex = objRandom.choice(X_train.shape[0], size=1000, replace=False)
        X_train = X_train[npSampleIndex]

    if isinstance(X_train, pd.DataFrame):
        feature_names = [str(strFeatureName) for strFeatureName in X_train.columns]
    else:
        try:
            feature_names = [
                str(strFeatureName)
                for strFeatureName in pipeTransformer.get_feature_names_out()
            ]
        except Exception:
            feature_names = [f"feat_{i}" for i in range(X_train.shape[1])]

    importance = None
    if hasattr(pipeEstimator, "feature_importances_"):
        importance = np.asarray(pipeEstimator.feature_importances_, dtype=float)
    elif hasattr(pipeEstimator, "coef_"):
        importance = np.asarray(pipeEstimator.coef_, dtype=float)
        if importance.ndim > 1:
            importance = np.abs(importance).mean(axis=0)
        else:
            importance = np.abs(importance)

    if importance is None:
        objExplainer = shap.Explainer(pipeEstimator, X_train)
        shap_values = objExplainer(X_train)
        values = shap_values.values
        if values.ndim == 3:
            values = values[..., 1]
        importance = np.abs(values).mean(axis=0)

    return dict(sorted(
        zip(feature_names, importance),
        key=lambda x: x[1],
        reverse=True
    ))


def get_all_evals(
        X_trainrain,
        X_trainest,
        y_train,
        y_test,
        pipeModel):
    """
    # Output
    Returns train/test counts, metrics, confusion matrix, and top features.
    """

    dicFeats = get_top_feats(
        X=X_trainrain,
        pipeModel=pipeModel
    )

    npYTest = np.asarray(y_test, dtype=int)
    npYPred = np.asarray(pipeModel.predict(X_trainest))

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
        "intCountTrainPositiveClass": int(np.count_nonzero(np.asarray(y_train, dtype=int) == 1)),
        "intCountTrainNegativeClass": int(np.count_nonzero(np.asarray(y_train, dtype=int) == 0)),
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
