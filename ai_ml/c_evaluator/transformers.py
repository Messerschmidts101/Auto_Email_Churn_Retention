from sklearn.base import BaseEstimator, TransformerMixin
import shap
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
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
