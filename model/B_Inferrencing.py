import os
# Point to your actual Python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join('venv','Scripts','python.exe')
os.environ['PYSPARK_PYTHON'] = os.path.join('venv','Scripts','python.exe')
# Your Java and Hadoop setup
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-11"
os.environ['HADOOP_HOME'] = "C:/Program Files/Hadoop"
# Load the pipeline (if saved via pickle)

import pickle
with open('temp_pipeline_preprocess.pkl', 'rb') as f:
    pipeline = pickle.load(f)

# Get the Random Forest model from the pipeline
rf_model = pipeline.named_steps['Random_Forest']

# Get feature names after preprocessing
encoder = pipeline.named_steps['Encoder']
feat_names = encoder.lisstrColNames   # assuming your custom Encoder_Transformer saves final col names here

# Get feature importances
importances = rf_model.feature_importances_

# Match names to importances
feat_importance = list(zip(feat_names, importances))

# Sort and get top 5
top_5 = sorted(feat_importance, key=lambda x: x[1], reverse=True)[:5]

# Print nicely
for i, (feat, imp) in enumerate(top_5, 1):
    print(f"{i}. {feat}: {imp:.4f}")
