import pandas as pd
import numpy as np
# Load your data
df = pd.read_csv("model\dataset\source.csv")

np.random.seed(42)

# Base satisfaction by NumOfProducts
base_score = df['NumOfProducts'].apply(lambda x: 2.0 + 0.7 * x)

# Penalize churned customers by subtracting 0.8 from base score (tunable)
base_score[df['Exited'] == 1] -= 0.8

# Add Gaussian noise
noise = np.random.normal(loc=0, scale=0.8, size=len(df))

# Combine, round, and clip
satisfaction_score = np.clip(np.round(base_score + noise), 1, 5)

df['RecentSatisfactionScore'] = satisfaction_score.astype(int)

df.to_csv("churn_bank_with_satisfaction.csv", index=False)