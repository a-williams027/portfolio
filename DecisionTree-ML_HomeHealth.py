# Import Data
import pandas as pd
import numpy as np
data = pd.read_excel('/Users/amandawilliams/Desktop/AW Grad School/BAN - Predictive Analytics - S24/MCR_HomeHlth.xlsx')
# Understanding dataset
print(data.describe())
print(data.info())

# Convert 'region' column to string
data['region'] = data['region'].astype(str)
# Convert 'Provider_ID' column to string
data['Provider_ID'] = data['Provider_ID'].astype(str)

# Check for missing values
print(data.isnull().sum())

# checking for outliers
# Basic statistics
columns_to_check = ['TotalEpisNonLUPA', 'DistBenNonLUPA']
print(data[columns_to_check].describe())
# Visualize box plots for outlier detection
import matplotlib.pyplot as plt
from pandas.plotting import scatter_matrix
plt.figure(figsize=(12, 6))
for i, column in enumerate(columns_to_check, 1):
    plt.subplot(1, len(columns_to_check), i)
    plt.boxplot(data[column])
    plt.title(f'Boxplot for {column}')
plt.show()

# Calculate the Z-score for 'TotalEpisNonLUPA'
data['TotalEpisNonLUPA_Zscore'] = (data['TotalEpisNonLUPA'] - data['TotalEpisNonLUPA'].mean()) / data['TotalEpisNonLUPA'].std()

# Calculate the Z-score for 'DistBenNonLUPA'
data['DistBenNonLUPA_Zscore'] = (data['DistBenNonLUPA'] - data['DistBenNonLUPA'].mean()) / data['DistBenNonLUPA'].std()

# Define the threshold for outliers (e.g., Z-score greater than 3 or less than -3)
threshold = 3

# Filter out data points beyond the threshold for 'TotalEpisNonLUPA'
filtered_data = data[(data['TotalEpisNonLUPA_Zscore'] < threshold) & (data['TotalEpisNonLUPA_Zscore'] > -threshold)]

# Filter out data points beyond the threshold for 'DistBenNonLUPA'
filtered_data = filtered_data[(filtered_data['DistBenNonLUPA_Zscore'] < threshold) & (filtered_data['DistBenNonLUPA_Zscore'] > -threshold)]

# Remove the 'Z_score' columns if no longer needed
filtered_data.drop(['TotalEpisNonLUPA_Zscore', 'DistBenNonLUPA_Zscore'], axis=1, inplace=True)


# Visualize box plots for outlier detection for both DistBenNonLUPA and TotalEpisNonLUPA after outlier removal for DistBenNonLUPA
plt.figure(figsize=(12, 6))
# Box plot for DistBenNonLUPA after outlier removal
plt.subplot(1, 2, 1)
plt.boxplot(filtered_data['DistBenNonLUPA'])
plt.title('Boxplot for DistBenNonLUPA after Outlier Removal')
plt.xlabel('DistBenNonLUPA')
plt.grid(True)
# Box plot for TotalEpisNonLUPA
plt.subplot(1, 2, 2)
plt.boxplot(filtered_data['TotalEpisNonLUPA'])
plt.title('Boxplot for TotalEpisNonLUPA')
plt.xlabel('TotalEpisNonLUPA')
plt.grid(True)
plt.tight_layout()
plt.show()
print(filtered_data.describe())

# Count the occurrences of each unique value in 'profit_b' column
profit_b_counts = filtered_data['profit_b'].value_counts()

# Create a bar plot for the counts
plt.figure(figsize=(8, 6))
profit_b_counts.plot(kind='bar', color=['skyblue', 'salmon'])
plt.title('Frequency of Binary Target Variable (Profit_b)')
plt.xlabel('Profit_b')
plt.ylabel('Count')
plt.xticks(rotation=0)  # Rotate x-axis labels if needed
plt.grid(axis='y')      # Show gridlines on y-axis only
plt.show()

from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder

#filter data
filtered_data.drop(columns=['Agency_Name'], inplace=True)
# Encode the 'State' column using one-hot encoding
filtered_data = pd.get_dummies(filtered_data, columns=['State'])

# Define features (X) and target variable (y)
X = filtered_data.drop('profit_b', axis=1)  # Features
y = filtered_data['profit_b']  # Target variable

# Split the data into training and validation sets (70% train, 30% validation)
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.3, random_state=12345)

# Train decision tree
clf = DecisionTreeClassifier(random_state=12345)
clf.fit(X_train, y_train)

# Evaluate performance on validation set
val_accuracy = accuracy_score(y_val, clf.predict(X_val))
print("Validation Accuracy (before pruning):", val_accuracy)

# Prune the tree
pruned_clf = DecisionTreeClassifier(max_depth=2, random_state=12345)
pruned_clf.fit(X_train, y_train)

# Evaluate pruned tree performance on validation set
pruned_val_accuracy = accuracy_score(y_val, pruned_clf.predict(X_val))
print("Validation Accuracy (after pruning):", pruned_val_accuracy)

# Display classification report and confusion matrix for pruned tree
y_pred = pruned_clf.predict(X_val)
print("\nClassification Report:")
print(classification_report(y_val, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_val, y_pred))

# Calculate True Positive (TP), False Positive (FP), True Negative (TN), False Negative (FN)
TP = confusion_matrix(y_val, y_pred)[1, 1]
FP = confusion_matrix(y_val, y_pred)[0, 1]
TN = confusion_matrix(y_val, y_pred)[0, 0]
FN = confusion_matrix(y_val, y_pred)[1, 0]

# Calculate lift
lift = (TP / (TP + FP)) / ((TP + FN) / (TP + FP + TN + FN))

# Calculate gain
cumulative_TP = TP
total_positives = TP + FN
gain = cumulative_TP / total_positives

print("Lift:", lift)
print("Gain:", gain)

from sklearn.tree import plot_tree
plt.figure(figsize=(20,10))
plot_tree(clf, filled=True, feature_names=X.columns, class_names=['Not Above Avg Profit', 'Above Avg Profit'])
plt.show()
