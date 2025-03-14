import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set the style for the plots
sns.set(style="whitegrid")

# Query 2: Top 5 Districts with the Highest Number of Accidents
query2_df = pd.read_csv('/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query2_Results.csv')
# Plotting the top 5 districts
plt.figure(figsize=(10, 6))
sns.barplot(x='Num_Accidents', y='Local_Authority_District', data=query2_df, palette='viridis')
plt.title('Top 5 Districts with the Highest Number of Accidents')
plt.xlabel('Number of Accidents')
plt.ylabel('District')
plt.tight_layout()
plt.show()

# Query 3: Average Speed Limit and Total Number of Accidents by Road Type
query3_df = pd.read_csv('/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query3_Results.csv')
# Plotting average speed limit by road type
plt.figure(figsize=(10, 6))
sns.barplot(x='Road_Type', y='Avg_Speed_Limit', data=query3_df, palette='coolwarm')
plt.title('Average Speed Limit by Road Type')
plt.xlabel('Road Type')
plt.ylabel('Average Speed Limit')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Query 4: Total Number of Casualties by Year
query4_df = pd.read_csv('/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query4_Results.csv')
# Plotting total casualties by year
plt.figure(figsize=(10, 6))
sns.lineplot(x='Year', y='Total_Casualties', data=query4_df, marker='o', color='b')
plt.title('Total Number of Casualties by Year')
plt.xlabel('Year')
plt.ylabel('Total Casualties')
plt.tight_layout()
plt.show()

# Query 5: Top 5 Vehicle Types with Highest Number of Casualties
query5_df = pd.read_csv('/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query5_Results.csv')
# Plotting the top 5 vehicle types with the highest number of casualties as a pie chart
plt.figure(figsize=(8, 8))
# Pie chart for Total Casualties by Vehicle Type
plt.pie(query5_df['Total_Casualties'], labels=query5_df['Vehicle_Type'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('magma', len(query5_df)))
plt.title('Top 5 Vehicle Types with Highest Number of Casualties')
plt.axis('equal')  # Equal aspect ratio ensures the pie chart is circular.
plt.tight_layout()
plt.show()

# Query 6:
# Load the dataset
file_path = "/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query_Time-of-day_Results.csv"
query6_df = pd.read_csv(file_path)

# Ensure correct column names
query6_df.columns = query6_df.columns.str.strip()

# Define colors for severity levels
severity_colors = {"Fatal": "red", "Serious": "orange", "Slight": "blue"}

# Plot the line chart with color differentiation
plt.figure(figsize=(10, 6))
for severity, color in severity_colors.items():
    subset = query6_df[query6_df['Accident_Severity'] == severity]
    plt.plot(subset['Hour'], subset['Num_Accidents'], marker='o', label=severity, color=color)

plt.xlabel("Time of Day")
plt.ylabel("Number of Accidents")
plt.title("Accident Severity Over Different Time Periods")
plt.legend()
plt.xticks(rotation=30)
plt.grid(True)
plt.show()

#Query 6: Heatmap
import seaborn as sns


# Load the dataset
df_heatmap = pd.read_csv(file_path)

# Ensure correct column names (adjust based on actual CSV structure)
df_heatmap.columns = df_heatmap.columns.str.strip()

#Only show Fatal and Serious Accidents
df_heatmap= df_heatmap[df_heatmap['Accident_Severity'] == 'Fatal']

# Pivot the data to create a heatmap structure
heatmap_data = df_heatmap.pivot(index="Accident_Severity", columns="Hour", values="Num_Accidents")

# Plot heatmap
plt.figure(figsize=(12, 6))
sns.heatmap(heatmap_data, cmap="Reds", annot=False, linewidths=0.5)
plt.xlabel("Hour of the Day")
plt.ylabel("Fatal Accidents")
plt.title("Heatmap of Accident Frequency by Time of Day")
plt.show()


# Line Chart - Seasonality Trends
# Load the dataset
file_path = "/Users/amandawilliams/Desktop/ProjectDataFiles/UpdatedProjectFolder/Query_Seasonal-Trends2_Results.csv"
data = pd.read_csv(file_path)

# Plotting the data
plt.figure(figsize=(12, 6))

# Loop through years and plot accidents by month
for year in data['accident_year'].unique():
    year_data = data[data['accident_year'] == year]
    plt.plot(year_data['accident_month'], year_data['total_accidents'], label=str(year))

# Formatting the plot
plt.title('Accidents by Month and Year')
plt.xlabel('Month/Year')
plt.ylabel('Total Accidents')
plt.xticks(rotation=45)
# Move the legend to the right side of the plot
plt.legend(title='Year', loc='upper left', bbox_to_anchor=(1.05, 1), borderaxespad=0.)
# Adjust layout to make room for the legend
plt.tight_layout()

# Show the plot
plt.show()

