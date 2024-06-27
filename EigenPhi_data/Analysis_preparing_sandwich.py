import pandas as pd
import os

# Define the range of file names
file_range = range(17, 26)
file_prefix = "/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/EigenPhi_data/BSC_Sandwich_data_"
file_suffix = ".csv"

# Create a list to hold the dataframes
dataframes = []

# Load each CSV file into a dataframe and append it to the list
for i in file_range:
    file_name = f"{file_prefix}{i}{file_suffix}"
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        dataframes.append(df)
    else:
        print(f"File {file_name} does not exist")

# Combine all dataframes into a single dataframe
combined_df = pd.concat(dataframes, ignore_index=True)

# Display the combined dataframe
combined_df = combined_df.drop(columns=['Token','Tx'])
combined_df = combined_df.dropna(subset=['Profit'])

combined_df = combined_df.drop_duplicates(subset=['Transaction Hash URL'])
# for those with small values
Tx_smallAmount = combined_df[(combined_df['Profit']=='<$0.01') | (combined_df['Cost']=='<$0.01')| (combined_df['Revenue']=='<$0.01') | (combined_df['Profit']=='<-$0.01') | (combined_df['Cost']=='<-$0.01')| (combined_df['Revenue']=='<-$0.01') ]
filtered_df = combined_df[~(((combined_df['Profit']=='<$0.01') | (combined_df['Cost']=='<$0.01'))| (combined_df['Revenue']=='<$0.01') | (combined_df['Profit']=='<-$0.01') | (combined_df['Cost']=='<-$0.01')| (combined_df['Revenue']=='<-$0.01') )]
# calculate profit margin
filtered_df['Profit'] = filtered_df['Profit'].str.replace('$', '')
filtered_df['Revenue'] = filtered_df['Revenue'].str.replace('$', '')
filtered_df['Cost'] = filtered_df['Cost'].str.replace('$', '')
filtered_df['Profit'] = filtered_df['Profit'].str.replace(',', '').astype(float)
filtered_df['Revenue'] = filtered_df['Revenue'].str.replace(',', '').astype(float)
filtered_df['Cost'] = filtered_df['Cost'].str.replace(',', '').astype(float)

filtered_df['Profit_margin'] = filtered_df['Profit'] / filtered_df['Revenue']
filtered_df['Profit_margin'] = filtered_df.apply(lambda row: -row['Profit_margin'] if pd.to_numeric(row['Revenue'], errors='coerce') < 0 else row['Profit_margin'], axis=1)
# Convert 'datetime' column to datetime type
filtered_df['Time'] = pd.to_datetime(filtered_df['Time'])
# Extract day from datetime and create a new column 'day'
filtered_df['day'] = filtered_df['Time'].dt.date
filtered_df.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/EigenPhi_data/Sandwich_profitMargin.csv')
Tx_smallAmount.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/EigenPhi_data/Sandwich_smallAmount.csv')
