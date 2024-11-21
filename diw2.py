############################################
# Append all diw datas
############################################

import os
import pandas as pd
from datetime import datetime

start_time = datetime.now()

# Replace "path/to/your/directory" with the actual path
diw_path = "./diw"

# Get list of entries in the directory
entries = os.listdir(diw_path)

# Filter for directories (optional)
folders = [entry for entry in entries if os.path.isdir(os.path.join(diw_path, entry))]

dfs = []

output_path_name = 'all'
df_output_path = f"./diw/{output_path_name}"
file_output_name = 'diw_client.xlsx'

for folder in folders:
    diw_folder_path = os.path.join(diw_path, folder)
    diw_datas = os.listdir(diw_folder_path)
    for data in diw_datas:
        if data != output_path_name:
            diw_data_path = os.path.join(diw_folder_path, data)
            df = pd.read_excel(diw_data_path)
            df = df.iloc[:-2,:]
            df['Area'] = [folder]*len(df)
            dfs.append(df)

df_concat = pd.concat(dfs,axis=0)

# Create the directory if it doesn't exist
os.makedirs(df_output_path, exist_ok=True)  # Handles existing directories gracefully

# Save the DataFrame to an XLS file
df_concat.to_excel(os.path.join(df_output_path, file_output_name), index=False)

end_time = datetime.now()
diff_time = end_time - start_time
print(diff_time)