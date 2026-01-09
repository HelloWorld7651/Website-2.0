import pandas as pd

# Read the first file 
df1 = pd.read_csv('2024.csv')

# Read the second file
df2 = pd.read_csv('g_claims_2024.tsv', sep='\t')

# Merge on patent identifiers 
merged = pd.merge(
    df1,
    df2,
    left_on='patent_number',
    right_on='patent_id',
    how='inner'
)

# Select and reorder the required columns
output_columns = [
    'patent_id', 
    'claim_sequence',
    'claim_text',
    'cpc_sections',
    'ipc_sections',
    'assignee',
    'wipo_field_ids',
    'first_wipo_field_title',
    'first_wipo_sector_title'
]

# Create output DataFrame with exact column order
output_df = merged[['patent_number', 'cpc_sections', 'ipc_sections', 'assignee', 'wipo_field_ids', 
                   'first_wipo_field_title', 'first_wipo_sector_title', 'claim_sequence', 'claim_text']]

# Rename patent_number to patent_id
output_df = output_df.rename(columns={'patent_number': 'patent_id'})

# Reorder columns to match output requirements
output_df = output_df[output_columns]

# Save to new CSV
output_df.to_csv('combined.csv', index=False)

print(f"Successfully created 'combined.csv' with {len(output_df)} rows")