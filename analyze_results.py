import pandas as pd
import os
import ast

run_id = '76a911f2-4cfb-48ca-813d-4231057b3329'
clean_file = f'user_output/final_output_{run_id}.csv'
log_file = f'user_output/pipeline_log_{run_id}.csv'

print(f'--- Final Verification: Employee Run {run_id} ---')

if os.path.exists(clean_file):
    df = pd.read_csv(clean_file)
    cols = list(df.columns)
    print(f'First 5 Columns: {cols[:5]}')
    
    # Check if internal metadata is gone
    metadata_keys = ['run_id', 'status', 'confidence_score', 'failure_reason']
    found_metadata = [k for k in metadata_keys if k in df.columns]
    print(f'Internal Metadata Found in Clean CSV: {found_metadata}')
    
    # Check priority
    print(f'Priority Check: {cols[:3]}')
else:
    print('Clean file not found')

if os.path.exists(log_file):
    log = pd.read_csv(log_file)
    enrichment = log[log['action'] == 'ROW_ENRICHED']
    print(f'Total Smart Enrichments: {len(enrichment)}')
    
    p2_resolved = log[log['action'] == 'ROW_RESOLVED']
    print(f'AI Resolved Rows: {len(p2_resolved)}')
