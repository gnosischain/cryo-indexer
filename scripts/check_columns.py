#!/usr/bin/env python3

import subprocess
import os
import sys

# Define what columns your migration tables expect
MIGRATION_COLUMNS = {
    'blocks': [
        'block_number', 'block_hash', 'author', 'gas_used', 'gas_limit',
        'extra_data', 'timestamp', 'base_fee_per_gas', 'chain_id'
    ],
    'transactions': [
        'block_number', 'transaction_index', 'transaction_hash', 'nonce',
        'from_address', 'to_address', 'value_binary', 'value_string',
        'value_f64', 'input', 'gas_limit', 'gas_used', 'gas_price',
        'transaction_type', 'max_priority_fee_per_gas', 'max_fee_per_gas',
        'success', 'n_input_bytes', 'n_input_zero_bytes',
        'n_input_nonzero_bytes', 'chain_id'
    ],
    'logs': [
        'block_number', 'transaction_index', 'log_index', 'transaction_hash',
        'address', 'topic0', 'topic1', 'topic2', 'topic3', 'data',
        'n_data_bytes', 'chain_id'
    ],
    'traces': [
        'action_from', 'action_to', 'action_value', 'action_gas', 'action_input',
        'action_call_type', 'action_init', 'action_reward_type', 'action_type',
        'result_gas_used', 'result_output', 'result_code', 'result_address',
        'trace_address', 'subtraces', 'transaction_index', 'transaction_hash',
        'block_number', 'block_hash', 'error', 'chain_id'
    ],
    'contracts': [
        'block_number', 'block_hash', 'create_index', 'transaction_hash',
        'contract_address', 'deployer', 'factory', 'init_code', 'code',
        'init_code_hash', 'n_init_code_bytes', 'n_code_bytes', 'code_hash',
        'chain_id'
    ],
    'native_transfers': [
        'block_number', 'block_hash', 'transaction_index', 'transfer_index',
        'transaction_hash', 'from_address', 'to_address', 'value_binary',
        'value_string', 'value_f64', 'chain_id'
    ],
    'balance_diffs': [
        'block_number', 'transaction_index', 'transaction_hash', 'address',
        'from_value_binary', 'from_value_string', 'from_value_f64',
        'to_value_binary', 'to_value_string', 'to_value_f64', 'chain_id'
    ],
    'code_diffs': [
        'block_number', 'transaction_index', 'transaction_hash', 'address',
        'from_value', 'to_value', 'chain_id'
    ],
    'nonce_diffs': [
        'block_number', 'transaction_index', 'transaction_hash', 'address',
        'from_value', 'to_value', 'chain_id'
    ],
    'storage_diffs': [
        'block_number', 'transaction_index', 'transaction_hash', 'address',
        'slot', 'from_value', 'to_value', 'chain_id'
    ]
}

def parse_schema_from_output(output):
    """Parse the schema from cryo dry run output"""
    lines = output.split('\n')
    in_schema = False
    columns = []
    
    for line in lines:
        if 'schema for' in line:
            in_schema = True
            continue
        if in_schema and line.strip() == '':
            break
        if in_schema and line.startswith('- '):
            col_name = line.split('-')[1].strip().split(':')[0].strip()
            columns.append(col_name)
    
    return columns

def check_dataset_dry(dataset, rpc_url):
    """Check dataset schema using dry run (no download needed)"""
    print(f"\n{'='*60}")
    print(f"CHECKING: {dataset}")
    print('='*60)
    
    try:
        # Use a more recent block that should work
        block_range = '40000000:40000001' if dataset != 'balance_diffs' else '40000001:40000002'
        
        # Run cryo with dry flag to just get schema
        cmd = [
            'cryo', dataset,
            '--blocks', block_range,
            '--columns', 'all',
            '--dry',
            '--rpc', rpc_url
        ]
        
        print(f"Running dry run for {dataset}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return
        
        # Parse columns from output
        cryo_columns = parse_schema_from_output(result.stdout)
        
        if not cryo_columns:
            print("Could not parse schema from output")
            print("Raw output:")
            print(result.stdout)
            return
        
        print(f"\nCryo provides these columns:")
        for col in sorted(cryo_columns):
            print(f"  - {col}")
        
        # Compare with migration
        if dataset in MIGRATION_COLUMNS:
            migration_cols = sorted(MIGRATION_COLUMNS[dataset])
            
            print(f"\nMigration expects these columns:")
            for col in migration_cols:
                print(f"  - {col}")
            
            # Find differences
            missing = set(migration_cols) - set(cryo_columns)
            extra = set(cryo_columns) - set(migration_cols)
            
            if missing:
                print(f"\n❌ MISSING in Cryo (will be NULL in your table):")
                for col in sorted(missing):
                    print(f"  - {col}")
            
            if extra:
                print(f"\n➕ EXTRA in Cryo (not in your table):")
                for col in sorted(extra):
                    print(f"  - {col}")
            
            if not missing:
                print("\n✅ All required columns are provided by Cryo!")
        
    except Exception as e:
        print(f"Exception: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("CRYO COLUMN CHECKER (DRY RUN)")
    print("This checks schemas without downloading data")
    
    # Check for RPC URL
    rpc_url = os.environ.get('ETH_RPC_URL')
    if not rpc_url and len(sys.argv) > 1:
        rpc_url = sys.argv[1]
    
    if not rpc_url:
        print("\nError: No RPC URL provided!")
        print("Usage: python3 check_columns.py <RPC_URL>")
        print("Or set ETH_RPC_URL environment variable")
        return
    
    print(f"\nUsing RPC: {rpc_url}")
    
    # Check if cryo is installed
    try:
        subprocess.run(['cryo', '--version'], capture_output=True, check=True)
    except:
        print("Error: cryo not found. Please install cryo first.")
        return
    
    # Check all datasets
    for dataset in ['blocks', 'transactions', 'logs', 'traces', 'contracts', 
                    'native_transfers', 'balance_diffs', 'code_diffs', 
                    'nonce_diffs', 'storage_diffs']:
        try:
            check_dataset_dry(dataset, rpc_url)
        except Exception as e:
            print(f"Error checking {dataset}: {e}")
    
    print("\n" + "="*60)
    print("SUMMARY: Look for ❌ MISSING columns - these need to be handled!")
    print("="*60)

if __name__ == "__main__":
    main()