import pandas as pd
from datetime import datetime
import os

def validate_field(value, field_spec):
    
    # Handle null values
    if pd.isna(value):
        return not field_spec.get('required', False)
    
    try:
        if field_spec['type'] == 'int':
            int(value)
        elif field_spec['type'] == 'float':
            float(value)
        elif field_spec['type'] == 'date':
            datetime.strptime(str(value), field_spec.get('format', '%Y-%m-%d'))
        elif field_spec['type'] == 'enum':
            return str(value) in field_spec['values']
        return True
    except (ValueError, TypeError):
        return False

def validate_data(source_name, dataframe):
    # Get the appropriate schema based on source_name
    schema_map = {
        'customers': CUSTOMER_SCHEMA,
        'sales': SALES_SCHEMA,
        'products': PRODUCT_SCHEMA
    }
    
    schema = schema_map.get(source_name)
    if not schema:
        raise ValueError(f"Unknown source_name: {source_name}")
    
    
    schema_dict = {field['name']: field for field in schema}
    
    
    error_mask = pd.Series(False, index=dataframe.index)
    error_messages = []
    
    # Validate each field in the schema
    for field_name, field_spec in schema_dict.items():
        if field_name not in dataframe.columns and field_spec.get('required', False):
            
            error_mask |= True
            error_messages.extend([f"Missing required column: {field_name}"] * len(dataframe))
            continue
            
        if field_name in dataframe.columns:
            
            for idx, value in dataframe[field_name].items():
                if not validate_field(value, field_spec):
                    error_mask[idx] = True
                    error_messages.append(f"Invalid {field_name}: {value}")
    
    # Split into clean and error rows
    error_rows = dataframe[error_mask].copy()
    clean_rows = dataframe[~error_mask].copy()
    
    # Add error messages to error rows
    if not error_rows.empty:
        error_rows['error_messages'] = error_messages
        
        # Create error_reports directory if it doesn't exist
        os.makedirs('./error_reports', exist_ok=True)
        
        # Write error rows to CSV with dynamic filename
        current_date = datetime.now().strftime('%Y%m%d')
        error_filename = f"error_reports/errors_{source_name}_{current_date}.csv"
        error_rows.to_csv(error_filename, index=False)
    
    return clean_rows, error_rows