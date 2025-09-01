import pandas as pd

def fetch_first_entry(unp_csv, **kwargs):
    df = pd.read_csv(unp_csv)
    if df.empty:
        raise ValueError(f"CSV file {unp_csv} is empty!")
    
    artist_id = df.iloc[0]

    first_entry = df.iloc[0].to_dict()  # Convert row to dictionary

    if artist_id is None:
        raise ValueError("No artist_id found in the first row!")

    # Push artist_id as a string or int (JSON serializable)
    kwargs['ti'].xcom_push(key='artist_id', value=str(artist_id))

    return first_entry 


def remove_first_entry(unp_csv):
    df = pd.read_csv(unp_csv)
    df = df.drop(index=0)
    df.to_csv(unp_csv, index=False)

def add_first_entry_to_another_csv(unp_csv, pro_csv):
    df = pd.read_csv(unp_csv)
    
    # Fetch the first entry (first row)
    first_entry = df.iloc[0]
    
    try:
        if first_entry is not None:
            # Check if the target CSV is empty or exists
            try:
                target_df = pd.read_csv(pro_csv)
                
                # Check if the entry already exists in the target CSV
                if first_entry.to_dict() not in target_df.to_dict(orient='records'):
                    # If the target CSV is empty, create a new one with the first entry
                    if target_df.empty:
                        target_df = pd.DataFrame([first_entry])  # Create a new DataFrame with the first entry
                    else:
                        # Append the first entry to the target CSV
                        target_df = pd.concat([target_df, pd.DataFrame([first_entry])], ignore_index=True)
                    # Save the updated target DataFrame
                    target_df.to_csv(pro_csv, index=False)
                    print(f"First entry added to {pro_csv}")
                else:
                    print("Duplicate entry found. First entry not added.")
            except pd.errors.EmptyDataError:
                # If the target CSV doesn't exist or is empty, create a new one with the first entry
                target_df = pd.DataFrame([first_entry])  # Create a new DataFrame with the first entry
                target_df.to_csv(pro_csv, index=False)
                print(f"Target CSV was empty. Created new file with first entry.")
    except Exception as e:
        print(f"Error updating {pro_csv}: {e}")
