import click
import pandas as pd
import requests
import configparser
import os

# Determine the directory of the current script
script_dir = os.path.dirname(__file__)

# Construct the absolute path to the config.ini file
config_path = os.path.join(script_dir, 'config.ini')

# Load the configuration file
config = configparser.ConfigParser()
config.read(config_path)

# Check if 'API' section exists in the configuration
if 'API' not in config:
    raise Exception(f"Section 'API' not found in the configuration file at {config_path}")

api_url = config['API']['url']

@click.command()
@click.argument('input_csv', type=click.Path(exists=True))
@click.option('--output-csv', type=click.Path(), default=None, help='Output CSV file path')
def classify_messages(input_csv, output_csv):
    """
    This script reads messages from a CSV file, sends them to an API for classification,
    and appends the response to the CSV file in a new column 'label_classification_api'.
    """
    # Read the CSV file
    df = pd.read_csv(input_csv)

    # Ensure the 'label_classification_api' column exists
    if 'label_classification_api' not in df.columns:
        df['label_classification_api'] = None

    # Iterate over the rows in the DataFrame
    for index, row in df.iterrows():
        # Prepare the data for the POST request
        data = {
            "id": "DUMMY",
            "message": row['message']
        }
        headers = {'Content-Type': 'application/json'}

        # Send the POST request
        response = requests.post(api_url, json=data, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            response_data = response.json()
            # Update the 'label_classification_api' column with the message_type
            df.at[index, 'label_classification_api'] = response_data.get('message_type')
        else:
            print(f"Failed to classify message at index {index}: {response.text}")

    # Determine the output CSV file path
    output_csv = output_csv if output_csv else input_csv

    # Save the updated DataFrame to a CSV file
    df.to_csv(output_csv, index=False)

    print(f"Classification completed. Updated CSV saved to {output_csv}")

if __name__ == '__main__':
    classify_messages()
