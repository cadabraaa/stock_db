from flask import Flask, render_template
import requests
from database import insert_data_into_database  # Assuming 'database' is a module that contains your SQLAlchemy engine

app = Flask(__name__)

# Global variable to store the previous timestamp
previous_timestamp = None

def find_timestamp(data):
    """
    Recursively find and return the timestamp from the data structure.
    """
    timestamp = data.get('timestamp')
    if timestamp:
        return timestamp

    for key, value in data.items():
        if isinstance(value, (dict, list)):
            result = find_timestamp(value)
            if result:
                return result

    return None

@app.route("/", methods=['GET'])
def index():
    global previous_timestamp

    # URL of the NSE India API
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

    # Set headers to mimic a browser request
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    # Make a GET request with headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the entire JSON response to inspect its structure
        print("JSON Response:", response.json())

        # Parse the JSON content of the response
        data = response.json()

        # Extract the timestamp using the new function
        current_timestamp = find_timestamp(data)

        if current_timestamp and current_timestamp != previous_timestamp:
            # Update the previous timestamp
            previous_timestamp = current_timestamp

            # Insert data into the database
            insert_data_into_database(data)

        # Return the timestamp and data as a dictionary
        data_to_render = {'timestamp': current_timestamp, 'filtered_data': data.get('filtered', {}).get('data', [])}

        # You can return the data to the template without saving it to a file
        return render_template('index.html', data=data_to_render)
    else:
        # If the request was not successful, return an error message
        return f"Error fetching data: {response.status_code}"

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
