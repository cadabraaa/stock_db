from flask import Flask, render_template, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from database import insert_data_into_database

app = Flask(__name__)

# Global variable to store the previous timestamp
previous_timestamp = None

scheduler = BackgroundScheduler()


def extract_timestamp(data):
  """
    Extract timestamp from data structure.
    """
  timestamp = data.get('timestamp')

  if timestamp:
    print("Extracted Timestamp:", timestamp)
    return timestamp

  if isinstance(data, dict):
    for key, value in data.items():
      if isinstance(value, (dict, list)):
        result = extract_timestamp(value)
        if result:
          return result
  elif isinstance(data, list):
    for item in data:
      result = extract_timestamp(item)
      if result:
        return result

  return None


def job():
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
    # Parse the JSON content of the response
    data = response.json()

    # Extract the timestamp using a recursive approach
    current_timestamp = extract_timestamp(data)

    if current_timestamp and current_timestamp != previous_timestamp:
      # Update the previous timestamp
      previous_timestamp = current_timestamp

      timestamp_value = data['records']['timestamp']
      for dict_temp in data['filtered']['data']:
        dict_temp['PE']['timestamp'] = timestamp_value
        dict_temp['CE']['timestamp'] = timestamp_value

      # Insert data into the database
      insert_data_into_database(data)


# Schedule the job to run every 60 seconds
scheduler.add_job(job, 'interval', seconds=15)
scheduler.start()


@app.route("/", methods=['GET'])
def index():
  return render_template("index.html")


if __name__ == "__main__":
  app.run(host='0.0.0.0', debug=True)
