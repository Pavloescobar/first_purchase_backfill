import requests
from datetime import datetime
from time import time, sleep
import threading
import logging

# ​ Script Execution ###​

# Account Information
SEGMENT_ID = ""
METRIC_ID = ""
PRIVATE_API_KEY = ""
HEADERS = {
    "accept": "application/json",
    "revision": "2023-09-15",
    "Authorization": f"Klaviyo-API-Key {PRIVATE_API_KEY}",
}
logging.basicConfig(filename='script.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

current_cursor = 'STARTED'
profile_count = 0
# Helper methods


def get_segment_profiles(cursor=""):
    print('get profiles', time())
    global current_cursor
    # Handling for non 200 response
    try:
        response = requests.get(cursor, headers=HEADERS)
        response.raise_for_status()  # Raises an exception for non-200 responses
        data = response.json().get('data')
        links = response.json().get('links').get('next')

        profile_ids = []
        for item in data:
            profile_ids.append(item["id"])

        if links:
            current_cursor = links
            print('current cursor', current_cursor)
        else:
            print('FINISHED PAGINATION!')
            current_cursor = 'DONE'

        return profile_ids
    except requests.exceptions.HTTPError as e:
        print(f"Error getting segment profiles: {e}")
        return []

    return profile_ids


def get_properties_of_first_event_for_profile(profile_id, metric_id):
    # print('get events', time.time())
    url = f"https://a.klaviyo.com/api/events/?filter=equals(profile_id,'{profile_id}'),and(equals(metric_id,'{metric_id}'))&sort=datetime&page[size]=1"
    response = ''
    data = ''
    # Handling non 200 response & logging
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json().get('data')
    except requests.exceptions.HTTPError as e:
        print(
            f"Error fetching event for {profile_id}: HTTP {e.response.status_code}")
        logging.error("Error fetching event for %s: HTTP %s",
                      profile_id, e.response.status_code)

    except:
        print("Error fetching event for ", profile_id,  response)
        logging.error('Error fetching event for %s %s', profile_id, response)

    if len(data) > 0:
        # DATA MAY BE NESTED DIFFERENTLY FOR YOUR PLACED ORDER EVENT - MAKE CHANGES AS REQUIRED
        # Use POSTMAN to Evaluate with a GET request using the URL above
        # print(event_properties)
        event_properties = data[0]["attributes"].get("event_properties")
        event_items = event_properties.get("Items")
        event_value = event_properties.get("$value")
        # added in rounding to 2dp
        event_value = round(float(event_value), 2)
        first_purchase_date = event_properties.get(
            '$extra').get('created_at')[:10]
        return {
            "First Purchase Date": first_purchase_date,
            "First Purchase Value": event_value,
            "First Purchase Products": event_items
        }
    else:
        print('data missing for ', profile_id, data)
        logging.error('Data missing for %s', profile_id)
    return {}


def update_profile_payload(profile_id, properties):
    return {
        "data": {
            "type": "profile",
            "id": profile_id,
            "attributes": {
                "properties": properties
            }
        }
    }


def set_properties_for_profile(profile_id, properties):
    url = f"https://a.klaviyo.com/api/profiles/{profile_id}"
    payload = update_profile_payload(profile_id, properties)
    try:
        response = requests.patch(url, json=payload, headers=HEADERS)
        if(response.status_code != 200):
            logging.error('Update error %s ', response.status_code)
            logging.error('Reponse is %s', response.json())
    except:
        logging.error('something went wrong with %s %s',
                      profile_id, payload)

    global profile_count
    profile_count = profile_count + 1
    print('Updated ', profile_count, ' profiles so far', time())


def threaded_update(profile_id, metric_id):
    new_properties = get_properties_of_first_event_for_profile(
        profile_id, metric_id)
    if new_properties and isinstance(new_properties, dict) and new_properties:
        # Check if new_properties is not None, is a dictionary, and not empty
        set_properties_for_profile(profile_id, new_properties)
    else:
        print(
            f"Skipping profile {profile_id} due to invalid or empty properties")
        logging.error(
            f'Skipping profile, {profile_id} due to invalid or empty properties')


# Main script execution


def main():
    global current_cursor
    current_cursor = f"https://a.klaviyo.com/api/segments/{SEGMENT_ID}/profiles/?page[size]=100"

    while current_cursor != "DONE":
        # Get IDs of all profiles belonging into the sepcified segment
        profile_ids = get_segment_profiles(current_cursor)
        # print(profile_ids)
        # Extract desired properties from their first Placed Order event
        for profile_id in profile_ids:
            # we have a limit of 700 updates per min, so need to sleep briefly
            sleep(60/700)
            threading.Thread(target=threaded_update, args=[
                             profile_id, METRIC_ID]).start()


if __name__ == "__main__":
    main()
