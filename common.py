from datetime import datetime
from openai import OpenAI
import xml.etree.ElementTree as xmlTree
from telethon import TelegramClient

nse_feed_output = True

baseurl = "https://www.nseindia.com/"


open_client = None  # Declare the global variable

def initialize_open_ai(open_ai_api_key):
    global open_client
    open_client = OpenAI(api_key=open_ai_api_key)

def send_telegram_message_with_attachment(api_id, api_hash, phone_number, target, message, attachment_path=None):
    # Initialize the Telegram client
    client = TelegramClient('telegram_session', api_id, api_hash)

    async def main():
        # Connect to Telegram
        await client.start(phone_number)

        # Send the message
        await client.send_message(target, message)

        # Send the attachment if provided
        if attachment_path:
            await client.send_file(target, attachment_path)

    # Start the client and run the main function
    with client:
        client.loop.run_until_complete(main())


def is_valid_xml(xml_string):
    try:
        # Attempt to parse the string as XML
        root_local = xmlTree.fromstring(xml_string)
        return True, root_local
    except xmlTree.ParseError:
        return False, ""


def generate_filename(prefix='file', extension='txt'):
    # Get the current date and time
    now = datetime.now()

    # Format the date and time as a string (e.g., '2024-07-30_17-38-29')
    timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')

    # Construct the filename
    filename = f"{prefix}_{timestamp}.{extension}"

    return filename


def get_response_from_gpt(chat_search_content):
    chat_completion = open_client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": chat_search_content
            }
        ],
        model="gpt-4o-mini")

    return chat_completion.choices[0].message.content


def get_telegram_parameters(api_id, api_hash, phone_number):
    # Initialize the Telegram client
    client = TelegramClient('telegram_session', api_id, api_hash)

    async def main():
        # Connect to Telegram
        await client.start(phone_number)

        # Fetch the latest message from the bot or specific chat
        messages = await client.get_messages(phone_number, limit=1)

        if messages:
            last_message = messages[0].message
            return parse_message(last_message)

    # Start the client and run the main function
    with client:
        result = client.loop.run_until_complete(main())
        return result


# Function to parse the received message
def parse_message(message):
    params = {}
    try:
        # Split the message into key-value pairs
        items = message.split(',')
        for item in items:
            key, value = item.split(':')
            key = key.strip()
            value = value.strip().strip("'")
            # Convert the value to appropriate types
            if value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            elif value.isdigit():
                value = int(value)
            params[key] = value
    except Exception as e:
        print(f"Error parsing message: {e}")
    return params
