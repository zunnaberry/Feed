import re
import requests

from common import *
from datetime import datetime, timedelta
import io
import pdfplumber
import concurrent.futures

import pandas as pd
import yfinance as yf
from KiteSingleton import KiteSingleton


def clean_tittle(text):
    text = text.lower()
    modified_text = text.replace("the ", "")
    modified_text = modified_text.replace("and", "").replace("&", "")
    modified_text = modified_text.replace("(", "").replace(")", "")
    modified_text = modified_text.replace("-", "").replace(" ", "")
    modified_text = modified_text.replace("limited", "").replace("ltd", "")
    modified_text = modified_text.replace("the", "")
    return modified_text


def get_stock_price(stock_code):
    # Yahoo Finance uses a different format for NSE stocks, typically '.NS' is appended
    stock_code_with_suffix = f"{stock_code}.NS"

    try:
        # Fetch stock data
        stock = yf.Ticker(stock_code_with_suffix)
        stock_info = stock.info

        # Extract current price
        current_price = stock_info.get('currentPrice', 'N/A')  # You can use other fields if needed

        return current_price
    except Exception as e:
        print(f"Error fetching data for {stock_code}: {e}")
        return ""


# Path to the Excel file
excel_file = "./inputfile/input_file.xlsx"  # Replace with the path to your Excel file


class CompanyLookup:
    def __init__(self):
        # Load the Excel file into a DataFrame once during initialization
        self.company_df = pd.read_excel(excel_file)
        # Clean the company names in the DataFrame
        self.company_df['Company Name'] = self.company_df['Company Name'].apply(clean_tittle)
        # Create a dictionary mapping cleaned names to their symbols
        self.company_map = dict(zip(self.company_df['Company Name'], self.company_df['Symbol']))
        self.format_company_map = dict(zip(self.company_df['Company Name'], self.company_df['Formatted Symbol']))

    def get_company_symbol(self, input_value):
        # Convert the input value to lowercase for case-insensitive lookup
        return self.company_map.get(input_value.lower())

    def get_format_company_symbol(self, input_value):
        # Convert the input value to lowercase for case-insensitive lookup
        return self.format_company_map.get(input_value.lower())


# Initialize the CompanyLookup class with the path to your Excel file
lookup = CompanyLookup()

already_sent = []  # Initialize an empty array to keep track of sent reports


def process_item(item):
    subjects = extract_subject(item['description'])
    subjects = subjects.lower()
    final_data = None
    final_data_not_financial_report = None
    if "financial result updates" in subjects:
        all_reports = extract_pdf_text_with_pdfplumber(item['link'])
        if all_reports is None:
            return None

        # Compare with previous quarter
        chat_content = ("read below report and reply standalone Quarterly Results in below fix format : "
                        "sales - current quarter (last quarter: increase/decrease %), "
                        "expenses - current quarter (last quarter: increase/decrease %), "
                        "Operating Profit - current quarter (last quarter: increase/decrease %), "
                        "Profit before tax - current quarter (last quarter: increase/decrease %), "
                        "Net Profit- current quarter (last quarter: increase/decrease %),"
                        f"\"{all_reports}\"")
        chat_gpt_response_with_previous_quarter_compare = get_response_from_gpt(chat_content)
        html_chat_gpt_response_with_previous_quarter_compare = generate_html_report(
            chat_gpt_response_with_previous_quarter_compare)

        chat_content = ("from this statement just reply company having \"strong\" or \"weak\" performance compare "
                        "with last quarter and with net \"profit\" or \"loss\" in percentage:"
                        f"\"{chat_gpt_response_with_previous_quarter_compare}\"")
        chat_gpt_response_with_previous_quarter_compare_short = get_response_from_gpt(chat_content)

        # Compare with same quarter of last year
        chat_content = ("read below report and reply standalone Quarterly Results in below fix format : "
                        "sales - current quarter (same quarter of last year: increase/decrease %), "
                        "expenses - current quarter (same quarter of last year: increase/decrease %), "
                        "Operating Profit - current quarter (same quarter of last year: increase/decrease %), "
                        "Profit before tax - current quarter (same quarter of last year: increase/decrease %), "
                        "Net Profit- current quarter (same quarter of last year: increase/decrease %),"
                        f"\"{all_reports}\"")
        chat_gpt_response_with_same_quarter_last_year_compare = get_response_from_gpt(chat_content)
        html_chat_gpt_response_with_same_quarter_last_year_compare = generate_html_report(
            chat_gpt_response_with_same_quarter_last_year_compare)

        chat_content = ("from this statement just reply company having \"strong\" or \"weak\" performance compare "
                        "with same quarter of last year and with net \"profit\" or \"loss\" in percentage:"
                        f"\"{chat_gpt_response_with_same_quarter_last_year_compare}\"")
        chat_gpt_response_with_same_quarter_last_year__short = get_response_from_gpt(chat_content)

        final_data = {
            'channel_title': item['channel_title'],
            'link': item['link'],
            'description': item['description'],
            'pub_date': item['pub_date'],
            'chat_gpt_response_last_quarter': html_chat_gpt_response_with_previous_quarter_compare,
            'chat_gpt_response_last_quarter_short': chat_gpt_response_with_previous_quarter_compare_short,
            'chat_gpt_response_same_quarter_last_year': html_chat_gpt_response_with_same_quarter_last_year_compare,
            'chat_gpt_response_same_quarter_last_year_short': chat_gpt_response_with_same_quarter_last_year__short,
        }

    else:
        final_data_not_financial_report = {
            'channel_title': item['channel_title'],
            'link': item['link'],
            'description': item['description'],
            'pub_date': item['pub_date'],
            'chat_content_title_response': "update",
            'chat_content_content_response': "update"
        }

    return final_data, final_data_not_financial_report


def run_in_threads(items_data):
    fina_items_data = []
    fina_items_no_financial_data = []

    # Using ThreadPoolExecutor to manage 5 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_item = {executor.submit(process_item, item): item for item in items_data}

        for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                data, no_financial_data = future.result()
                if data:
                    fina_items_data.append(data)
                if no_financial_data:
                    fina_items_no_financial_data.append(no_financial_data)
            except Exception as e:
                print(f"Exception occurred while processing {item['description']}: {e}")

    return fina_items_data, fina_items_no_financial_data


def extract_pdf_text_with_pdfplumber(url_to_download):
    try:
        if url_to_download is None:
            print("url is present to download file.")
            return None

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'
        }
        # Create a session
        session = requests.Session()

        # First request to get cookies
        request = session.get(baseurl, headers=headers, timeout=20)
        cookies = dict(request.cookies)

        # Second request to fetch the PDF content using the cookies
        response_value = session.get(url_to_download, headers=headers, timeout=5, cookies=cookies)

        # Check if the request was successful
        if response_value.status_code == 200:
            # Read the PDF from the response content
            pdf_stream = io.BytesIO(response_value.content)
            text = ""

            # Use pdfplumber to open the PDF and extract text
            with pdfplumber.open(pdf_stream) as pdf:
                for page in pdf.pages:
                    text += page.extract_text()

            return text
        else:
            print(f"Failed to retrieve the PDF. Status code: {response_value.status_code} " + url_to_download)
            return None

    except Exception as e:
        print(f"Exception occurred while extracting text from the PDF. Error: {e} " + url_to_download)
        return None


def process_row_with_nse_news():
    try:
        url = f"https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                 'like Gecko) '
                                 'Chrome/80.0.3987.149 Safari/537.36',
                   'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
        session = requests.Session()
        request = session.get(baseurl, headers=headers, timeout=60)
        cookies = dict(request.cookies)
        response_value = session.get(url, headers=headers, timeout=60, cookies=cookies)
        return response_value.text
        # with open('.\\inputfile\\input.xml', 'r') as file:
        #    return file.read()
    except Exception as e:
        print(f"exception occur in flow while downloading complete rss feed! continue")

from datetime import datetime, timedelta

def is_within_n_time_units_nse(date_str, duration, unit):
    try:
        # Try to parse with or without seconds
        try:
            timestamp = datetime.strptime(date_str, '%d-%b-%Y %H:%M:%S')
        except ValueError:
            timestamp = datetime.strptime(date_str, '%d-%b-%Y %H:%M')

        now = datetime.now()

        if unit == 'hours':
            duration_ago = now - timedelta(hours=duration)
        elif unit == 'minutes':
            duration_ago = now - timedelta(minutes=duration)
        elif unit == 'seconds':
            duration_ago = now - timedelta(seconds=duration)
        else:
            raise ValueError("Invalid unit. Please choose 'hours', 'minutes', or 'seconds'.")

        return duration_ago <= timestamp <= now

    except Exception as e:
        print(f"Exception occurred while checking is_within_n_time_units_nse! date_str: {date_str} - Error: {e}")
        return False



def parse_nse_response(current, time_cycle_for_report, time_unit_for_report):
    valid_xml_flag, root = is_valid_xml(current)
    if not valid_xml_flag:
        print('xml report is not valid')
        return None

    items_data = []
    for item in root.findall('.//item'):
        title_element = item.find('title')
        title_value = title_element.text.strip() if title_element is not None and title_element.text is not None else None
        link_element = item.find('link')
        link_value = link_element.text.strip() if link_element is not None and link_element.text is not None else None
        description_element = item.find('description')
        description_value = description_element.text.strip() if description_element is not None and description_element.text is not None else None
        pub_date_element = item.find('pubDate')
        pub_date_value = pub_date_element.text.strip() if pub_date_element is not None and pub_date_element.text is not None else None

        if not is_within_n_time_units_nse(pub_date_value, time_cycle_for_report, time_unit_for_report):
            continue

        items_data.append({
            'channel_title': title_value,
            'link': link_value,
            'description': description_value,
            'pub_date': pub_date_value,
        })

    return items_data


def replace_with_strong(match):
    return f'<strong>{match.group(1)}</strong>'


def generate_html_report(summary):
    summary = summary.replace("\n", "<br>")

    pattern = r'\*\*(.*?)\*\*'

    new_text = re.sub(pattern, replace_with_strong, summary)

    return new_text


def extract_subject(announcement):
    # Split the announcement on '|SUBJECT:'
    parts = announcement.split('|SUBJECT:')

    # Check if there are parts after '|SUBJECT:'
    if len(parts) > 1:
        # Return the part after '|SUBJECT:' as a trimmed string
        return parts[1].strip()
    else:
        # Return an empty string if no subject part is found
        return ""


# Define the keywords to search for
keywords = ["order", "received", "purchase", "sale", "transaction",
            "confirmation", "delivery", "shipment", "fulfillment",
            "invoice", "received", "mou", "sign", "contract",
            "agreements", "arrangements", "loa", 'award', 'worth', "deal",
            "sebi"]


# Function to search for keywords in a string and return True/False
def positive_word_search(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in keywords)


keywords1 = ["gst"]


# Function to search for keywords in a string and return True/False
def negative_word_search(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in keywords1)


def execute_nse_core(time_cycle_for_report, time_unit_for_report, use_chat_gpt):
    nse_news_data = process_row_with_nse_news()
    items_data = parse_nse_response(nse_news_data, time_cycle_for_report, time_unit_for_report)

    if items_data is None or len(items_data) == 0:
        print("No new report found!")
        return

    fina_items_data, fina_items_no_financial_data = run_in_threads(items_data)

    if len(fina_items_data) != 0:
        for final_data in fina_items_data:
            if any(
                    final_data['channel_title'] == item['channel_title'] and
                    final_data['link'] == item['link'] and
                    final_data['description'] == item['description'] and
                    final_data['pub_date'] == item['pub_date']
                    for item in already_sent
            ):
                print("Matching item found in already_sent, skipping sending the message.")
                continue

            clean_news_title = clean_tittle(final_data['channel_title'])
            symbol = lookup.get_company_symbol(clean_news_title)
            instrument_token = f"NSE:{symbol}"
            if symbol is None:
                current_price = "Stock price not found"
            else:
                current_price = get_stock_price(symbol)

            # quote = KiteSingleton().get_kite_instance().ltp(instrument_token)

            # Check if the quote is valid and contains the expected data
            # if not quote or instrument_token not in quote:
            #    symbol = lookup.get_format_company_symbol(clean_news_title)
            #    instrument_token = f"NSE:{symbol}"
            #    quote = KiteSingleton().get_kite_instance().ltp(instrument_token)
            #    if not quote or instrument_token not in quote:
            #        current_price = 'N/A'
            #    else:
            #        current_price = quote[instrument_token]["last_price"]
            # else:
            #    current_price = quote[instrument_token]["last_price"]
            final_telegram_tex = (
                f"**Channel Title:** {final_data.get('channel_title', 'N/A')}\n"
                f"**Link:** {final_data.get('link', 'N/A')}\n"
                f"**Description:** {final_data.get('description', 'N/A')}\n"
                f"**Published Date:** {final_data.get('pub_date', 'N/A')}\n"
                f"**current price:** {current_price}\n"
                f"**last quarter:** {final_data.get('chat_gpt_response_last_quarter_short', 'N/A')}\n"
                f"**same quarter of last year:** {final_data.get('chat_gpt_response_same_quarter_last_year_short', 'N/A')}\n"
            )

            api_id = '26456121'
            api_hash = '74df6d467ed43266c62111554fcb6e90'
            phone_number = '+919004317058'  # Your Telegram phone number
            target = '+919029085929'  # The recipient's username or phone number
            try:
                send_telegram_message_with_attachment(api_id, api_hash, phone_number, target,
                                                      final_telegram_tex)
                # Add the final_data to the already_sent array after successfully sending the message
                already_sent.append(final_data)
            except Exception as e:
                print(f"Failed to send message: {e}")

    else:
        print('no financial new report found!')

    if len(fina_items_no_financial_data) != 0:

        for final_data in reversed(fina_items_no_financial_data):
            if any(
                    final_data['channel_title'] == item['channel_title'] and
                    final_data['link'] == item['link'] and
                    final_data['description'] == item['description'] and
                    final_data['pub_date'] == item['pub_date']
                    for item in already_sent
            ):
                continue

            # if positive_word_search(final_data.get('description', 'N/A')) is False:
            #     continue
            #
            # if negative_word_search(final_data.get('description', 'N/A')) is True:
            #     continue

            # Compare with previous quarter
            chat_content = ("Read this subject \"yes\"or \"no\" if company received any work order or sales order"
                            " or signed mou or sign any contract or any agreements or any arrangements"
                            " or received any work order or"
                            " anything that will increase revenue of company " +
                            final_data['description'])

            order_response = 'yes'
            if use_chat_gpt is True:
                order_response = get_response_from_gpt(chat_content)

            if order_response.lower() == 'yes':
                all_reports = ""
                if use_chat_gpt is True:
                    all_reports = extract_pdf_text_with_pdfplumber(final_data['link'])
                if all_reports is not None:
                    # Compare with previous quarter
                    chat_content = ("Read this subject \"yes\"or \"no\" if company received any work order or sales "
                                    "order or signed mou or sign any contract or any agreements or any arrangements"
                                    " anything that will increase revenue of company " +
                                    all_reports)

                    chat_gpt_response_with_previous_quarter_compare = 'yes'
                    if use_chat_gpt is True:
                        chat_gpt_response_with_previous_quarter_compare = get_response_from_gpt(chat_content)

                    if chat_gpt_response_with_previous_quarter_compare.lower() == 'yes':
                        clean_news_title = clean_tittle(final_data['channel_title'])
                        symbol = lookup.get_company_symbol(clean_news_title)

                        if symbol is None:
                            current_price = "Stock price not found"
                        else:
                            current_price = get_stock_price(symbol)

                        # instrument_token = f"NSE:{symbol}"
                        # quote = KiteSingleton().get_kite_instance().ltp(instrument_token)
                        #
                        # # Check if the quote is valid and contains the expected data
                        # if not quote or instrument_token not in quote:
                        #     symbol = lookup.get_format_company_symbol(clean_news_title)
                        #     instrument_token = f"NSE:{symbol}"
                        #     quote = KiteSingleton().get_kite_instance().ltp(instrument_token)
                        #     if not quote or instrument_token not in quote:
                        #         current_price = 'N/A'
                        #     else:
                        #         current_price = quote[instrument_token]["last_price"]
                        # else:
                        #     current_price = quote[instrument_token]["last_price"]
                        final_telegram_tex = (
                            f"**Channel Title:** {final_data.get('channel_title', 'N/A')}\n"
                            f"**Link:** {final_data.get('link', 'N/A')}\n"
                            f"**Description:** {final_data.get('description', 'N/A')}\n"
                            f"**Published Date:** {final_data.get('pub_date', 'N/A')}\n"
                            f"**current price:** {current_price}\n"
                            f"**Title Response:** {final_data.get('chat_content_title_response', 'N/A')}\n"
                            f"**Content Response:** {final_data.get('chat_content_content_response', 'N/A')}\n"
                        )

                        api_id = '26456121'
                        api_hash = '74df6d467ed43266c62111554fcb6e90'
                        phone_number = '+919004317058'  # Your Telegram phone number
                        target = '+919029085929'  # The recipient's username or phone number
                        # attachment_path = excel_file  # Path to your file
                        try:
                            send_telegram_message_with_attachment(api_id, api_hash, phone_number, target,
                                                                  final_telegram_tex)
                            # Add the final_data to the already_sent array after successfully sending the message
                            already_sent.append(final_data)
                        except Exception as e:
                            print(f"Failed to send message: {e}")

    else:
        print('no new feed report found!')
