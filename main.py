from googleNews import *
from nse import *
from KiteSingleton import KiteSingleton

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    api_key = "bz3rntveexlha8bw"  # Replace with your API key
    api_secret = "2qhn7fu54apvelwmqdduuhyvhzndqxfe"  # Replace with your API secret

    api_id = '26456121'
    api_hash = '74df6d467ed43266c62111554fcb6e90'
    phone_number = '+919322504136'  # Your Telegram phone number
    targets = '+919029085929'  # List of recipient phone numbers
    message = 'login attempt'

    send_telegram_message_with_attachment(api_id, api_hash, phone_number, targets, message)

    params = get_telegram_parameters(api_id, api_hash, phone_number)

    nse_feed_output = params.get('nse_feed_output', False)
    one_time = params.get('one_time', False)
    time_cycle_for_report = params.get('time_cycle_for_report', 1)
    time_unit_for_report = params.get('time_unit_for_report', 'minutes')
    the_value = params.get('the_value', 'minutes')
    use_chat_gpt = params.get('use_chat_gpt', False)
    # Initialize the KiteSingleton instance
    # KiteSingleton(api_key, api_secret, the_value)

    if nse_feed_output:
        if one_time is True:
            try:
                execute_nse_core(time_cycle_for_report, time_unit_for_report, use_chat_gpt)
            except Exception as e:
                print(f"Exception occurred in flow! Continuing... only one more time")
                execute_nse_core(time_cycle_for_report, time_unit_for_report, use_chat_gpt)
                print(f"single report executed with {time_cycle_for_report}{time_unit_for_report}")
            finally:
                print(f"single report executed with {time_cycle_for_report}{time_unit_for_report}")
        else:
            while True:
                try:
                    execute_nse_core(time_cycle_for_report, time_unit_for_report, use_chat_gpt)
                except Exception as e:
                    print(f"Exception occurred in flow! Continuing...")
                finally:
                    print(f"Next scan! for {time_cycle_for_report}{time_unit_for_report}")
    else:
        execute_google_core(time_cycle_for_report, time_unit_for_report)
