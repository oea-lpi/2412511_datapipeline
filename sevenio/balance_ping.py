from seven_api import SevenClient

client = SevenClient(api_key='YOUR_API_KEY')

try:
    balance = client.balance.retrieve()
    print(f"Seven Io is reachable. Your balance is: {balance} EUR")
except Exception as e:
    print(f"Could not reach Seven Io: {e}")
