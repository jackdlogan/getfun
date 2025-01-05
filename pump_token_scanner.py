import asyncio
import websockets
import json
import csv
from datetime import datetime
from pathlib import Path
from supabase import create_client
from postgrest.exceptions import APIError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase configuration with error checking
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("""
    Missing Supabase credentials!
    Please set SUPABASE_URL and SUPABASE_KEY environment variables.
    Current values:
    SUPABASE_URL: {url}
    SUPABASE_KEY: {key}
    """.format(
        url=SUPABASE_URL or 'Not set',
        key='[Hidden]' if SUPABASE_KEY else 'Not set'
    ))

# Initialize Supabase client
try:
    # Initialize with default options
    supabase = create_client(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY
    )
    print("✓ Successfully initialized Supabase client")
except Exception as e:
    print(f"❌ Error initializing Supabase client: {str(e)}")
    raise

async def init_db():
    """Initialize connection to Supabase"""
    try:
        # Create tokens table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tokens (
            id bigint generated by default as identity primary key,
            created_at timestamp with time zone default timezone('utc'::text, now()),
            token_address text unique,
            token_name text,
            token_symbol text
        );
        """
        
        try:
            # Test if table exists
            supabase.table('tokens').select('*').execute()
            print("✓ Connected to Supabase successfully")
            print("✓ Tokens table ready")
        except APIError as e:
            if 'relation "public.tokens" does not exist' in str(e):
                print("Creating tokens table...")
                print("\n⚠️ Please create the tokens table in Supabase SQL editor with this SQL:")
                print("\n" + create_table_query)
                print("\nAfter creating the table, restart this script.")
                raise
            else:
                print(f"❌ Error connecting to Supabase: {str(e)}")
                raise
                
    except Exception as e:
        print(f"❌ Error connecting to Supabase: {str(e)}")
        raise

async def store_raw_message(message, parsed_data):
    """Store message in both CSV and Supabase"""
    # Skip if no signature in the message
    if not isinstance(parsed_data, dict) or 'signature' not in parsed_data:
        return

    csv_file = 'tokens.csv'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Token-specific fields might be in different locations based on message type
        token_address = (
            parsed_data.get('address', '') or 
            parsed_data.get('token', '') or 
            parsed_data.get('mint', '')
        )
        
        token_name = parsed_data.get('name', '')
        token_symbol = parsed_data.get('symbol', '')

        # Only store if we have at least the token address
        if token_address:
            # Store in CSV (keeping local backup)
            await store_in_csv(csv_file, timestamp, token_address, token_name, token_symbol)
            
            # Store in Supabase
            try:
                # Check if token already exists
                response = supabase.table('tokens').select('token_address').eq('token_address', token_address).execute()
                
                if not response.data:  # Token doesn't exist yet
                    data = {
                        'token_address': token_address,
                        'token_name': token_name,
                        'token_symbol': token_symbol
                    }
                    
                    supabase.table('tokens').insert(data).execute()
                    print(f"\n💎 New token stored in Supabase:")
                    print(f"Time: {timestamp}")
                    print(f"Address: {token_address}")
                    print(f"Name: {token_name}")
                    print(f"Symbol: {token_symbol}")
            
            except Exception as e:
                print(f"❌ Error storing in Supabase: {str(e)}")
            
    except Exception as e:
        print(f"❌ Error processing message: {str(e)}")

async def store_in_csv(csv_file, timestamp, token_address, token_name, token_symbol):
    """Store token data in CSV as backup"""
    # Create file with headers if it doesn't exist
    if not Path(csv_file).exists():
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp',
                'Token Address',
                'Token Name',
                'Token Symbol'
            ])
    
    try:
        # Check if token already exists
        existing_tokens = set()
        if Path(csv_file).exists():
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_tokens = {row['Token Address'] for row in reader}

        # Only write if token address is new
        if token_address not in existing_tokens:
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                row = [timestamp, token_address, token_name, token_symbol]
                writer.writerow(row)
                print(f"📝 Token also stored in CSV backup")
            
    except Exception as e:
        print(f"❌ Error writing to CSV: {str(e)}")

async def subscribe():
    uri = "wss://pumpportal.fun/api/data"
    
    print("🔄 Connecting to PumpPortal websocket...")
    
    # Initialize database connection
    await init_db()
    
    while True:  # Add reconnection loop
        try:
            async with websockets.connect(uri) as websocket:
                print("✅ Successfully connected to websocket")
                
                # Subscribing to token creation events
                payload = {
                    "method": "subscribeNewToken",
                }
                await websocket.send(json.dumps(payload))
                print("✓ Subscribed to new token events")
                
                # Subscribing to trades made by accounts
                payload = {
                    "method": "subscribeAccountTrade",
                    "keys": ["AArPXm8JatJiuyEffuC1un2Sc835SULa4uQqDcaGpAjV"]
                }
                await websocket.send(json.dumps(payload))
                print("✓ Subscribed to account trade events")
                
                # Subscribing to trades on tokens
                payload = {
                    "method": "subscribeTokenTrade",
                    "keys": ["91WNez8D22NwBssQbkzjy4s2ipFrzpmn5hfvWVe2aY5p"]
                }
                await websocket.send(json.dumps(payload))
                print("✓ Subscribed to token trade events")
                
                print("\n👀 Monitoring for messages...\n")
                
                while True:  # Add message receiving loop
                    try:
                        message = await websocket.recv()
                        
                        # Parse and store message
                        try:
                            parsed_data = json.loads(message)
                            
                            # Only process and store messages with signatures
                            if isinstance(parsed_data, dict) and 'signature' in parsed_data:
                                print(f"📩 Received signed transaction: {parsed_data.get('signature', '')[:10]}...")
                                await store_raw_message(message, parsed_data)
                            else:
                                print("⏭️ Skipping message without signature")
                                
                        except json.JSONDecodeError as e:
                            print(f"❌ Failed to parse JSON: {str(e)}")
                            continue
                            
                    except Exception as e:
                        print(f"❌ Error processing message: {str(e)}")
                        raise  # Re-raise to trigger reconnection

        except websockets.exceptions.ConnectionClosed:
            print("❌ Connection closed. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ Connection error: {str(e)}")
            print("Retrying in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        print("\n" + "=" * 80)
        print("🚀 PumpPortal Data Collector")
        print("=" * 80 + "\n")
        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(subscribe())
        
    except KeyboardInterrupt:
        print("\n\n👋 Data collection stopped by user") 