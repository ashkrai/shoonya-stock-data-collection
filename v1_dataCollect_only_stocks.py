from datetime import datetime, time as dtime
import sqlite3
import threading
import queue
import logging
import sys
import time
import yaml
import pyotp
import re
import csv
import signal # Import the signal module
import ssl    # Import the ssl module
import websocket # Import the websocket library for patching

# --- START SSL VERIFICATION FIX ---

# FIX 1: Global context for standard HTTPS requests (like login/logout)
# This addresses SSL errors for RESTful API calls made by the library.
ssl._create_default_https_context = ssl._create_unverified_context

# FIX 2: Monkey-patch the websocket library to disable verification
# This directly targets the websocket connection, as it may ignore the global setting.
# We save the original function first.
_original_run_forever = websocket.WebSocketApp.run_forever

def _patched_run_forever(self, *args, **kwargs):
    """A patched version of run_forever that injects SSL options."""
    if "sslopt" not in kwargs:
        # Add the SSL option to disable certificate verification.
        kwargs["sslopt"] = {"cert_reqs": ssl.CERT_NONE}
    # Call the original function with the modified arguments.
    _original_run_forever(self, *args, **kwargs)

# Apply the patch by replacing the original function with our patched version.
websocket.WebSocketApp.run_forever = _patched_run_forever

# --- END SSL VERIFICATION FIX ---


from api_helper import ShoonyaApiPy

# --- Setup logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Globals and Constants ---
DB_PATH = 'AllStockData.db'
db_queue = queue.Queue()
api = ShoonyaApiPy()
token_to_symbol_map = {}
is_websocket_open = False

# --- Signal Handler for Graceful Shutdown ---
def graceful_shutdown_handler(signum, frame):
    """
    Handles OS signals to initiate a clean shutdown. This is the single entry point for all exit signals.
    """
    logging.warning(f"Received signal {signal.Signals(signum).name}. Initiating graceful shutdown...")
    raise KeyboardInterrupt

def load_config(filepath='config.yml'):
    """Loads the YAML configuration file."""
    try:
        with open(filepath, 'r') as f:
            config = yaml.safe_load(f)
        logging.info("Configuration file loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {filepath}. Please create it.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading configuration file: {e}")
        sys.exit(1)

def sanitize_table_name(name):
    """Sanitizes a string to be a valid SQL table name."""
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
    if table_name and table_name[0].isdigit():
        table_name = '_' + table_name
    return table_name

def db_writer():
    """A dedicated thread to write data from a queue to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    created_tables = set()

    while True:
        try:
            item = db_queue.get()
            if item is None:
                break

            token, timestamp, lp = item
            symbol_name = token_to_symbol_map.get(token)

            if not symbol_name:
                continue

            table_name = sanitize_table_name(symbol_name)

            if table_name not in created_tables:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS "{table_name}" (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        token TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        lp REAL NOT NULL
                    )
                ''')
                cursor.execute(f'''
                    CREATE INDEX IF NOT EXISTS "idx_{table_name}_timestamp" 
                    ON "{table_name}" (timestamp)
                ''')
                conn.commit()
                created_tables.add(table_name)
            
            cursor.execute(f'INSERT INTO "{table_name}" (token, timestamp, lp) VALUES (?, ?, ?)', 
                           (token, timestamp, lp))
            conn.commit()

        except sqlite3.Error as e:
            logging.error(f"Database error in db_writer: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred in db_writer: {e}")

    conn.close()
    logging.info("Database writer thread has shut down.")

def event_handler_feed_update(tick_data):
    """Callback for incoming price updates."""
    if 'lp' in tick_data and 'tk' in tick_data:
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            lp_value = float(tick_data['lp'])
            token = tick_data['tk']
            db_queue.put((token, current_time, lp_value))
        except (ValueError, TypeError) as e:
            logging.error(f"Error processing tick data: {e} - Data: {tick_data}")

def event_handler_order_update(tick_data):
    logging.info(f"ORDER HANDLER: Received update -> {tick_data}")

def close_callback():
    global is_websocket_open
    is_websocket_open = False
    logging.warning("WebSocket connection closed.")

def load_instruments_from_file(filepath):
    """Reads the NSE_symbols.txt file to prepare for subscription."""
    global token_to_symbol_map
    instruments = []
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            try:
                token_idx, symbol_idx, instrument_idx = header.index('Token'), header.index('Symbol'), header.index('Instrument')
            except ValueError as e:
                logging.error(f"Header column not found in {filepath}: {e}. Expected 'Token', 'Symbol', 'Instrument'.")
                return []

            for row in reader:
                if len(row) > max(token_idx, symbol_idx, instrument_idx) and row[instrument_idx] == 'EQ':
                    token, symbol = row[token_idx], row[symbol_idx]
                    token_to_symbol_map[token] = symbol
                    instruments.append(f"NSE|{token}")
                    
        logging.info(f"Loaded {len(instruments)} equity instruments from {filepath}")
        return instruments
    except FileNotFoundError:
        logging.error(f"Instrument file not found: {filepath}. Download and place 'NSE_symbols.txt' in the script's directory.")
        return []
    except Exception as e:
        logging.error(f"Error reading instrument file {filepath}: {e}")
        return []

def main():
    """Main function to run the application."""
    global is_websocket_open

    signal.signal(signal.SIGINT, graceful_shutdown_handler)
    signal.signal(signal.SIGTERM, graceful_shutdown_handler)
    
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, graceful_shutdown_handler)
    if hasattr(signal, 'SIGQUIT'):
        signal.signal(signal.SIGQUIT, graceful_shutdown_handler)

    config = load_config()
    creds = config['shoonya_creds']

    instruments_to_subscribe = load_instruments_from_file('NSE_symbols.txt')
    if not instruments_to_subscribe:
        sys.exit(1)
    
    writer_thread = threading.Thread(target=db_writer, daemon=True)
    writer_thread.start()

    def open_callback_with_subscriptions():
        global is_websocket_open
        is_websocket_open = True
        logging.info("WebSocket connection opened. Subscribing to instruments...")
        if instruments_to_subscribe:
            chunk_size = 500
            for i in range(0, len(instruments_to_subscribe), chunk_size):
                api.subscribe(instruments_to_subscribe[i:i + chunk_size])
                logging.info(f"Subscribed to chunk {i//chunk_size + 1}")
                time.sleep(1)
    
    ret = None
    try:
        totp = pyotp.TOTP(creds['totp_secret'])
        ret = api.login(
            userid=creds['user'], password=creds['password'], twoFA=totp.now(),
            vendor_code=creds['vendor_code'], api_secret=creds['api_key'], imei=creds['imei']
        )

        if ret and ret.get('stat') == 'Ok':
            logging.info("Login successful.")
            api.start_websocket(
                order_update_callback=event_handler_order_update,
                subscribe_callback=event_handler_feed_update,
                socket_open_callback=open_callback_with_subscriptions,
                socket_close_callback=close_callback
            )
            logging.info("WebSocket started. Main loop is running. Awaiting exit signal...")
            while True:
                time.sleep(5) 
        else:
            logging.error(f"Login failed: {ret}")
            sys.exit(1)

    except KeyboardInterrupt:
        logging.warning("Shutdown initiated by signal...")
    except Exception as e:
        logging.error(f"A critical error occurred in main: {e}", exc_info=True)
    finally:
        logging.info("Starting graceful shutdown process...")
        
        if is_websocket_open:
            logging.info("Closing WebSocket connection...")
            api.close_websocket()
            time.sleep(1)

        if ret and ret.get('stat') == 'Ok':
            logging.info("Logging out from Shoonya API...")
            api.logout()

        logging.info("Signaling database writer to shut down...")
        db_queue.put(None)
        if 'writer_thread' in locals() and writer_thread.is_alive():
            writer_thread.join()
        
        logging.info("Program has been shut down gracefully.")


if __name__ == "__main__":
    main()
