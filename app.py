import os
import pandas as pd
import numpy as np
import sqlite3
import time
from datetime import datetime, timedelta
import pytz
import ccxt
import pandas_ta as ta
from telegram import Bot
import telegram
import logging
import threading
import requests
import base64
from flask import Flask, render_template, jsonify, request, redirect, url_for, session, flash, send_file
import atexit
import asyncio
import secrets
import smtplib
from email.mime.text import MIMEText
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import re
import uuid
from zoneinfo import ZoneInfo

pd.set_option('future.no_silent_downcasting', True)

# Custom formatter for EU timezone
class EUFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, tz=pytz.utc):
        super().__init__(fmt, datefmt)
        self.tz = tz

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('td_sto.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.debug("Loaded environment variables from .env file")
except ImportError:
    logger.warning("python-dotenv not installed. Relying on system environment variables.")

werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_handler = logging.StreamHandler()
werkzeug_handler.setFormatter(EUFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
werkzeug_logger.handlers = [werkzeug_handler, logging.FileHandler('td_sto.log')]
werkzeug_logger.setLevel(logging.DEBUG)

# Flask app setup
app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "CHAT_ID")
SYMBOL = os.getenv("SYMBOL", "SYMBOL")
TIMEFRAME = os.getenv("TIMEFRAME", "TIMEFRAME")
TIMEFRAMES = int(os.getenv("INTER_SECONDS", "60"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "2.0"))
TAKE_PROFIT_PERCENT = float(os.getenv("TAKE_PROFIT_PERCENT", "5.0"))
STOP_AFTER_SECONDS = float(os.getenv("STOP_AFTER_SECONDS", "0"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "GITHUB_REPO")
GITHUB_PATH = os.getenv("GITHUB_PATH", "GITHUB_PATH")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "BINANCE_API_SECRET")
AMOUNTS = float(os.getenv("AMOUNTS", "100.0"))
ADMIN_PASSPHRASE = os.getenv("ADMIN_PASSPHRASE", "admin_secret_passphrase")
EMAIL_SERVER = os.getenv("EMAIL_SERVER", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "your_email@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "your_email_password")
SMS_API_KEY = os.getenv("SMS_API_KEY", "your_sms_api_key")
PASSWORD_RESET_TOKEN_EXPIRY = int(os.getenv("PASSWORD_RESET_TOKEN_EXPIRY", 3600))
UPLOAD_FOLDER = 'static/Uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}

# Configure upload folder
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# GitHub API setup
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Database path
db_path = 'rnn_bot.db'

# Timezone setup
EU_TZ = ZoneInfo("Europe/Berlin")

# Global state
bot_thread = None
bot_active = True
bot_lock = threading.Lock()
db_lock = threading.Lock()
conn = None
exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
})
position = None
buy_price = None
total_profit = 0
pause_duration = 0
pause_start = None
tracking_enabled = True
last_sell_profit = 0
tracking_has_buy = False
tracking_buy_price = None
total_return_profit = 0
start_time = datetime.now(EU_TZ)
stop_time = None
last_valid_price = None

# Helper functions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_pin():
    return str(secrets.randbelow(1000000)).zfill(6)

def send_email(to_email, pin, is_resend=False):
    try:
        subject = 'IFYBNG Verification PIN' if not is_resend else 'IFYBNG Verification PIN (Resend)'
        msg = MIMEText(f"Your verification PIN is: {pin}")
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = to_email
        with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        logger.info(f"{'Resent' if is_resend else 'Sent'} verification email to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Error sending email to {to_email}: {e}")
        return False

def send_sms(phone, pin, is_resend=False):
    try:
        # Placeholder for SMS API (e.g., Twilio)
        logger.info(f"{'Resent' if is_resend else 'Sent'} SMS to {phone} with PIN {pin} (mock)")
        return True
    except Exception as e:
        logger.error(f"Error sending SMS to {phone}: {e}")
        return False

def upload_to_github(file_path, file_name):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return
        logger.debug(f"Uploading {file_name} to GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        sha = None
        if response.status_code == 200:
            sha = response.json().get("sha")
        payload = {
            "message": f"Update {file_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": content
        }
        if sha:
            payload["sha"] = sha
        response = requests.put(GITHUB_API_URL, headers=HEADERS, json=payload)
        if response.status_code in [200, 201]:
            logger.info(f"Successfully uploaded {file_name} to GitHub")
        else:
            logger.error(f"Failed to upload {file_name} to GitHub: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error uploading {file_name} to GitHub: {e}")

def download_from_github(file_name, destination_path):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return False
        logger.debug(f"Downloading {file_name} from GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        if response.status_code == 404:
            logger.info(f"No {file_name} found in GitHub repository. Starting with a new database.")
            return False
        elif response.status_code != 200:
            logger.error(f"Failed to fetch {file_name} from GitHub: {response.status_code} - {response.text}")
            return False
        content = base64.b64decode(response.json()["content"])
        with open(destination_path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded {file_name} from GitHub to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_name} from GitHub: {e}")
        return False

def keep_alive():
    while True:
        try:
            requests.get('https://www.google.com')
            logger.debug("Keep-alive ping sent")
            time.sleep(300)
        except Exception as e:
            logger.error(f"Keep-alive error: {e}")
            time.sleep(60)

def periodic_db_backup():
    while True:
        try:
            with db_lock:
                if os.path.exists(db_path) and conn is not None:
                    logger.info("Performing periodic database backup to GitHub")
                    upload_to_github(db_path, 'rnn_bot.db')
                else:
                    logger.warning("Database file or connection not available for periodic backup")
            time.sleep(300)  # Backup every 5 minutes
        except Exception as e:
            logger.error(f"Error during periodic database backup: {e}")
            time.sleep(60)

def setup_database(first_attempt=False):
    global conn
    with db_lock:
        for attempt in range(3):
            try:
                logger.info(f"Database setup attempt {attempt + 1}/3")
                if not os.path.exists(db_path):
                    logger.info(f"Database file {db_path} does not exist. Creating new database.")
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                else:
                    try:
                        test_conn = sqlite3.connect(db_path, check_same_thread=False)
                        c = test_conn.cursor()
                        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        logger.info(f"Existing database found at {db_path}, tables: {c.fetchall()}")
                        test_conn.close()
                    except sqlite3.DatabaseError as e:
                        logger.error(f"Existing database at {db_path} is corrupted: {e}")
                        os.remove(db_path)
                        conn = sqlite3.connect(db_path, check_same_thread=False)

                if conn is None:
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                logger.info(f"Connected to database at {db_path}")

                c = conn.cursor()
                # Signals table
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE signals (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            time TEXT,
                            action TEXT,
                            symbol TEXT,
                            price REAL,
                            message TEXT,
                            timeframe TEXT,
                            strategy TEXT,
                            win_rate TEXT,
                            lose_rate TEXT,
                            hold_rate TEXT,
                            buy_rate TEXT,
                            sell_rate TEXT,
                            total_winning REAL,
                            total_lose REAL,
                            profit REAL,
                            total_profit REAL,
                            return_profit REAL,
                            total_return_profit REAL
                        )
                    ''')
                    logger.info("Created new signals table")
                    conn.commit()

                # Trades table
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE trades (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            time TEXT,
                            action TEXT,
                            symbol TEXT,
                            price REAL,
                            open_price REAL,
                            close_price REAL,
                            volume REAL,
                            percent_change REAL,
                            stop_loss REAL,
                            take_profit REAL,
                            profit REAL,
                            total_profit REAL,
                            return_profit REAL,
                            total_return_profit REAL,
                            ema1 REAL,
                            ema2 REAL,
                            rsi REAL,
                            k REAL,
                            d REAL,
                            j REAL,
                            diff REAL,
                            diff1e REAL,
                            diff2m REAL,
                            diff3k REAL,
                            macd REAL,
                            macd_signal REAL,
                            macd_hist REAL,
                            macd_hollow REAL,
                            lst_diff REAL,
                            supertrend REAL,
                            supertrend_trend INTEGER,
                            stoch_rsi REAL,
                            stoch_k REAL,
                            stoch_d REAL,
                            obv REAL,
                            message TEXT,
                            timeframe TEXT,
                            order_id TEXT,
                            strategy TEXT
                        )
                    ''')
                    logger.info("Created new trades table")
                    conn.commit()

                # Users table
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            first_name TEXT,
                            last_name TEXT,
                            email TEXT UNIQUE,
                            phone TEXT UNIQUE,
                            age TEXT,
                            country TEXT,
                            state TEXT,
                            address TEXT,
                            occupation TEXT,
                            id_card_front TEXT,
                            id_card_back TEXT,
                            password TEXT,
                            email_verified INTEGER DEFAULT 0,
                            phone_verified INTEGER DEFAULT 0,
                            status TEXT DEFAULT 'pending',
                            created_at TEXT,
                            warning_message TEXT,
                            warning_expiry TEXT
                        )
                    ''')
                    logger.info("Created new users table")
                    conn.commit()

                # Admin logs table
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admin_logs';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE admin_logs (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            admin_id INTEGER,
                            action TEXT,
                            target_user_id INTEGER,
                            details TEXT,
                            timestamp TEXT
                        )
                    ''')
                    logger.info("Created new admin_logs table")
                    conn.commit()

                # Password reset tokens table
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='password_reset_tokens';")
                if not c.fetchone():
                    c.execute('''
                        CREATE TABLE password_reset_tokens (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER,
                            token TEXT UNIQUE,
                            created_at TEXT,
                            expires_at TEXT
                        )
                    ''')
                    logger.info("Created new password_reset_tokens table")
                    conn.commit()

                c.execute("PRAGMA table_info(trades);")
                existing_columns = {col[1] for col in c.fetchall()}
                required_columns = {
                    'time': 'TEXT', 'action': 'TEXT', 'symbol': 'TEXT', 'price': 'REAL',
                    'open_price': 'REAL', 'close_price': 'REAL', 'volume': 'REAL',
                    'percent_change': 'REAL', 'stop_loss': 'REAL', 'take_profit': 'REAL',
                    'profit': 'REAL', 'total_profit': 'REAL', 'return_profit': 'REAL',
                    'total_return_profit': 'REAL', 'ema1': 'REAL', 'ema2': 'REAL',
                    'rsi': 'REAL', 'k': 'REAL', 'd': 'REAL', 'j': 'REAL', 'diff': 'REAL',
                    'diff1e': 'REAL', 'diff2m': 'REAL', 'diff3k': 'REAL', 'macd': 'REAL',
                    'macd_signal': 'REAL', 'macd_hist': 'REAL', 'macd_hollow': 'REAL',
                    'lst_diff': 'REAL', 'supertrend': 'REAL', 'supertrend_trend': 'INTEGER',
                    'stoch_rsi': 'REAL', 'stoch_k': 'REAL', 'stoch_d': 'REAL', 'obv': 'REAL',
                    'message': 'TEXT', 'timeframe': 'TEXT', 'order_id': 'TEXT', 'strategy': 'TEXT'
                }

                for col, col_type in required_columns.items():
                    if col not in existing_columns:
                        c.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type};")
                        conn.commit()
                        logger.info(f"Added column {col} to trades table")

                if not first_attempt:
                    if download_from_github('rnn_bot.db', db_path):
                        try:
                            test_conn = sqlite3.connect(db_path, check_same_thread=False)
                            c = test_conn.cursor()
                            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                            logger.info(f"Downloaded database is valid, tables: {c.fetchall()}")
                            test_conn.close()
                        except sqlite3.DatabaseError as e:
                            logger.error(f"Downloaded database is corrupted: {e}")
                            os.remove(db_path)
                            conn = sqlite3.connect(db_path, check_same_thread=False)

                logger.info(f"Database initialized successfully at {db_path}")
                upload_to_github(db_path, 'rnn_bot.db')
                return True
            except sqlite3.Error as e:
                logger.error(f"SQLite error during database setup (attempt {attempt + 1}/3): {e}")
                if conn:
                    conn.close()
                    conn = None
                time.sleep(2)
        logger.error("Failed to initialize database after 3 attempts.")
        return False

# Initialize database
if not setup_database(first_attempt=True):
    logger.critical("Failed to initialize database. Flask routes may fail.")

# Flask routes
@app.route('/')
def index():
    if 'user_id' not in session:
        flash("Please login first.")
        return redirect(url_for('login'))
    global conn, stop_time
    status = "active" if bot_active else "stopped"
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                if not setup_database(first_attempt=True):
                    logger.critical("Failed to reinitialize database for index route")
                    return "<h1>Error</h1><p>Database unavailable. Please try again later.</p>", 503

            c = conn.cursor()
            # Fetch the latest signal
            c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 1")
            signal_row = c.fetchone()
            signal = None
            if signal_row:
                signal = dict(zip(['id', 'time', 'action', 'symbol', 'price', 'message', 'timeframe', 'strategy',
                                   'win_rate', 'lose_rate', 'hold_rate', 'buy_rate', 'sell_rate', 'total_winning',
                                   'total_lose', 'profit', 'total_profit', 'return_profit', 'total_return_profit'], signal_row))

            # Fetch recent trades
            c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 48")
            rows = c.fetchall()
            columns = [col[0] for col in c.description]
            trades = [dict(zip(columns, row)) for row in rows]

            numeric_fields = [
                'price', 'open_price', 'close_price', 'volume', 'percent_change', 'stop_loss',
                'take_profit', 'profit', 'total_profit', 'return_profit', 'total_return_profit',
                'ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff', 'diff1e', 'diff2m', 'diff3k',
                'macd', 'macd_signal', 'macd_hist', 'macd_hollow', 'lst_diff', 'supertrend',
                'stoch_rsi', 'stoch_k', 'stoch_d', 'obv'
            ]

            for trade in trades:
                for field in numeric_fields:
                    trade[field] = float(trade[field]) if trade[field] is not None else 0.0

            # Fetch user data
            user = None
            profile_status = 'incomplete'
            if 'user_id' in session:
                c.execute("SELECT * FROM users WHERE id=?", (session['user_id'],))
                user_row = c.fetchone()
                if user_row:
                    user = dict(zip([col[0] for col in c.description], user_row))
                    logger.debug(f"User data: {user}")
                    is_complete = all([
                        user.get('first_name'), user.get('last_name'), user.get('email'), user.get('phone'),
                        user.get('age'), user.get('country'), user.get('state'), user.get('address'),
                        user.get('occupation'), user.get('email_verified'), user.get('phone_verified')
                    ])
                    has_id_cards = user.get('id_card_front') and user.get('id_card_back')
                    has_warning = False
                    try:
                        if user.get('warning_message') and user.get('warning_expiry'):
                            warning_expiry = datetime.strptime(user['warning_expiry'], "%Y-%m-%d %H:%M:%S")
                            has_warning = warning_expiry > datetime.now(EU_TZ)
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error parsing warning_expiry for user {session['user_id']}: {e}")
                        has_warning = False

                    if has_warning:
                        profile_status = 'warning'
                    elif is_complete:
                        profile_status = 'complete'
                    elif has_id_cards:
                        profile_status = 'id_uploaded'
                    else:
                        profile_status = 'incomplete'
                else:
                    logger.warning(f"No user found for user_id: {session['user_id']}")

            # Get performance metrics
            metrics = get_performance_metrics()
            if signal:
                signal.update(metrics)

            stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
            current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
            elapsed = time.time() - start_time
            logger.info(f"Rendering index.html: status={status}, timeframe={TIMEFRAME}, trades={len(trades)}, query_time={elapsed:.3f}s")

            return render_template(
                'index.html',
                signal=signal,
                status=status,
                timeframe=TIMEFRAME,
                trades=trades,
                stop_time=stop_time_str,
                current_time=current_time,
                metrics=metrics,
                user=user,
                profile_status=profile_status
            )

        except Exception as e:
            logger.error(f"Error rendering index.html: {e}")
            return "<h1>Error</h1><p>Failed to load page. Please try again later.</p>", 500

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        first_name = request.form.get('first_name')[:50]
        last_name = request.form.get('last_name')[:50]
        email = request.form.get('email')
        phone = request.form.get('phone')
        age = request.form.get('age')[:50]
        country = request.form.get('country')[:50]
        state = request.form.get('state')[:50]
        address = request.form.get('address')[:250]
        occupation = request.form.get('occupation')[:100]
        password = request.form.get('password')
        id_card_front = request.files.get('id_card_front')
        id_card_back = request.files.get('id_card_back')
        skip_verification = request.form.get('skip_verification') == 'on'

        # Validation
        if not re.match(r"^\+\d{1,3}\d{10}$", phone):
            flash("Phone number must start with country code (e.g., +2348127000001)")
            return redirect(url_for('register'))
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            flash("Invalid email address")
            return redirect(url_for('register'))
        if len(password) < 8:
            flash("Password must be at least 8 characters")
            return redirect(url_for('register'))

        # Handle file uploads
        id_front_path = id_back_path = None
        if id_card_front and allowed_file(id_card_front.filename):
            filename = secure_filename(f"{secrets.token_hex(8)}_front_{id_card_front.filename}")
            id_front_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            id_card_front.save(id_front_path)
        if id_card_back and allowed_file(id_card_back.filename):
            filename = secure_filename(f"{secrets.token_hex(8)}_back_{id_card_back.filename}")
            id_back_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            id_card_back.save(id_back_path)

        # Generate PINs and store creation time
        email_pin = generate_pin()
        phone_pin = generate_pin()
        pin_created_at = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")

        # Store user data
        try:
            with db_lock:
                c = conn.cursor()
                c.execute('''
                    INSERT INTO users (first_name, last_name, email, phone, age, country, state, address,
                    occupation, id_card_front, id_card_back, password, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    first_name, last_name, email, phone, age, country, state, address,
                    occupation, id_front_path, id_back_path, generate_password_hash(password),
                    pin_created_at
                ))
                conn.commit()
                user_id = c.lastrowid
                if not skip_verification:
                    session['pending_email'] = email
                    session['pending_phone'] = phone
                    session['email_pin'] = email_pin
                    session['phone_pin'] = phone_pin
                    session['pin_created_at'] = pin_created_at
                    session['user_id'] = user_id
                    if send_email(email, email_pin) and send_sms(phone, phone_pin):
                        flash("Registration successful. Please verify your email and phone.")
                        return redirect(url_for('verify'))
                    else:
                        flash("Failed to send verification PINs. You can resend or skip verification.")
                        return redirect(url_for('verify'))
                else:
                    session['user_id'] = user_id
                    flash("Registration successful. You can complete verification later from your profile.")
                    return redirect(url_for('index'))
        except sqlite3.IntegrityError:
            flash("Email or phone number already registered.")
            return redirect(url_for('register'))
        except Exception as e:
            logger.error(f"Error during registration: {e}")
            flash("Registration failed. Please try again.")
            return redirect(url_for('register'))
    return render_template('register.html')

@app.route('/verify', methods=['GET', 'POST'])
def verify():
    if 'pending_email' not in session:
        flash("No pending verification found.")
        return redirect(url_for('register'))
    if request.method == 'POST':
        if request.form.get('action') == 'resend':
            email_pin = generate_pin()
            phone_pin = generate_pin()
            pin_created_at = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
            session['email_pin'] = email_pin
            session['phone_pin'] = phone_pin
            session['pin_created_at'] = pin_created_at
            if send_email(session['pending_email'], email_pin, is_resend=True) and send_sms(session['pending_phone'], phone_pin, is_resend=True):
                flash("New verification PINs sent.")
            else:
                flash("Failed to resend verification PINs.")
            return redirect(url_for('verify'))
        elif request.form.get('action') == 'skip':
            flash("Verification skipped. You can complete it later from your profile.")
            return redirect(url_for('index'))
        else:
            email_pin = request.form.get('email_pin')
            phone_pin = request.form.get('phone_pin')
            pin_created_at = datetime.strptime(session.get('pin_created_at'), "%Y-%m-%d %H:%M:%S")
            if (datetime.now(EU_TZ) - pin_created_at).total_seconds() > 180:
                flash("Verification PINs have expired. Please resend.")
                return redirect(url_for('verify'))
            if email_pin == session.get('email_pin') and phone_pin == session.get('phone_pin'):
                try:
                    with db_lock:
                        c = conn.cursor()
                        c.execute("UPDATE users SET email_verified=1, phone_verified=1, status='pending_admin' WHERE id=?", (session['user_id'],))
                        conn.commit()
                        flash("Verification successful. Awaiting admin approval.")
                        session.pop('pending_email', None)
                        session.pop('pending_phone', None)
                        session.pop('email_pin', None)
                        session.pop('phone_pin', None)
                        session.pop('pin_created_at', None)
                        session.pop('user_id', None)
                        return redirect(url_for('login'))
                except Exception as e:
                    logger.error(f"Error during verification: {e}")
                    flash("Verification failed. Please try again.")
            else:
                flash("Invalid PINs. Please try again or resend.")
    return render_template('verify.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        try:
            with db_lock:
                c = conn.cursor()
                c.execute("SELECT id, password, status FROM users WHERE email=?", (email,))
                user = c.fetchone()
                if user and check_password_hash(user[1], password):
                    if user[2] in ['active', 'pending_admin', 'pending']:
                        session['user_id'] = user[0]
                        flash("Login successful.")
                        return redirect(url_for('index'))
                    else:
                        flash("Invalid email or password.")
                else:
                    flash("Invalid email or password.")
        except Exception as e:
            logger.error(f"Error during login: {e}")
            flash("Login failed. Please try again.")
    return render_template('login.html')

@app.route('/profile', methods=['GET', 'POST'])
def profile():
    if 'user_id' not in session:
        flash("Please login first.")
        return redirect(url_for('login'))
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE id=?", (session['user_id'],))
            user = c.fetchone()
            if not user:
                flash("User not found.")
                return redirect(url_for('login'))
            user_dict = dict(zip([col[0] for col in c.description], user))
            
            is_complete = all([user_dict['first_name'], user_dict['last_name'], user_dict['email'], user_dict['phone'], 
                               user_dict['age'], user_dict['country'], user_dict['state'], user_dict['address'], 
                               user_dict['occupation'], user_dict['email_verified'], user_dict['phone_verified']])
            has_id_cards = user_dict['id_card_front'] and user_dict['id_card_back']
            has_warning = user_dict['warning_message'] and user_dict['warning_expiry'] and datetime.strptime(user_dict['warning_expiry'], "%Y-%m-%d %H:%M:%S") > datetime.now(EU_TZ)
            if has_warning:
                profile_status = 'warning'
            elif is_complete:
                profile_status = 'complete'
            elif has_id_cards:
                profile_status = 'id_uploaded'
            else:
                profile_status = 'incomplete'

            if request.method == 'POST':
                if request.form.get('action') == 'update_profile':
                    first_name = request.form.get('first_name')[:50]
                    last_name = request.form.get('last_name')[:50]
                    age = request.form.get('age')[:50]
                    country = request.form.get('country')[:50]
                    state = request.form.get('state')[:50]
                    address = request.form.get('address')[:250]
                    occupation = request.form.get('occupation')[:100]
                    id_card_front = request.files.get('id_card_front')
                    id_card_back = request.files.get('id_card_back')

                    updates = {}
                    if first_name:
                        updates['first_name'] = first_name
                    if last_name:
                        updates['last_name'] = last_name
                    if age:
                        updates['age'] = age
                    if country:
                        updates['country'] = country
                    if state:
                        updates['state'] = state
                    if address:
                        updates['address'] = address
                    if occupation:
                        updates['occupation'] = occupation
                    if id_card_front and allowed_file(id_card_front.filename):
                        filename = secure_filename(f"{secrets.token_hex(8)}_front_{id_card_front.filename}")
                        id_front_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                        id_card_front.save(id_front_path)
                        updates['id_card_front'] = id_front_path
                    if id_card_back and allowed_file(id_card_back.filename):
                        filename = secure_filename(f"{secrets.token_hex(8)}_back_{id_card_back.filename}")
                        id_back_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                        id_card_back.save(id_back_path)
                        updates['id_card_back'] = id_back_path

                    if updates:
                        query = "UPDATE users SET " + ", ".join(f"{k}=?" for k in updates.keys()) + " WHERE id=?"
                        c.execute(query, list(updates.values()) + [session['user_id']])
                        conn.commit()
                        flash("Profile updated successfully.")

                    return redirect(url_for('profile'))
                elif request.form.get('action') == 'verify':
                    email_pin = generate_pin()
                    phone_pin = generate_pin()
                    pin_created_at = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    session['pending_email'] = user_dict['email']
                    session['pending_phone'] = user_dict['phone']
                    session['email_pin'] = email_pin
                    session['phone_pin'] = phone_pin
                    session['pin_created_at'] = pin_created_at
                    if send_email(user_dict['email'], email_pin) and send_sms(user_dict['phone'], phone_pin):
                        flash("Verification PINs sent.")
                        return redirect(url_for('verify'))
                    else:
                        flash("Failed to send verification PINs.")
                        return redirect(url_for('profile'))

            return render_template('profile.html', user=user_dict, profile_status=profile_status)
    except Exception as e:
        logger.error(f"Error loading profile: {e}")
        flash("Error loading profile.")
        return redirect(url_for('index'))

@app.route('/admin', methods=['GET', 'POST'])
def admin():
    if 'user_id' not in session:
        flash("Please login first.")
        return redirect(url_for('login'))
    if request.method == 'POST':
        if request.form.get('action') == 'search_incomplete':
            try:
                with db_lock:
                    c = conn.cursor()
                    c.execute("""
                        SELECT id, first_name, last_name, email, phone, age, country, state, address, occupation,
                               id_card_front, id_card_back, status, created_at, warning_message, warning_expiry
                        FROM users
                        WHERE first_name IS NULL OR last_name IS NULL OR age IS NULL OR country IS NULL OR
                              state IS NULL OR address IS NULL OR occupation IS NULL OR
                              email_verified = 0 OR phone_verified = 0
                    """)
                    users = [dict(zip(['id', 'first_name', 'last_name', 'email', 'phone', 'age', 'country', 'state',
                                       'address', 'occupation', 'id_card_front', 'id_card_back', 'status', 'created_at',
                                       'warning_message', 'warning_expiry'], row)) for row in c.fetchall()]
                    c.execute("SELECT id, admin_id, action, target_user_id, details, timestamp FROM admin_logs")
                    admin_logs = [dict(zip(['id', 'admin_id', 'action', 'target_user_id', 'details', 'timestamp'], row)) for row in c.fetchall()]
                    return render_template('admin.html', users=users, admin_logs=admin_logs, search_type='incomplete')
            except Exception as e:
                logger.error(f"Error searching incomplete profiles: {e}")
                flash("Error performing search.")
                return redirect(url_for('admin'))
        elif request.form.get('action') == 'issue_warning':
            user_id = request.form.get('user_id')
            warning_message = request.form.get('warning_message')
            warning_days = int(request.form.get('warning_days', 7))
            warning_expiry = (datetime.now(EU_TZ) + timedelta(days=warning_days)).strftime("%Y-%m-%d %H:%M:%S")
            try:
                with db_lock:
                    c = conn.cursor()
                    c.execute("UPDATE users SET warning_message=?, warning_expiry=? WHERE id=?", (warning_message, warning_expiry, user_id))
                    conn.commit()
                    c.execute("INSERT INTO admin_logs (admin_id, action, target_user_id, details, timestamp) VALUES (?, ?, ?, ?, ?)",
                              (session['user_id'], 'warning', user_id, f"Issued warning: {warning_message}", datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit()
                    flash(f"Warning issued to user {user_id}.")
                    return redirect(url_for('admin'))
            except Exception as e:
                logger.error(f"Error issuing warning: {e}")
                flash("Error issuing warning.")
                return redirect(url_for('admin'))
        else:
            passphrase = request.form.get('passphrase')
            if passphrase != ADMIN_PASSPHRASE:
                flash("Invalid admin passphrase.")
                return redirect(url_for('admin'))
            session['is_admin'] = True
            try:
                with db_lock:
                    c = conn.cursor()
                    c.execute("SELECT id, first_name, last_name, email, phone, age, country, state, address, occupation, id_card_front, id_card_back, status, created_at, warning_message, warning_expiry FROM users")
                    users = [dict(zip(['id', 'first_name', 'last_name', 'email', 'phone', 'age', 'country', 'state', 'address', 'occupation', 'id_card_front', 'id_card_back', 'status', 'created_at', 'warning_message', 'warning_expiry'], row)) for row in c.fetchall()]
                    c.execute("SELECT id, admin_id, action, target_user_id, details, timestamp FROM admin_logs")
                    admin_logs = [dict(zip(['id', 'admin_id', 'action', 'target_user_id', 'details', 'timestamp'], row)) for row in c.fetchall()]
                    return render_template('admin.html', users=users, admin_logs=admin_logs, search_type='all')
            except Exception as e:
                logger.error(f"Error loading admin panel: {e}")
                flash("Error loading admin panel.")
                return redirect(url_for('index'))
    return render_template('admin_login.html')

@app.route('/admin/action', methods=['POST'])
def admin_action():
    if 'user_id' not in session or not session.get('is_admin'):
        flash("Unauthorized access.")
        return redirect(url_for('login'))
    action = request.form.get('action')
    user_id = request.form.get('user_id')
    try:
        with db_lock:
            c = conn.cursor()
            if action == 'approve':
                c.execute("UPDATE users SET status='active' WHERE id=?", (user_id,))
                conn.commit()
                c.execute("INSERT INTO admin_logs (admin_id, action, target_user_id, details, timestamp) VALUES (?, ?, ?, ?, ?)",
                          (session['user_id'], 'approve', user_id, f"Approved user {user_id}", datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                flash(f"User {user_id} approved.")
            elif action == 'block':
                c.execute("UPDATE users SET status='blocked' WHERE id=?", (user_id,))
                conn.commit()
                c.execute("INSERT INTO admin_logs (admin_id, action, target_user_id, details, timestamp) VALUES (?, ?, ?, ?, ?)",
                          (session['user_id'], 'block', user_id, f"Blocked user {user_id}", datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                flash(f"User {user_id} blocked.")
            elif action == 'delete':
                c.execute("SELECT id_card_front, id_card_back FROM users WHERE id=?", (user_id,))
                user = c.fetchone()
                if user:
                    for path in [user[0], user[1]]:
                        if path and os.path.exists(path):
                            os.remove(path)
                c.execute("DELETE FROM users WHERE id=?", (user_id,))
                conn.commit()
                c.execute("INSERT INTO admin_logs (admin_id, action, target_user_id, details, timestamp) VALUES (?, ?, ?, ?, ?)",
                          (session['user_id'], 'delete', user_id, f"Deleted user {user_id}", datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                flash(f"User {user_id} deleted.")
            return redirect(url_for('admin'))
    except Exception as e:
        logger.error(f"Error performing admin action: {e}")
        flash("Error performing admin action.")
        return redirect(url_for('admin'))

@app.route('/reset_password', methods=['GET', 'POST'])
def reset_password():
    if request.method == 'POST':
        email = request.form.get('email')
        try:
            with db_lock:
                c = conn.cursor()
                c.execute("SELECT id FROM users WHERE email=?", (email,))
                user = c.fetchone()
                if user:
                    token = str(uuid.uuid4())
                    created_at = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    expires_at = (datetime.now(EU_TZ) + timedelta(seconds=PASSWORD_RESET_TOKEN_EXPIRY)).strftime("%Y-%m-%d %H:%M:%S")
                    c.execute("INSERT INTO password_reset_tokens (user_id, token, created_at, expires_at) VALUES (?, ?, ?, ?)",
                              (user[0], token, created_at, expires_at))
                    conn.commit()
                    reset_link = url_for('reset_password_confirm', token=token, _external=True)
                    msg = MIMEText(f"Click the following link to reset your password: {reset_link}\nThis link expires in 1 hour.")
                    msg['Subject'] = 'IFYBNG Password Reset'
                    msg['From'] = EMAIL_ADDRESS
                    msg['To'] = email
                    with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
                        server.starttls()
                        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
                        server.send_message(msg)
                    flash("Password reset link sent to your email.")
                else:
                    flash("Email not found.")
        except Exception as e:
            logger.error(f"Error requesting password reset: {e}")
            flash("Error requesting password reset.")
        return redirect(url_for('reset_password'))
    return render_template('reset_password.html')

@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password_confirm(token):
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT user_id, expires_at FROM password_reset_tokens WHERE token=?", (token,))
            token_data = c.fetchone()
            if not token_data:
                flash("Invalid or expired reset token.")
                return redirect(url_for('login'))
            user_id, expires_at = token_data
            if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S") < datetime.now(EU_TZ):
                flash("Reset token has expired.")
                c.execute("DELETE FROM password_reset_tokens WHERE token=?", (token,))
                conn.commit()
                return redirect(url_for('login'))
            if request.method == 'POST':
                new_password = request.form.get('new_password')
                if len(new_password) < 8:
                    flash("Password must be at least 8 characters.")
                    return redirect(url_for('reset_password_confirm', token=token))
                c.execute("UPDATE users SET password=? WHERE id=?", (generate_password_hash(new_password), user_id))
                c.execute("DELETE FROM password_reset_tokens WHERE token=?", (token,))
                conn.commit()
                flash("Password reset successfully. Please login.")
                return redirect(url_for('login'))
            return render_template('reset_password_confirm.html', token=token)
    except Exception as e:
        logger.error(f"Error confirming password reset: {e}")
        flash("Error confirming password reset.")
        return redirect(url_for('login'))

@app.route('/search', methods=['GET', 'POST'])
def search():
    if 'user_id' not in session:
        flash("Please login first.")
        return redirect(url_for('login'))
    if request.method == 'POST':
        column = request.form.get('column')
        value = request.form.get('value')
        valid_columns = [
            'id', 'time', 'action', 'symbol', 'price', 'open_price', 'close_price', 'volume',
            'percent_change', 'stop_loss', 'take_profit', 'profit', 'total_profit', 'return_profit',
            'total_return_profit', 'ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff', 'diff1e',
            'diff2m', 'diff3k', 'macd', 'macd_signal', 'macd_hist', 'macd_hollow', 'lst_diff',
            'supertrend', 'stoch_rsi', 'stoch_k', 'stoch_d', 'obv', 'message', 'timeframe',
            'order_id', 'strategy'
        ]
        if column not in valid_columns:
            flash("Invalid search column.")
            return redirect(url_for('search'))
        try:
            with db_lock:
                c = conn.cursor()
                query = f"SELECT * FROM trades WHERE {column} LIKE ?"
                c.execute(query, (f"%{value}%",))
                trades = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
                for trade in trades:
                    for key in valid_columns:
                        if key not in ['time', 'action', 'symbol', 'message', 'timeframe', 'order_id', 'strategy']:
                            trade[key] = float(trade[key]) if trade[key] is not None else 0.0
                if not trades:
                    flash("No results found.")
                return render_template('search.html', trades=trades, column=column, value=value)
        except Exception as e:
            logger.error(f"Error searching trades: {e}")
            flash("Error performing search.")
    return render_template('search.html', trades=[], column='', value='')

@app.route('/status')
def status():
    status = "active" if bot_active else "stopped"
    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
    return jsonify({"status": status, "timeframe": TIMEFRAME, "stop_time": stop_time_str})

@app.route('/performance')
def performance():
    return jsonify({"performance": get_performance()})

@app.route('/trades')
def trades():
    if 'user_id' not in session:
        return jsonify({"error": "Please login first."}), 401
    global conn
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                if not setup_database(first_attempt=True):
                    logger.error("Failed to reinitialize database for trades route")
                    return jsonify({"error": "Database not initialized."}), 503
            c = conn.cursor()
            c.execute("SELECT * FROM trades ORDER BY time DESC LIMIT 48")
            trades = [dict(zip([col[0] for col in c.description], row)) for row in c.fetchall()]
            numeric_fields = [
                'price', 'open_price', 'close_price', 'volume', 'percent_change', 'stop_loss',
                'take_profit', 'profit', 'total_profit', 'return_profit', 'total_return_profit',
                'ema1', 'ema2', 'rsi', 'k', 'd', 'j', 'diff', 'diff1e', 'diff2m', 'diff3k',
                'macd', 'macd_signal', 'macd_hist', 'macd_hollow', 'lst_diff', 'supertrend',
                'stoch_rsi', 'stoch_k', 'stoch_d', 'obv'
            ]
            for trade in trades:
                for field in numeric_fields:
                    trade[field] = float(trade[field]) if trade[field] is not None else 0.0
            elapsed = time.time() - start_time
            logger.debug(f"Fetched {len(trades)} trades for /trades endpoint in {elapsed:.3f}s")
            return jsonify(trades)
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return jsonify({"error": f"Failed to fetch trades: {str(e)}"}), 500

def get_simulated_price(symbol=SYMBOL, exchange=exchange, timeframe=TIMEFRAME, retries=3, delay=5):
    global last_valid_price
    for attempt in range(retries):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=5)
            if not ohlcv:
                logger.warning(f"No data returned for {symbol}. Retrying...")
                time.sleep(delay)
                continue
            data = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EU_TZ)
            data['diff'] = data['Close'] - data['Open']
            non_zero_diff = data[abs(data['diff']) > 0]
            selected_data = non_zero_diff.iloc[-1] if not non_zero_diff.empty else data.iloc[-1]
            last_valid_price = selected_data
            logger.debug(f"Fetched price data: {selected_data.to_dict()}")
            return selected_data
        except Exception as e:
            logger.error(f"Error fetching price (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    if last_valid_price is not None:
        logger.info("Using last valid price data as fallback.")
        return last_valid_price
    return pd.Series({'Open': np.nan, 'Close': np.nan, 'High': np.nan, 'Low': np.nan, 'Volume': np.nan, 'diff': np.nan})

def add_technical_indicators(df):
    start_time = time.time()
    try:
        df = df.copy()
        df['Close'] = df['Close'].ffill()
        df['High'] = df['High'].ffill()
        df['Low'] = df['Low'].ffill()
        df['Volume'] = df['Volume'].ffill()

        df['ema1'] = ta.ema(df['Close'], length=12)
        df['ema2'] = ta.ema(df['Close'], length=26)
        df['rsi'] = ta.rsi(df['Close'], length=14)
        kdj = ta.kdj(df['High'], df['Low'], df['Close'], length=9, signal=3)
        df['k'] = kdj['K_9_3']
        df['d'] = kdj['D_9_3']
        df['j'] = kdj['J_9_3']
        macd = ta.macd(df['Close'], fast=12, slow=26, signal=9)
        df['macd'] = macd['MACD_12_26_9']
        df['macd_signal'] = macd['MACDs_12_26_9']
        df['macd_hist'] = macd['MACDh_12_26_9']
        df['diff'] = df['Close'] - df['Open']
        df['diff1e'] = df['ema1'] - df['ema2']
        df['diff2m'] = df['macd'] - df['macd_signal']
        df['diff3k'] = df['j'] - df['d']
        df['lst_diff'] = df['ema1'].shift(1) - df['ema1']
        df['macd_hollow'] = 0.0
        df.loc[(df['macd_hist'] > 0) & (df['macd_hist'] < df['macd_hist'].shift(1)), 'macd_hollow'] = df['macd_hist']
        df.loc[(df['macd_hist'] < 0) & (df['macd_hist'] > df['macd_hist'].shift(1)), 'macd_hollow'] = -df['macd_hist']

        st_length = 10
        st_multiplier = 3.0
        high_low = df['High'] - df['Low']
        high_close_prev = (df['High'] - df['Close'].shift()).abs()
        low_close_prev = (df['Low'] - df['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
        atr = tr.rolling(st_length, min_periods=1).mean()
        hl2 = (df['High'] + df['Low']) / 2
        basic_upperband = hl2 + (st_multiplier * atr)
        basic_lowerband = hl2 - (st_multiplier * atr)
        final_upperband = basic_upperband.copy()
        final_lowerband = basic_lowerband.copy()

        for i in range(1, len(df)):
            if (basic_upperband.iloc[i] < final_upperband.iloc[i-1]) or (df['Close'].iloc[i-1] > final_upperband.iloc[i-1]):
                final_upperband.iloc[i] = basic_upperband.iloc[i]
            else:
                final_upperband.iloc[i] = final_upperband.iloc[i-1]
            if (basic_lowerband.iloc[i] > final_lowerband.iloc[i-1]) or (df['Close'].iloc[i-1] < final_lowerband.iloc[i-1]):
                final_lowerband.iloc[i] = basic_lowerband.iloc[i]
            else:
                final_lowerband.iloc[i] = final_lowerband.iloc[i-1]

        supertrend = final_upperband.where(df['Close'] <= final_upperband, final_lowerband)
        supertrend_trend = df['Close'] > final_upperband.shift()
        supertrend_trend = supertrend_trend.fillna(True)
        df['supertrend'] = supertrend
        df['supertrend_trend'] = supertrend_trend.astype(int)
        df['supertrend_signal'] = np.where(
            supertrend_trend & ~supertrend_trend.shift().fillna(True), 'buy',
            np.where(~supertrend_trend & supertrend_trend.shift().fillna(True), 'sell', None)
        )

        stoch_rsi_len = 14
        stoch_k_len = 3
        stoch_d_len = 3
        rsi = df['rsi'].ffill()
        rsi_min = rsi.rolling(stoch_rsi_len, min_periods=1).min()
        rsi_max = rsi.rolling(stoch_rsi_len, min_periods=1).max()
        stochrsi = (rsi - rsi_min) / (rsi_max - rsi_min + 1e-12)
        df['stoch_rsi'] = stochrsi
        df['stoch_k'] = stochrsi.rolling(stoch_k_len, min_periods=1).mean() * 100
        df['stoch_d'] = df['stoch_k'].rolling(stoch_d_len, min_periods=1).mean()

        close_diff = df['Close'].diff().fillna(0)
        direction = np.sign(close_diff)
        df['obv'] = (direction * df['Volume']).fillna(0).cumsum()

        elapsed = time.time() - start_time
        logger.debug(f"Technical indicators calculated in {elapsed:.3f}s")
        return df
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error calculating indicators after {elapsed:.3f}s: {e}")
        return df

def ai_decision(df, stop_loss_percent=STOP_LOSS_PERCENT, take_profit_percent=TAKE_PROFIT_PERCENT, position=None, buy_price=None):
    if df.empty or len(df) < 1:
        logger.warning("DataFrame is empty or too small for decision.")
        return "hold", None, None, None

    latest = df.iloc[-1]
    close_price = latest['Close']
    kdj_j = latest['j'] if not pd.isna(latest['j']) else 0.0
    kdj_d = latest['d'] if not pd.isna(latest['d']) else 0.0
    ema1 = latest['ema1'] if not pd.isna(latest['ema1']) else 0.0
    ema2 = latest['ema2'] if not pd.isna(latest['ema2']) else 0.0
    rsi = latest['rsi'] if not pd.isna(latest['rsi']) else 0.0
    macd = latest['macd'] if not pd.isna(latest['macd']) else 0.0
    macd_signal = latest['macd_signal'] if not pd.isna(latest['macd_signal']) else 0.0
    supertrend_trend = latest['supertrend_trend'] if not pd.isna(latest['supertrend_trend']) else 0
    stop_loss = None
    take_profit = None
    action = "hold"
    order_id = None

    usdt_amount = AMOUNTS
    try:
        quantity = usdt_amount / close_price
        market = exchange.load_markets()[SYMBOL]
        quantity_precision = market['precision']['amount']
        quantity = exchange.amount_to_precision(SYMBOL, quantity)
        logger.debug(f"Calculated quantity: {quantity} for {usdt_amount} USDT at price {close_price:.2f}")
    except Exception as e:
        logger.error(f"Error calculating quantity: {e}")
        return "hold", None, None, None

    if position == "long" and buy_price is not None:
        stop_loss = buy_price * (1 - stop_loss_percent / 100)
        take_profit = buy_price * (1 + take_profit_percent / 100)

        if close_price <= stop_loss:
            logger.info("Stop-loss triggered.")
            action = "sell"
        elif close_price >= take_profit:
            logger.info("Take-profit triggered.")
            action = "sell"
        elif (supertrend_trend == 1 and kdj_j > kdj_d and kdj_j > 112.00):
            logger.info(f"Sell triggered by Supertrend: supertrend_trend=Up, close={close_price:.2f}")
            action = "sell"
        elif (kdj_j > kdj_d and kdj_j > 100.00 and ema1 > ema2 and rsi > 60.00):
            logger.info(f"Sell triggered by KDJ/MACD: kdj_j={kdj_j:.2f}, kdj_d={kdj_d:.2f}, close={close_price:.2f}")
            action = "sell"

    if action == "hold" and position is None:
        if (supertrend_trend == 0 and kdj_j < kdj_d and kdj_j < -6.00):
            logger.info(f"Buy triggered by Supertrend: supertrend_trend=Down, close={close_price:.2f}")
            action = "buy"
        elif (kdj_j < kdj_d and kdj_j < -5.00 and ema1 < ema2 and rsi < 19.00):
            logger.info(f"Buy triggered by KDJ/MACD: kdj_j={kdj_j:.2f}, kdj_d={kdj_d:.2f}, close={close_price:.2f}")
            action = "buy"

    if action == "buy" and position is not None:
        logger.debug("Prevented consecutive buy order.")
        action = "hold"
    if action == "sell" and position is None:
        logger.debug("Prevented sell order without open position.")
        action = "hold"

    if action in ["buy", "sell"] and bot_active:
        try:
            if action == "buy":
                order = exchange.create_market_buy_order(SYMBOL, quantity)
                order_id = str(order['id'])
                logger.info(f"Placed market buy order: {order_id}, quantity={quantity}, price={close_price:.2f}")
            elif action == "sell":
                balance = exchange.fetch_balance()
                asset_symbol = SYMBOL.split("/")[0]
                available_amount = balance[asset_symbol]['free']
                quantity = exchange.amount_to_precision(SYMBOL, available_amount)
                if float(quantity) <= 0:
                    logger.warning("No asset balance available to sell.")
                    return "hold", None, None, None
                order = exchange.create_market_sell_order(SYMBOL, quantity)
                order_id = str(order['id'])
                logger.info(f"Placed market sell order: {order_id}, quantity={quantity}, price={close_price:.2f}")
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            action = "hold"
            order_id = None

    logger.debug(f"AI decision: action={action}, stop_loss={stop_loss}, take_profit={take_profit}, order_id={order_id}")
    return action, stop_loss, take_profit, order_id

def handle_second_strategy(action, current_price, primary_profit):
    global tracking_enabled, last_sell_profit, tracking_has_buy, tracking_buy_price, total_return_profit
    return_profit = 0
    msg = ""
    if action == "buy":
        if last_sell_profit > 0:
            tracking_has_buy = True
            tracking_buy_price = current_price
            msg = ""
        else:
            msg = " (Paused Buy2)"
    elif action == "sell" and tracking_has_buy:
        last_sell_profit = primary_profit
        if last_sell_profit > 0:
            tracking_enabled = True
        else:
            tracking_enabled = False
        return_profit = current_price - tracking_buy_price
        total_return_profit += return_profit
        tracking_has_buy = False
        msg = f", Return Profit: {return_profit:.2f}"
    elif action == "sell" and not tracking_has_buy:
        last_sell_profit = primary_profit
        if last_sell_profit > 0:
            tracking_enabled = True
        else:
            tracking_enabled = False
        msg = " (Paused Sell2)"
    return return_profit, msg

def send_telegram_message(signal, bot_token, chat_id, retries=3, delay=5):
    for attempt in range(retries):
        try:
            bot = Bot(token=bot_token)
            diff_color = "🟢" if signal['diff'] > 0 else "🔴"
            message = f"""
Time: {signal['time']}
Timeframe: {signal['timeframe']}
Strategy: {signal['strategy']}
Msg: {signal['message']}
Price: {signal['price']:.2f}
Diff: {diff_color} {signal['diff']:.2f}
Order ID: {signal['order_id'] if signal['order_id'] else 'N/A'}
"""
            bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"Telegram message sent successfully: {signal['action']}, order_id={signal['order_id']}")
            return
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)

def get_performance_metrics():
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT action, profit, return_profit FROM trades")
            trades = c.fetchall()
            total_trades = len(trades)
            wins = sum(1 for trade in trades if trade[1] > 0 or trade[2] > 0)
            losses = sum(1 for trade in trades if trade[1] < 0 or trade[2] < 0)
            holds = sum(1 for trade in trades if trade[0] == 'hold')
            buys = sum(1 for trade in trades if trade[0] == 'buy')
            sells = sum(1 for trade in trades if trade[0] == 'sell')
            total_winning = sum(trade[1] for trade in trades if trade[1] > 0)
            total_losing = sum(trade[1] for trade in trades if trade[1] < 0)
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
            lose_rate = (losses / total_trades * 100) if total_trades > 0 else 0
            hold_rate = (holds / total_trades * 100) if total_trades > 0 else 0
            buy_rate = (buys / total_trades * 100) if total_trades > 0 else 0
            sell_rate = (sells / total_trades * 100) if total_trades > 0 else 0
            return {
                'win_rate': f"{win_rate:.2f}%",
                'lose_rate': f"{lose_rate:.2f}%",
                'hold_rate': f"{hold_rate:.2f}%",
                'buy_rate': f"{buy_rate:.2f}%",
                'sell_rate': f"{sell_rate:.2f}%",
                'total_winning': total_winning,
                'total_lose': total_losing
            }
    except Exception as e:
        logger.error(f"Error calculating performance metrics: {e}")
        return {}

def get_performance():
    metrics = get_performance_metrics()
    return f"Win Rate: {metrics.get('win_rate', '0%')}, Lose Rate: {metrics.get('lose_rate', '0%')}, Total Winning: {metrics.get('total_winning', 0):.2f}, Total Losing: {metrics.get('total_lose', 0):.2f}"

def get_trade_counts():
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT action, COUNT(*) FROM trades GROUP BY action")
            counts = c.fetchall()
            return ", ".join(f"{action}: {count}" for action, count in counts)
    except Exception as e:
        logger.error(f"Error getting trade counts: {e}")
        return "Error retrieving trade counts"

def create_signal(action, price, latest_data, df, profit, total_profit, return_profit, total_return_profit, message, order_id, strategy):
    try:
        metrics = get_performance_metrics()
        signal = {
            'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            'action': action,
            'symbol': SYMBOL,
            'price': price,
            'message': message,
            'timeframe': TIMEFRAME,
            'strategy': strategy,
            'win_rate': metrics.get('win_rate', '0%'),
            'lose_rate': metrics.get('lose_rate', '0%'),
            'hold_rate': metrics.get('hold_rate', '0%'),
            'buy_rate': metrics.get('buy_rate', '0%'),
            'sell_rate': metrics.get('sell_rate', '0%'),
            'total_winning': metrics.get('total_winning', 0),
            'total_lose': metrics.get('total_lose', 0),
            'profit': profit,
            'total_profit': total_profit,
            'return_profit': return_profit,
            'total_return_profit': total_return_profit,
            'diff': latest_data['diff'] if not pd.isna(latest_data.get('diff')) else 0.0,
            'order_id': order_id
        }
        return signal
    except Exception as e:
        logger.error(f"Error creating signal: {e}")
        return None

def store_signal(signal):
    try:
        with db_lock:
            c = conn.cursor()
            c.execute('''
                INSERT INTO signals (
                    time, action, symbol, price, message, timeframe, strategy, win_rate, lose_rate,
                    hold_rate, buy_rate, sell_rate, total_winning, total_lose, profit, total_profit,
                    return_profit, total_return_profit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal['time'], signal['action'], signal['symbol'], signal['price'],
                signal['message'], signal['timeframe'], signal['strategy'], signal['win_rate'],
                signal['lose_rate'], signal['hold_rate'], signal['buy_rate'], signal['sell_rate'],
                signal['total_winning'], signal['total_lose'], signal['profit'], signal['total_profit'],
                signal['return_profit'], signal['total_return_profit']
            ))
            conn.commit()
            logger.info(f"Stored signal: {signal['action']}, price={signal['price']:.2f}, strategy={signal['strategy']}")
    except Exception as e:
        logger.error(f"Error storing signal: {e}")

def store_trade(signal, latest_data, df, stop_loss, take_profit, order_id):
    try:
        with db_lock:
            c = conn.cursor()
            c.execute('''
                INSERT INTO trades (
                    time, action, symbol, price, open_price, close_price, volume, percent_change,
                    stop_loss, take_profit, profit, total_profit, return_profit, total_return_profit,
                    ema1, ema2, rsi, k, d, j, diff, diff1e, diff2m, diff3k, macd, macd_signal,
                    macd_hist, macd_hollow, lst_diff, supertrend, supertrend_trend, stoch_rsi,
                    stoch_k, stoch_d, obv, message, timeframe, order_id, strategy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal['time'], signal['action'], signal['symbol'], signal['price'],
                latest_data.get('Open', 0.0), latest_data.get('Close', 0.0), latest_data.get('Volume', 0.0),
                ((signal['price'] - latest_data.get('Open', signal['price'])) / latest_data.get('Open', signal['price']) * 100) if latest_data.get('Open') else 0.0,
                stop_loss if stop_loss is not None else 0.0, take_profit if take_profit is not None else 0.0,
                signal['profit'], signal['total_profit'], signal['return_profit'], signal['total_return_profit'],
                df['ema1'].iloc[-1] if not df.empty and not pd.isna(df['ema1'].iloc[-1]) else 0.0,
                df['ema2'].iloc[-1] if not df.empty and not pd.isna(df['ema2'].iloc[-1]) else 0.0,
                df['rsi'].iloc[-1] if not df.empty and not pd.isna(df['rsi'].iloc[-1]) else 0.0,
                df['k'].iloc[-1] if not df.empty and not pd.isna(df['k'].iloc[-1]) else 0.0,
                df['d'].iloc[-1] if not df.empty and not pd.isna(df['d'].iloc[-1]) else 0.0,
                df['j'].iloc[-1] if not df.empty and not pd.isna(df['j'].iloc[-1]) else 0.0,
                df['diff'].iloc[-1] if not df.empty and not pd.isna(df['diff'].iloc[-1]) else 0.0,
                df['diff1e'].iloc[-1] if not df.empty and not pd.isna(df['diff1e'].iloc[-1]) else 0.0,
                df['diff2m'].iloc[-1] if not df.empty and not pd.isna(df['diff2m'].iloc[-1]) else 0.0,
                df['diff3k'].iloc[-1] if not df.empty and not pd.isna(df['diff3k'].iloc[-1]) else 0.0,
                df['macd'].iloc[-1] if not df.empty and not pd.isna(df['macd'].iloc[-1]) else 0.0,
                df['macd_signal'].iloc[-1] if not df.empty and not pd.isna(df['macd_signal'].iloc[-1]) else 0.0,
                df['macd_hist'].iloc[-1] if not df.empty and not pd.isna(df['macd_hist'].iloc[-1]) else 0.0,
                df['macd_hollow'].iloc[-1] if not df.empty and not pd.isna(df['macd_hollow'].iloc[-1]) else 0.0,
                df['lst_diff'].iloc[-1] if not df.empty and not pd.isna(df['lst_diff'].iloc[-1]) else 0.0,
                df['supertrend'].iloc[-1] if not df.empty and not pd.isna(df['supertrend'].iloc[-1]) else 0.0,
                df['supertrend_trend'].iloc[-1] if not df.empty and not pd.isna(df['supertrend_trend'].iloc[-1]) else 0,
                df['stoch_rsi'].iloc[-1] if not df.empty and not pd.isna(df['stoch_rsi'].iloc[-1]) else 0.0,
                df['stoch_k'].iloc[-1] if not df.empty and not pd.isna(df['stoch_k'].iloc[-1]) else 0.0,
                df['stoch_d'].iloc[-1] if not df.empty and not pd.isna(df['stoch_d'].iloc[-1]) else 0.0,
                df['obv'].iloc[-1] if not df.empty and not pd.isna(df['obv'].iloc[-1]) else 0.0,
                signal['message'], signal['timeframe'], signal['order_id'], signal['strategy']
            ))
            conn.commit()
            logger.info(f"Stored trade: {signal['action']}, price={signal['price']:.2f}, strategy={signal['strategy']}")
    except Exception as e:
        logger.error(f"Error storing trade: {e}")

def get_next_timeframe_boundary(current_time, timeframe_seconds):
    current_seconds = (current_time.hour * 3600 + current_time.minute * 60 + current_time.second)
    intervals_passed = current_seconds // timeframe_seconds
    next_boundary = (intervals_passed + 1) * timeframe_seconds
    return next_boundary

async def trading_bot():
    global bot_active, position, buy_price, total_profit, pause_duration, pause_start, stop_time, conn, exchange, tracking_enabled, last_sell_profit, tracking_has_buy, tracking_buy_price, total_return_profit

    logger.info(f"Trading bot started: Symbol={SYMBOL}, Timeframe={TIMEFRAME}, Interval={TIMEFRAMES}s")
    bot = Bot(token=BOT_TOKEN)

    while bot_active:
        try:
            if STOP_AFTER_SECONDS > 0 and (datetime.now(EU_TZ) - start_time).total_seconds() >= STOP_AFTER_SECONDS:
                logger.info("Stopping bot due to STOP_AFTER_SECONDS limit reached.")
                bot_active = False
                stop_time = datetime.now(EU_TZ)
                break

            if pause_duration > 0 and pause_start:
                elapsed = (datetime.now(EU_TZ) - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.debug(f"Bot paused: {pause_duration - elapsed:.2f}s remaining")
                    await asyncio.sleep(1)
                    continue
                else:
                    logger.info("Pause duration ended, resuming trading.")
                    pause_start = None
                    pause_duration = 0

            current_time = datetime.now(EU_TZ)
            next_boundary = get_next_timeframe_boundary(current_time, TIMEFRAMES)
            sleep_seconds = max(0, next_boundary - (current_time.hour * 3600 + current_time.minute * 60 + current_time.second))
            if sleep_seconds > 0:
                logger.debug(f"Sleeping for {sleep_seconds:.2f}s until next timeframe boundary")
                await asyncio.sleep(sleep_seconds)
                continue

            logger.debug(f"Fetching data for {SYMBOL} at {current_time}")
            latest_data = get_simulated_price(SYMBOL, exchange, TIMEFRAME)
            if latest_data is None or pd.isna(latest_data['Close']):
                logger.error("Failed to fetch valid price data, skipping iteration.")
                await asyncio.sleep(TIMEFRAMES)
                continue

            ohlcv = exchange.fetch_ohlcv(SYMBOL, TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EU_TZ)
            df = add_technical_indicators(df)

            if df.empty or len(df) < 26:
                logger.warning("Not enough data to make a decision, skipping.")
                await asyncio.sleep(TIMEFRAMES)
                continue

            current_price = latest_data['Close']
            action, stop_loss, take_profit, order_id = ai_decision(df, STOP_LOSS_PERCENT, TAKE_PROFIT_PERCENT, position, buy_price)
            logger.debug(f"AI Decision: action={action}, price={current_price:.2f}, position={position}, stop_loss={stop_loss}, take_profit={take_profit}")

            profit = 0
            message = ""
            strategy = "Supertrend/KDJ/EMA/MACD"
            return_profit, second_message = handle_second_strategy(action, current_price, profit)

            if action == "buy" and position is None:
                position = "long"
                buy_price = current_price
                message = f"Buy at {current_price:.2f}"
                logger.info(f"Buy signal executed: price={current_price:.2f}, order_id={order_id}")
            elif action == "sell" and position == "long":
                profit = (current_price - buy_price) * AMOUNTS / buy_price
                total_profit += profit
                position = None
                buy_price = None
                message = f"Sell at {current_price:.2f}, Profit: {profit:.2f}, Total Profit: {total_profit:.2f}{second_message}"
                logger.info(f"Sell signal executed: price={current_price:.2f}, profit={profit:.2f}, total_profit={total_profit:.2f}, order_id={order_id}")
            elif action == "hold":
                message = f"Hold at {current_price:.2f}{second_message}"
                logger.debug(f"Hold signal: price={current_price:.2f}")
            else:
                message = f"Invalid action {action} at {current_price:.2f}{second_message}"
                logger.warning(f"Invalid action detected: {action}")

            signal = create_signal(
                action=action,
                price=current_price,
                latest_data=latest_data,
                df=df,
                profit=profit,
                total_profit=total_profit,
                return_profit=return_profit,
                total_return_profit=total_return_profit,
                message=message,
                order_id=order_id,
                strategy=strategy
            )

            if signal:
                store_signal(signal)
                store_trade(signal, latest_data, df, stop_loss, take_profit, order_id)
                send_telegram_message(signal, BOT_TOKEN, CHAT_ID)

            logger.debug(f"Completed iteration: action={action}, price={current_price:.2f}, total_profit={total_profit:.2f}")
            await asyncio.sleep(TIMEFRAMES)

        except Exception as e:
            logger.error(f"Error in trading bot loop: {e}")
            await asyncio.sleep(TIMEFRAMES)

    logger.info("Trading bot stopped.")
    with db_lock:
        if conn is not None:
            try:
                upload_to_github(db_path, 'rnn_bot.db')
                conn.close()
                logger.info("Database connection closed and uploaded to GitHub.")
            except Exception as e:
                logger.error(f"Error closing database: {e}")

def cleanup():
    global conn, bot_active
    bot_active = False
    with db_lock:
        if conn is not None:
            try:
                upload_to_github(db_path, 'rnn_bot.db')
                conn.close()
                logger.info("Cleanup: Database connection closed and uploaded to GitHub.")
            except Exception as e:
                logger.error(f"Cleanup: Error closing database: {e}")
            conn = None

atexit.register(cleanup)

async def main():
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    logger.info("Started keep-alive thread")

    db_backup_thread = threading.Thread(target=periodic_db_backup, daemon=True)
    db_backup_thread.start()
    logger.info("Started periodic database backup thread")

    global bot_thread
    bot_thread = threading.Thread(target=lambda: asyncio.run(trading_bot()), daemon=True)
    bot_thread.start()
    logger.info("Started trading bot thread")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 4000))
    logger.info(f"Starting Flask server on port {port}")
    asyncio.run(main())
    app.run(host='0.0.0.0', port=port, debug=False)
