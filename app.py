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
    global conn, stop_time
    status = "active" if bot_active else "stopped"
    start_time = time.time()
    with db_lock:
        try:
            if conn is None:
                if not setup_database(first_attempt=True):
                    logger.error("Failed to reinitialize database for index route")
                    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
                    current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    return render_template(
                        'index.html',
                        signal=None,
                        status=status,
                        timeframe=TIMEFRAME,
                        trades=[],
                        stop_time=stop_time_str,
                        current_time=current_time,
                        metrics={},
                        user=None,
                        profile_status='incomplete'
                    )

            c = conn.cursor()
            c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 1")
            signal_row = c.fetchone()
            signal = None
            if signal_row:
                signal = dict(zip(['id', 'time', 'action', 'symbol', 'price', 'message', 'timeframe', 'strategy',
                                   'win_rate', 'lose_rate', 'hold_rate', 'buy_rate', 'sell_rate', 'total_winning',
                                   'total_lose', 'profit', 'total_profit', 'return_profit', 'total_return_profit'], signal_row))

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

            user = None
            profile_status = 'incomplete'
            if 'user_id' in session:
                c.execute("SELECT * FROM users WHERE id=?", (session['user_id'],))
                user = c.fetchone()
                if user:
                    user = dict(zip([col[0] for col in c.description], user))
                    is_complete = all([user['first_name'], user['last_name'], user['email'], user['phone'],
                                       user['age'], user['country'], user['state'], user['address'],
                                       user['occupation'], user['email_verified'], user['phone_verified']])
                    has_id_cards = user['id_card_front'] and user['id_card_back']
                    has_warning = user['warning_message'] and user['warning_expiry'] and datetime.strptime(user['warning_expiry'], "%Y-%m-%d %H:%M:%S") > datetime.now(EU_TZ)
                    if has_warning:
                        profile_status = 'warning'
                    elif is_complete:
                        profile_status = 'complete'
                    elif has_id_cards:
                        profile_status = 'id_uploaded'
                    else:
                        profile_status = 'incomplete'

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

def get_next_timeframe_boundary(current_time, timeframe_seconds):
    current_seconds = (current_time.hour * 3600 + current_time.minute * 60 + current_time.second)
    intervals_passed = current_seconds // timeframe_seconds
    next_boundary = (intervals_passed + 1) * timeframe_seconds
    seconds_until_boundary = next_boundary - current_seconds
    return seconds_until_boundary

def trading_bot():
    global bot_active, position, buy_price, total_profit, pause_duration, pause_start, conn, stop_time
    bot = None
    try:
        bot = Bot(token=BOT_TOKEN)
        logger.info("Telegram bot initialized successfully")
        test_signal = create_signal('test', 0.0, pd.Series(), pd.DataFrame(), 0, 0, 0, 0, f"Test message for {SYMBOL} bot startup", None, "test")
        send_telegram_message(test_signal, BOT_TOKEN, CHAT_ID)
        store_signal(test_signal)
    except Exception as e:
        logger.error(f"Error initializing Telegram bot: {e}")
        bot = None

    last_update_id = 0
    df = None

    initial_signal = create_signal('hold', 0.0, pd.Series(), pd.DataFrame(), 0, 0, 0, 0, f"Initializing bot for {SYMBOL}", None, "initial")
    store_signal(initial_signal)
    upload_to_github(db_path, 'rnn_bot.db')

    for attempt in range(3):
        try:
            ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=100)
            if not ohlcv:
                logger.warning(f"No historical data for {SYMBOL}. Retrying...")
                time.sleep(5)
                continue
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(EU_TZ)
            df.set_index('timestamp', inplace=True)
            df['High'] = df['High'].fillna(df['Close'])
            df['Low'] = df['Low'].fillna(df['Close'])
            df = add_technical_indicators(df)
            logger.info(f"Initial df shape: {df.shape}")
            break
        except Exception as e:
            logger.error(f"Error fetching historical data (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                logger.error(f"Failed to fetch historical data for {SYMBOL}")
                return

    timeframe_seconds = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '1d': 86400}.get(TIMEFRAME, TIMEFRAMES)

    current_time = datetime.now(EU_TZ)
    seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
    time.sleep(seconds_to_wait)

    while True:
        loop_start_time = datetime.now(EU_TZ)
        with bot_lock:
            if STOP_AFTER_SECONDS > 0 and datetime.now(EU_TZ) >= stop_time:
                bot_active = False
                if position == "long":
                    latest_data = get_simulated_price()
                    if not pd.isna(latest_data['Close']):
                        profit = latest_data['Close'] - buy_price
                        total_profit += profit
                        return_profit, msg = handle_second_strategy("sell", latest_data['Close'], profit)
                        usdt_amount = AMOUNTS
                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / latest_data['Close'])
                        order_id = None
                        try:
                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                            order_id = str(order['id'])
                        except Exception as e:
                            logger.error(f"Error placing market sell order on stop: {e}")
                        signal = create_signal("sell", latest_data['Close'], latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped due to time limit{msg}", order_id, "primary")
                        store_signal(signal)
                        if bot:
                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                    position = None
                logger.info("Bot stopped due to time limit")
                upload_to_github(db_path, 'rnn_bot.db')
                break

            if not bot_active:
                logger.info("Bot is stopped. Attempting to restart.")
                bot_active = True
                position = None
                pause_start = None
                pause_duration = 0
                if STOP_AFTER_SECONDS > 0:
                    stop_time = datetime.now(EU_TZ) + timedelta(seconds=STOP_AFTER_SECONDS)
                if bot:
                    bot.send_message(chat_id=CHAT_ID, text="Bot restarted automatically.")
                continue

        try:
            if pause_start and pause_duration > 0:
                elapsed = (datetime.now(EU_TZ) - pause_start).total_seconds()
                if elapsed < pause_duration:
                    logger.info(f"Bot paused, resuming in {int(pause_duration - elapsed)} seconds")
                    time.sleep(min(pause_duration - elapsed, 60))
                    continue
                else:
                    pause_start = None
                    pause_duration = 0
                    position = None
                    logger.info("Bot resumed after pause")
                    if bot:
                        bot.send_message(chat_id=CHAT_ID, text="Bot resumed after pause.")

            latest_data = get_simulated_price()
            if pd.isna(latest_data['Close']):
                logger.warning("Skipping cycle due to missing price data.")
                current_time = datetime.now(EU_TZ)
                seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
                time.sleep(seconds_to_wait)
                continue
            current_price = latest_data['Close']
            current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")

            if bot:
                try:
                    updates = bot.get_updates(offset=last_update_id, timeout=10)
                    for update in updates:
                        if update.message and update.message.text:
                            text = update.message.text.strip()
                            command_chat_id = update.message.chat.id
                            if text == '/help':
                                bot.send_message(chat_id=command_chat_id, text="Commands: /help, /stop, /stopN, /start, /status, /performance, /count")
                            elif text == '/stop':
                                with bot_lock:
                                    if bot_active and position == "long":
                                        profit = current_price - buy_price
                                        total_profit += profit
                                        return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                        usdt_amount = AMOUNTS
                                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / current_price)
                                        order_id = None
                                        try:
                                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                                            order_id = str(order['id'])
                                        except Exception as e:
                                            logger.error(f"Error placing market sell order on /stop: {e}")
                                        signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot stopped via Telegram{msg}", order_id, "primary")
                                        store_signal(signal)
                                        if bot:
                                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                        position = None
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text="Bot stopped.")
                                upload_to_github(db_path, 'rnn_bot.db')
                            elif text.startswith('/stop') and text[5:].isdigit():
                                multiplier = int(text[5:])
                                with bot_lock:
                                    pause_duration = multiplier * timeframe_seconds
                                    pause_start = datetime.now(EU_TZ)
                                    if position == "long":
                                        profit = current_price - buy_price
                                        total_profit += profit
                                        return_profit, msg = handle_second_strategy("sell", current_price, profit)
                                        usdt_amount = AMOUNTS
                                        quantity = exchange.amount_to_precision(SYMBOL, usdt_amount / current_price)
                                        order_id = None
                                        try:
                                            order = exchange.create_market_sell_order(SYMBOL, quantity)
                                            order_id = str(order['id'])
                                        except Exception as e:
                                            logger.error(f"Error placing market sell order on /stopN: {e}")
                                        signal = create_signal("sell", current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, f"Bot paused via Telegram{msg}", order_id, "primary")
                                        store_signal(signal)
                                        if bot:
                                            send_telegram_message(signal, BOT_TOKEN, CHAT_ID)
                                        position = None
                                    bot_active = False
                                bot.send_message(chat_id=command_chat_id, text=f"Bot paused for {pause_duration/60} minutes.")
                                upload_to_github(db_path, 'rnn_bot.db')
                            elif text == '/start':
                                with bot_lock:
                                    if not bot_active:
                                        bot_active = True
                                        position = None
                                        pause_start = None
                                        pause_duration = 0
                                        if STOP_AFTER_SECONDS > 0:
                                            stop_time = datetime.now(EU_TZ) + timedelta(seconds=STOP_AFTER_SECONDS)
                                        bot.send_message(chat_id=command_chat_id, text="Bot started.")
                            elif text == '/status':
                                status = "active" if bot_active else f"paused for {int(pause_duration - (datetime.now(EU_TZ) - pause_start).total_seconds())} seconds" if pause_start else "stopped"
                                bot.send_message(chat_id=command_chat_id, text=status)
                            elif text == '/performance':
                                bot.send_message(chat_id=command_chat_id, text=get_performance())
                            elif text == '/count':
                                bot.send_message(chat_id=command_chat_id, text=get_trade_counts())
                        last_update_id = update.update_id + 1
                except Exception as e:
                    logger.error(f"Error processing Telegram updates: {e}")

            new_row = pd.DataFrame({
                'Open': [latest_data['Open']],
                'Close': [latest_data['Close']],
                'High': [latest_data['High']],
                'Low': [latest_data['Low']],
                'Volume': [latest_data['Volume']],
                'diff': [latest_data['diff']]
            }, index=[pd.Timestamp.now(tz=EU_TZ)])
            df = pd.concat([df, new_row]).tail(100)
            df = add_technical_indicators(df)

            prev_close = df['Close'].iloc[-2] if len(df) >= 2 else df['Close'].iloc[-1]
            percent_change = ((current_price - prev_close) / prev_close * 100) if prev_close != 0 else 0.0
            action, stop_loss, take_profit, order_id = ai_decision(df, position=position, buy_price=buy_price)

            with bot_lock:
                profit = 0
                return_profit = 0
                msg = f"HOLD {SYMBOL} at {current_price:.2f}"
                if bot_active and action == "buy" and position is None:
                    position = "long"
                    buy_price = current_price
                    return_profit, msg_suffix = handle_second_strategy("buy", current_price, 0)
                    msg = f"BUY {SYMBOL} at {current_price:.2f}, Order ID: {order_id}{msg_suffix}"
                elif bot_active and action == "sell" and position == "long":
                    profit = current_price - buy_price
                    total_profit += profit
                    return_profit, msg_suffix = handle_second_strategy("sell", current_price, profit)
                    msg = f"SELL {SYMBOL} at {current_price:.2f}, Profit: {profit:.2f}, Order ID: {order_id}{msg_suffix}"
                    if stop_loss and current_price <= stop_loss:
                        msg += " (Stop-Loss)"
                    elif take_profit and current_price >= take_profit:
                        msg += " (Take-Profit)"
                    position = None

                signal = create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, order_id, "primary")
                store_signal(signal)
                logger.debug(f"Generated signal: action={signal['action']}, time={signal['time']}, price={signal['price']:.2f}")

                if bot_active and action != "hold" and bot:
                    threading.Thread(target=send_telegram_message, args=(signal, BOT_TOKEN, CHAT_ID), daemon=True).start()

            if bot_active and action != "hold":
                upload_to_github(db_path, 'rnn_bot.db')

            loop_end_time = datetime.now(EU_TZ)
            processing_time = (loop_end_time - loop_start_time).total_seconds()
            seconds_to_wait = get_next_timeframe_boundary(loop_end_time, timeframe_seconds)
            adjusted_sleep = seconds_to_wait - processing_time
            if adjusted_sleep < 0:
                logger.warning(f"Processing time ({processing_time:.2f}s) exceeded timeframe interval ({timeframe_seconds}s)")
                adjusted_sleep = 0
            time.sleep(adjusted_sleep)
        except Exception as e:
            logger.error(f"Error in trading loop: {e}")
            current_time = datetime.now(EU_TZ)
            seconds_to_wait = get_next_timeframe_boundary(current_time, timeframe_seconds)
            time.sleep(seconds_to_wait)

def create_signal(action, current_price, latest_data, df, profit, total_profit, return_profit, total_return_profit, msg, order_id, strategy):
    def safe_float(val, default=0.0):
        return float(val) if val is not None and not pd.isna(val) else default

    def safe_int(val, default=0):
        return int(val) if val is not None and not pd.isna(val) else default

    latest = df.iloc[-1] if not df.empty else pd.Series()

    return {
        'time': datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        'action': action,
        'symbol': SYMBOL,
        'price': float(current_price),
        'open_price': safe_float(latest_data.get('Open')),
        'close_price': safe_float(latest_data.get('Close')),
        'volume': safe_float(latest_data.get('Volume')),
        'percent_change': float(((current_price - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100)
                                if len(df) >= 2 and df['Close'].iloc[-2] != 0 else 0.0),
        'stop_loss': None,
        'take_profit': None,
        'profit': safe_float(profit),
        'total_profit': safe_float(total_profit),
        'return_profit': safe_float(return_profit),
        'total_return_profit': safe_float(total_return_profit),
        'ema1': safe_float(latest.get('ema1')),
        'ema2': safe_float(latest.get('ema2')),
        'rsi': safe_float(latest.get('rsi')),
        'k': safe_float(latest.get('k')),
        'd': safe_float(latest.get('d')),
        'j': safe_float(latest.get('j')),
        'diff': safe_float(latest.get('diff')),
        'diff1e': safe_float(latest.get('diff1e')),
        'diff2m': safe_float(latest.get('diff2m')),
        'diff3k': safe_float(latest.get('diff3k')),
        'macd': safe_float(latest.get('macd')),
        'macd_signal': safe_float(latest.get('macd_signal')),
        'macd_hist': safe_float(latest.get('macd_hist')),
        'macd_hollow': safe_float(latest.get('macd_hollow')),
        'lst_diff': safe_float(latest.get('lst_diff')),
        'supertrend': safe_float(latest.get('supertrend')),
        'supertrend_trend': safe_int(latest.get('supertrend_trend')),
        'stoch_rsi': safe_float(latest.get('stoch_rsi')),
        'stoch_k': safe_float(latest.get('stoch_k')),
        'stoch_d': safe_float(latest.get('stoch_d')),
        'obv': safe_float(latest.get('obv')),
        'message': msg,
        'timeframe': TIMEFRAME,
        'order_id': order_id,
        'strategy': strategy
    }

def store_signal(signal):
    global conn
    start_time = time.time()
    with db_lock:
        for attempt in range(3):
            try:
                if conn is None:
                    if not setup_database(first_attempt=True):
                        logger.error("Failed to reinitialize database for signal storage")
                        return
                c = conn.cursor()
                c.execute('''
                    INSERT INTO trades (
                        time, action, symbol, price, open_price, close_price, volume,
                        percent_change, stop_loss, take_profit, profit, total_profit,
                        return_profit, total_return_profit, ema1, ema2, rsi, k, d, j, diff,
                        diff1e, diff2m, diff3k, macd, macd_signal, macd_hist, macd_hollow,
                        lst_diff, supertrend, supertrend_trend, stoch_rsi, stoch_k, stoch_d,
                        obv, message, timeframe, order_id, strategy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    signal['time'], signal['action'], signal['symbol'], signal['price'],
                    signal['open_price'], signal['close_price'], signal['volume'],
                    signal['percent_change'], signal['stop_loss'], signal['take_profit'],
                    signal['profit'], signal['total_profit'],
                    signal['return_profit'], signal['total_return_profit'],
                    signal['ema1'], signal['ema2'], signal['rsi'],
                    signal['k'], signal['d'], signal['j'], signal['diff'],
                    signal['diff1e'], signal['diff2m'], signal['diff3k'],
                    signal['macd'], signal['macd_signal'], signal['macd_hist'], signal['macd_hollow'],
                    signal['lst_diff'], signal['supertrend'], signal['supertrend_trend'],
                    signal['stoch_rsi'], signal['stoch_k'], signal['stoch_d'], signal['obv'],
                    signal['message'], signal['timeframe'], signal['order_id'], signal['strategy']
                ))
                conn.commit()
                elapsed = time.time() - start_time
                logger.debug(f"Signal stored successfully: action={signal['action']}, time={signal['time']}, db_write_time={elapsed:.3f}s")
                return
            except sqlite3.Error as e:
                logger.error(f"Error storing signal (attempt {attempt + 1}/3): {e}")
                if conn:
                    conn.close()
                    conn = None
                time.sleep(2)
        logger.error("Failed to store signal after 3 attempts.")

def get_performance_metrics():
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit > 0")
            win_rate = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit < 0")
            lose_rate = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM trades WHERE action='hold'")
            hold_rate = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM trades WHERE action='buy'")
            buy_rate = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM trades WHERE action='sell'")
            sell_rate = c.fetchone()[0]
            c.execute("SELECT SUM(profit) FROM trades WHERE action='sell' AND profit > 0")
            total_winning = c.fetchone()[0] or 0.0
            c.execute("SELECT SUM(profit) FROM trades WHERE action='sell' AND profit < 0")
            total_lose = c.fetchone()[0] or 0.0
            return {
                'win_rate': win_rate,
                'lose_rate': lose_rate,
                'hold_rate': hold_rate,
                'buy_rate': buy_rate,
                'sell_rate': sell_rate,
                'total_winning': total_winning,
                'total_lose': total_lose
            }
    except Exception as e:
        logger.error(f"Error fetching performance metrics: {e}")
        return {
            'win_rate': 0,
            'lose_rate': 0,
            'hold_rate': 0,
            'buy_rate': 0,
            'sell_rate': 0,
            'total_winning': 0.0,
            'total_lose': 0.0
        }

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
                    logger.error("Failed to reinitialize database for index route")
                    stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
                    current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    return render_template(
                        'index.html',
                        signal=None,
                        status=status,
                        timeframe=TIMEFRAME,
                        trades=[],
                        stop_time=stop_time_str,
                        current_time=current_time,
                        metrics={}
                    )

            c = conn.cursor()
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

            signal = trades[0] if trades else None
            stop_time_str = stop_time.strftime("%Y-%m-%d %H:%M:%S") if stop_time else "N/A"
            current_time = datetime.now(EU_TZ).strftime("%Y-%m-%d %H:%M:%S")
            metrics = get_performance_metrics()

            if signal:
                signal.update(metrics)

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
                metrics=metrics
            )

        except Exception as e:
            logger.error(f"Error rendering index.html: {e}")
            return "<h1>Error</h1><p>Failed to load page. Please try again later.</p>", 500

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
            elapsed = time.time() - start_time
            logger.debug(f"Fetched {len(trades)} trades for /trades endpoint in {elapsed:.3f}s")
            return jsonify(trades)
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return jsonify({"error": f"Failed to fetch trades: {str(e)}"}), 500

def get_performance():
    global conn
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT DISTINCT timeframe FROM trades")
            timeframes = [row[0] for row in c.fetchall()]
            message = "Performance Statistics by Timeframe:\n"
            for tf in timeframes:
                c.execute("SELECT MIN(time), MAX(time), SUM(profit), SUM(return_profit), COUNT(*) FROM trades WHERE action='sell' AND profit IS NOT NULL AND timeframe=?", (tf,))
                result = c.fetchone()
                min_time, max_time, total_profit_db, total_return_profit_db, win_trades = result if result else (None, None, None, None, 0)
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit < 0 AND timeframe=?", (tf,))
                loss_trades = c.fetchone()[0]
                duration = (datetime.strptime(max_time, "%Y-%m-%d %H:%M:%S") - datetime.strptime(min_time, "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600 if min_time and max_time else "N/A"
                total_profit_db = total_profit_db if total_profit_db is not None else 0
                total_return_profit_db = total_return_profit_db if total_return_profit_db is not None else 0
                message += f"""
Timeframe: {tf}
Duration (hours): {duration}
Win Trades: {win_trades}
Loss Trades: {loss_trades}
Total Profit: {total_profit_db:.2f}
Total Return Profit: {total_return_profit_db:.2f}
"""
            return message
    except Exception as e:
        logger.error(f"Error fetching performance: {e}")
        return f"Error fetching performance data: {str(e)}"

def get_trade_counts():
    global conn
    try:
        with db_lock:
            c = conn.cursor()
            c.execute("SELECT DISTINCT timeframe FROM trades")
            timeframes = [row[0] for row in c.fetchall()]
            message = "Trade Counts by Timeframe:\n"
            for tf in timeframes:
                c.execute("SELECT COUNT(*), SUM(profit), SUM(return_profit) FROM trades WHERE timeframe=?", (tf,))
                total_trades, total_profit_db, total_return_profit_db = c.fetchone()
                c.execute("SELECT COUNT(*) FROM trades WHERE action='buy' AND timeframe=?", (tf,))
                buy_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND timeframe=?", (tf,))
                sell_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit > 0 AND timeframe=?", (tf,))
                win_trades = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM trades WHERE action='sell' AND profit < 0 AND timeframe=?", (tf,))
                loss_trades = c.fetchone()[0]
                total_profit_db = total_profit_db if total_profit_db is not None else 0
                total_return_profit_db = total_return_profit_db if total_return_profit_db is not None else 0
                message += f"""
Timeframe: {tf}
Total Trades: {total_trades}
Buy Trades: {buy_trades}
Sell Trades: {sell_trades}
Win Trades: {win_trades}
Loss Trades: {loss_trades}
Total Profit: {total_profit_db:.2f}
Total Return Profit: {total_return_profit_db:.2f}
"""
            return message
    except Exception as e:
        logger.error(f"Error fetching trade counts: {e}")
        return f"Error fetching trade counts: {str(e)}"

def cleanup():
    global conn
    if conn:
        conn.close()
        logger.info("Database connection closed")
        upload_to_github(db_path, 'rnn_bot.db')

atexit.register(cleanup)

async def main():
    pass

if bot_thread is None or not bot_thread.is_alive():
    bot_thread = threading.Thread(target=trading_bot, daemon=True)
    bot_thread.start()
    logger.info("Trading bot started automatically")

keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
keep_alive_thread.start()
logger.info("Keep-alive thread started")

db_backup_thread = threading.Thread(target=periodic_db_backup, daemon=True)
db_backup_thread.start()
logger.info("Database backup thread started")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 4000))
    logger.info(f"Starting Flask server on port {port}")
    asyncio.run(main())
    app.run(host='0.0.0.0', port=port, debug=False)
