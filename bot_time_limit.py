import sqlite3
import threading
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters
import logging

# Logging setup for debugging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup with thread safety
conn = sqlite3.connect('bot.db', check_same_thread=False)
lock = threading.Lock()

def db_execute(query, params=()):
    """Execute a database query with parameters and commit changes."""
    with lock:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor.lastrowid

def db_fetchone(query, params=()):
    """Fetch one row from the database."""
    with lock:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()

# Create database tables if they don't exist
db_execute('''CREATE TABLE IF NOT EXISTS limits
              (topic_id INTEGER PRIMARY KEY, limit_minutes INTEGER)''')
db_execute('''CREATE TABLE IF NOT EXISTS last_posts
              (user_id INTEGER, topic_id INTEGER, last_post_time TEXT,
               PRIMARY KEY (user_id, topic_id))''')

def format_time_limit(limit_minutes):
    """Convert minutes to a human-readable time string (hours or minutes)."""
    if limit_minutes % 60 == 0:
        hours = limit_minutes // 60
        return f"{hours} hour{'s' if hours > 1 else ''}"
    else:
        return f"{limit_minutes} minute{'s' if limit_minutes > 1 else ''}"

# Replace with your bot's API token from BotFather
TOKEN = '7932103636:AAFajB9ORAoUC3Yb0-xxXPib1oyBnVncxM0'

def format_time_limit(limit_minutes):
    """Convert minutes to a human-readable time string (hours or minutes)."""
    if limit_minutes % 60 == 0:
        hours = limit_minutes // 60
        return f"{hours} hour{'s' if hours > 1 else ''}"
    else:
        return f"{limit_minutes} minute{'s' if limit_minutes > 1 else ''}"
    

async def handle_message(update: Update, context):
    """Handle incoming messages and enforce posting limits."""
    message = update.message
    # Use 0 for the general topic (no thread ID), otherwise use the topic ID
    topic_id = message.message_thread_id if message.message_thread_id else 0
    user_id = message.from_user.id

    # Check if the user is an admin; admins are exempt from limits
    chat_member = await context.bot.get_chat_member(message.chat_id, user_id)
    if chat_member.status in ('administrator', 'creator'):
        return

    # Get the posting limit for this topic
    limit_row = db_fetchone('SELECT limit_minutes FROM limits WHERE topic_id = ?', (topic_id,))
    if limit_row is None:
        return  # No limit set for this topic

    limit_minutes = limit_row[0]
    now = datetime.utcnow()

    # Check the user's last posting time in this topic
    last_post_row = db_fetchone('SELECT last_post_time FROM last_posts WHERE user_id = ? AND topic_id = ?', (user_id, topic_id))
    if last_post_row:
        last_post_time = datetime.fromisoformat(last_post_row[0])
        if now - last_post_time < timedelta(minutes=limit_minutes):
            # User posted too soon; delete the message
            await message.delete()
            # Format the time limit and construct the warning message
            time_str = format_time_limit(limit_minutes)
            warning_message = f"To make sure everyone gets a fair chance to shine, you can post in this topic once every {time_str}. This keeps things balanced and gives all posts the attention they deserve."
            # Attempt to send a private warning
            try:
                await context.bot.send_message(user_id, warning_message)
            except Exception as e:
                logger.warning(f"Failed to send private message to {user_id}: {e}")
                # If private message fails, send in the group
                await context.bot.send_message(message.chat_id, warning_message, message_thread_id=topic_id)
            return

    # Update the user's last posting time
    db_execute('INSERT OR REPLACE INTO last_posts (user_id, topic_id, last_post_time) VALUES (?, ?, ?)', 
               (user_id, topic_id, now.isoformat()))
    

async def set_limit(update: Update, context):
    """Set a posting limit for a topic (admin only)."""
    message = update.message
    user_id = message.from_user.id

    # Verify that the user is an admin
    chat_member = await context.bot.get_chat_member(message.chat_id, user_id)
    if chat_member.status not in ('administrator', 'creator'):
        await message.reply_text("Only admins can set posting limits.")
        return

    # Determine the topic ID from the message or replied message
    if message.reply_to_message:
        topic_id = message.reply_to_message.message_thread_id if message.reply_to_message.message_thread_id else 0
    else:
        topic_id = message.message_thread_id if message.message_thread_id else 0

    # Parse the command argument (minutes)
    try:
        minutes = int(context.args[0])
        if minutes < 1:
            raise ValueError
    except (IndexError, ValueError):
        await message.reply_text("Usage: /set_limit <minutes> (e.g., /set_limit 360 for 6 hours)")
        return

    # Set the limit in the database
    db_execute('INSERT OR REPLACE INTO limits (topic_id, limit_minutes) VALUES (?, ?)', (topic_id, minutes))
    time_str = format_time_limit(minutes)
    await message.reply_text(f"Posting limit for this topic set to {time_str}.")

async def get_limit(update: Update, context):
    """Get the current posting limit for a topic."""
    message = update.message

    # Determine the topic ID
    if message.reply_to_message:
        topic_id = message.reply_to_message.message_thread_id if message.reply_to_message.message_thread_id else 0
    else:
        topic_id = message.message_thread_id if message.message_thread_id else 0

    # Fetch and display the limit
    limit_row = db_fetchone('SELECT limit_minutes FROM limits WHERE topic_id = ?', (topic_id,))
    if limit_row:
        time_str = format_time_limit(limit_row[0])
        await message.reply_text(f"The posting limit for this topic is {time_str}.")
    else:
        await message.reply_text("No posting limit is set for this topic.")

def main():
    """Initialize and run the bot."""
    application = Application.builder().token(TOKEN).build()

    # Add handlers
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CommandHandler("set_limit", set_limit))
    application.add_handler(CommandHandler("get_limit", get_limit))

    # Start the bot
    application.run_polling()

if __name__ == '__main__':
    main()