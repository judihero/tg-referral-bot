import os, logging, asyncio, aiosqlite
from typing import Optional
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler
)

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ref-bot")

# ---------- Config via environment variables ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")  # from @BotFather
CHANNEL = os.getenv("CHANNEL")      # e.g. @FREEAwekTiktok (must include @)
DB_PATH = os.getenv("DB_PATH", "data.db")

JOIN_TEXT = (
    "📣 Join our channel to activate your invite:\n"
    "{channel}\n\n"
    "After joining, tap ✅ Verify Join."
)

# ---------- DB ----------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referee_id INTEGER UNIQUE,
                credited INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def upsert_user(user):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, username) VALUES (?, ?)",
            (user.id, user.username or "")
        )
        await db.execute(
            "UPDATE users SET username=? WHERE user_id=?",
            (user.username or "", user.id)
        )
        await db.commit()

async def get_points(user_id:int)->int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id=? AND credited=1",
            (user_id,)
        )
        row = await cur.fetchone()
        if not row:
            return 0
        return int(row[0])

async def add_pending_referral(referrer_id:int, referee_id:int):
    if referrer_id == referee_id:
        return  # ignore self-referrals
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO referrals(referrer_id, referee_id, credited) VALUES (?, ?, 0)",
            (referrer_id, referee_id)
        )
        await db.commit()

async def mark_credited(referee_id:int) -> Optional[int]:
    """Mark referral as credited; return referrer_id if newly credited, else None."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT referrer_id, credited FROM referrals WHERE referee_id=?",
            (referee_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        referrer_id, credited = row
        if int(credited) == 1:
            return None
        await db.execute(
            "UPDATE referrals SET credited=1 WHERE referee_id=?",
            (referee_id,)
        )
        await db.commit()
        return int(referrer_id)

def verify_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📲 Join Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")],
        [InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")]
    ])

# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await upsert_user(user)

    # Deep-link: /start <referrer_id>
    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0])
        except ValueError:
            referrer_id = None

    if referrer_id:
        await add_pending_referral(referrer_id, user.id)

    bot_username = (await context.bot.get_me()).username
    my_link = f"https://t.me/{bot_username}?start={user.id}"

    text = (
        "👋 Welcome!\n\n"
        f"🔗 *Your personal invite link:*\n`{my_link}`\n\n"
        "Invite friends with this link. You’ll get +1 point when they join the channel and verify.\n\n"
        "If you arrived via someone’s link, please join & verify below."
    )
    if update.message:
        await update.message.reply_text(
            text, reply_markup=verify_keyboard(), parse_mode=ParseMode.MARKDOWN
        )
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=verify_keyboard(), parse_mode=ParseMode.MARKDOWN
        )

async def cb_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    try:
        member = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        status = member.status  # 'member','administrator','creator','left','kicked'
    except Exception as e:
        logger.exception("get_chat_member failed")
        await query.edit_message_text(
            "⚠️ I couldn't check your membership. Make sure the bot is an *admin* in the channel, then tap Verify again.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if status in ("member", "administrator", "creator"):
        referrer_id = await mark_credited(user_id)
        if referrer_id:
            points = await get_points(referrer_id)
            try:
                await context.bot.send_message(
                    referrer_id,
                    f"🎉 A friend joined via your link! You now have *{points}* points.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
        await query.edit_message_text("✅ Verified! Thanks for joining. Enjoy the channel 🎉")
    else:
        await query.edit_message_text(
            "❌ Not joined yet. Tap *Join Channel* first, then press *Verify Join*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=verify_keyboard()
        )

async def link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_username = (await context.bot.get_me()).username
    my_link = f"https://t.me/{bot_username}?start={user.id}"
    await update.message.reply_text(
        f"🔗 Your invite link:\n`{my_link}`",
        parse_mode=ParseMode.MARKDOWN
    )

async def points_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pts = await get_points(update.effective_user.id)
    await update.message.reply_text(f"🏅 Your points: *{pts}*", parse_mode=ParseMode.MARKDOWN)

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT referrer_id, COUNT(*) as pts
            FROM referrals
            WHERE credited=1
            GROUP BY referrer_id
            ORDER BY pts DESC
            LIMIT 10
        """)
        rows = await cur.fetchall()
        cur = await db.execute("SELECT user_id, username FROM users")
        user_map = {r[0]: (r[1] or "") for r in await cur.fetchall()}

    if not rows:
        await update.message.reply_text("No referrals yet.")
        return

    lines = ["🏆 Top Referrers:"]
    for i, (uid, pts) in enumerate(rows, start=1):
        uname = user_map.get(uid) or ""
        label = f"@{uname}" if uname else f"User {uid}"
        lines.append(f"{i}. {label} — {pts}")
    await update.message.reply_text("\n".join(lines))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Available commands:\n"
        "/start – get your invite link & verify join\n"
        "/link – show your personal invite link\n"
        "/points – show your points\n"
        "/top – leaderboard\n"
    )
    await update.message.reply_text(text)

def main():
    # Make sure required env vars exist
    if not BOT_TOKEN or not CHANNEL:
        raise SystemExit("Missing BOT_TOKEN or CHANNEL env vars.")

    # Initialize the DB before the bot starts
    asyncio.run(init_db())

    # Build the Telegram application and register handlers
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("link", link_cmd))
    app.add_handler(CommandHandler("points", points_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CallbackQueryHandler(cb_verify, pattern="^verify_join$"))

    logging.info("Bot starting…")
    # IMPORTANT: run_polling is synchronous (no await)
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()

