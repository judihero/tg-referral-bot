import os, io, csv, logging, asyncio, aiosqlite
from typing import Optional, Tuple
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, ContextTypes, CallbackQueryHandler
)

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ref-bot")

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")            # from @BotFather
CHANNEL  = os.getenv("CHANNEL")               # e.g. @FREEAwekTiktok  (include @)
DB_PATH  = os.getenv("DB_PATH", "data.db")    # SQLite file (ephemeral on redeploy)

# ---------- DB ----------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referee_id  INTEGER UNIQUE,
                credited    INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def upsert_user(user):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, username) VALUES (?, ?)",
            (user.id, user.username or "")
        )
        await db.execute("UPDATE users SET username=? WHERE user_id=?",
                         (user.username or "", user.id))
        await db.commit()

async def get_points(user_id:int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id=? AND credited=1",
            (user_id,)
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

async def add_pending_referral(referrer_id:int, referee_id:int):
    if referrer_id == referee_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO referrals(referrer_id, referee_id, credited)
            VALUES (?, ?, 0)
        """, (referrer_id, referee_id))
        await db.commit()

async def mark_credited(referee_id:int) -> Optional[int]:
    """Mark referral credited; return referrer_id if newly credited, else None."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT referrer_id, credited FROM referrals WHERE referee_id=?",
            (referee_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        referrer_id, credited = int(row[0]), int(row[1])
        if credited == 1:
            return None
        await db.execute("UPDATE referrals SET credited=1 WHERE referee_id=?",
                         (referee_id,))
        await db.commit()
        return referrer_id

# ---------- Helpers ----------
def verify_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📲 Join Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")],
        [InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")],
        [InlineKeyboardButton("🏅 My Points",  callback_data="my_points")]
    ])

async def is_channel_admin(user_id:int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

async def resolve_target_user(arg: str) -> Optional[Tuple[int, str]]:
    """Return (user_id, label) by @username or numeric id from our users table."""
    if not arg:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        if arg.startswith("@"):
            uname = arg[1:].lower()
            cur = await db.execute(
                "SELECT user_id, COALESCE(username,'') FROM users WHERE lower(username)=? LIMIT 1",
                (uname,)
            )
            row = await cur.fetchone()
            if not row:
                return None
            uid, uname = int(row[0]), row[1]
            label = f"@{uname}" if uname else f"User {uid}"
            return uid, label
        try:
            uid = int(arg)
        except ValueError:
            return None
        cur = await db.execute(
            "SELECT user_id, COALESCE(username,'') FROM users WHERE user_id=?",
            (uid,)
        )
        row = await cur.fetchone()
        uname = row[1] if row else ""
        label = f"@{uname}" if uname else f"User {uid}"
        return uid, label

# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await upsert_user(user)

    referrer_id = None
    if context.args:
        arg = context.args[0].lower()
        # t.me/Bot?start=points → show points instantly (for pinned "My Points" link)
        if arg == "points":
            pts = await get_points(user.id)
            await update.message.reply_text(
                f"🏅 Your points: *{pts}*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=verify_keyboard()
            )
            return
        try:
            referrer_id = int(arg)
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
    await update.message.reply_text(text, reply_markup=verify_keyboard(), parse_mode=ParseMode.MARKDOWN)

async def cb_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        status = m.status
    except Exception:
        logger.exception("get_chat_member failed")
        await q.edit_message_text(
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
        await q.edit_message_text("✅ Verified! Thanks for joining. Enjoy the channel 🎉")
    else:
        await q.edit_message_text(
            "❌ Not joined yet. Tap *Join Channel* first, then press *Verify Join*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=verify_keyboard()
        )

async def cb_points_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pts = await get_points(q.from_user.id)
    await q.message.reply_text(f"🏅 Your points: *{pts}*", parse_mode=ParseMode.MARKDOWN)

async def link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_username = (await context.bot.get_me()).username
    my_link = f"https://t.me/{bot_username}?start={user.id}"
    await update.message.reply_text(f"🔗 Your invite link:\n`{my_link}`", parse_mode=ParseMode.MARKDOWN)

async def points_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pts = await get_points(update.effective_user.id)
    await update.message.reply_text(f"🏅 Your points: *{pts}*", parse_mode=ParseMode.MARKDOWN)

async def top_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT referrer_id, COUNT(*) AS pts
            FROM referrals
            WHERE credited=1
            GROUP BY referrer_id
            ORDER BY pts DESC
            LIMIT 10
        """)
        rows = await cur.fetchall()
        cur2 = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users = {int(r[0]): (r[1] or "") for r in await cur2.fetchall()}

    if not rows:
        await update.message.reply_text("No referrals yet.")
        return

    lines = ["🏆 Top Referrers:"]
    for i, (uid, pts) in enumerate(rows, start=1):
        label = f"@{users.get(int(uid), '')}" if users.get(int(uid), "") else f"User {int(uid)}"
        lines.append(f"{i}. {label} — {int(pts)}")
    await update.message.reply_text("\n".join(lines))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Commands:\n"
        "/start – get your invite link & verify join\n"
        "/link – show your personal invite link\n"
        "/points – show your points\n"
        "/top – leaderboard\n\n"
        "Admins: /userpoints <@user|id>, /exportcsv"
    )

# ---------- Admin tools ----------
async def userpoints_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only channel admins can use
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /userpoints <@username or user_id>")
        return

    target = " ".join(context.args).strip()
    resolved = await resolve_target_user(target)
    if not resolved:
        await update.message.reply_text("User not found in my records. Ask them to /start the bot first.")
        return
    uid, label = resolved
    pts = await get_points(uid)

    # last 10 credited
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT r.referee_id, r.created_at, COALESCE(u.username,'')
            FROM referrals r
            LEFT JOIN users u ON u.user_id=r.referee_id
            WHERE r.referrer_id=? AND r.credited=1
            ORDER BY r.created_at DESC
            LIMIT 10
        """, (uid,))
        rows = await cur.fetchall()

    if rows:
        detail = "\n".join(
            f"• @{row[2]}" if row[2] else f"• {int(row[0])} — {row[1][:10]}"
            for row in rows
        )
        msg = f"📊 {label}\n🏅 Points: *{pts}*\n\nRecent referrals:\n{detail}"
    else:
        msg = f"📊 {label}\n🏅 Points: *{pts}*\n\n(No credited referrals yet.)"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def exportcsv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        # build points map
        cur = await db.execute("""
            SELECT referrer_id, COUNT(*) AS pts
            FROM referrals WHERE credited=1
            GROUP BY referrer_id
        """)
        pts_map = {int(r[0]): int(r[1]) for r in await cur.fetchall()}

        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users = await cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id", "username", "points"])
    for uid, uname in users:
        w.writerow([int(uid), uname, pts_map.get(int(uid), 0)])

    data = io.BytesIO(buf.getvalue().encode("utf-8"))
    await update.message.reply_document(document=data, filename="referral_leaderboard.csv")

# ---------- Runner ----------
async def runner():
    if not BOT_TOKEN or not CHANNEL:
        raise SystemExit("Missing BOT_TOKEN or CHANNEL env vars.")

    await init_db()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("link", link_cmd))
    app.add_handler(CommandHandler("points", points_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    # admin
    app.add_handler(CommandHandler("userpoints", userpoints_cmd))
    app.add_handler(CommandHandler("exportcsv", exportcsv_cmd))
    # callbacks
    app.add_handler(CallbackQueryHandler(cb_verify,        pattern="^verify_join$"))
    app.add_handler(CallbackQueryHandler(cb_points_button, pattern="^my_points$"))

    logging.info("Bot starting…")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=["message", "callback_query"])
    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(runner())
