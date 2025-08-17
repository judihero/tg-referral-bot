import os, io, csv, logging, asyncio, aiosqlite
from typing import Optional, Tuple
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ref-bot")

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")            # from @BotFather
CHANNEL  = os.getenv("CHANNEL")               # e.g. @FREEAwekTiktok (include @)
DB_PATH  = os.getenv("DB_PATH", "data.db")    # SQLite file (NOTE: resets on redeploy!)

# ---------- Reward catalog (EDIT THIS) ----------
# code: {label (button text), cost (points), payload (what user receives), repeatable (bool)}
REWARDS = {
    "vip1": {
        "label": "🎁 Unlock VIP Pack (2 Points)",
        "cost": 2,
        "payload": "https://t.me/lexxis00",
        "repeatable": True  # set True if you want users to buy multiple times
    },
    # Add more rewards by copying this block with a new key
}

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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redemptions(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                reward_code TEXT,
                cost        INTEGER,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, reward_code)  -- avoids double-redeem of same one-time reward
            )
        """)
        await db.commit()

async def upsert_user(user):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES (?, ?)", (user.id, user.username or ""))
        await db.execute("UPDATE users SET username=? WHERE user_id=?", (user.username or "", user.id))
        await db.commit()

async def get_earned(user_id:int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=? AND credited=1", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0

async def get_spent(user_id:int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COALESCE(SUM(cost),0) FROM redemptions WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

async def get_balance(user_id:int) -> Tuple[int,int,int]:
    earned = await get_earned(user_id)
    spent  = await get_spent(user_id)
    return earned, spent, max(0, earned - spent)

async def add_pending_referral(referrer_id:int, referee_id:int):
    if referrer_id == referee_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO referrals(referrer_id, referee_id, credited) VALUES (?, ?, 0)", (referrer_id, referee_id))
        await db.commit()

async def mark_credited(referee_id:int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT referrer_id, credited FROM referrals WHERE referee_id=?", (referee_id,))
        row = await cur.fetchone()
        if not row: return None
        referrer_id, credited = int(row[0]), int(row[1])
        if credited == 1: return None
        await db.execute("UPDATE referrals SET credited=1 WHERE referee_id=?", (referee_id,))
        await db.commit()
        return referrer_id

# ---------- Helpers ----------
def main_keyboard():
    rows = [
        [InlineKeyboardButton("📲 Join Channel", url=f"https://t.me/{CHANNEL.lstrip('@')}")],
        [InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")],
        [InlineKeyboardButton("🏅 My Points",  callback_data="my_points")],
    ]
    # Add one button per reward
    for code, r in REWARDS.items():
        rows.append([InlineKeyboardButton(r["label"], callback_data=f"redeem_{code}")])
    return InlineKeyboardMarkup(rows)

async def is_channel_admin(user_id:int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

async def resolve_target_user(arg: str) -> Optional[Tuple[int, str]]:
    if not arg: return None
    async with aiosqlite.connect(DB_PATH) as db:
        if arg.startswith("@"):
            uname = arg[1:].lower()
            cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users WHERE lower(username)=? LIMIT 1", (uname,))
            row = await cur.fetchone()
            if not row: return None
            uid, uname = int(row[0]), row[1]
            label = f"@{uname}" if uname else f"User {uid}"
            return uid, label
        try:
            uid = int(arg)
        except ValueError:
            return None
        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users WHERE user_id=?", (uid,))
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
        if arg == "points":
            earned, spent, balance = await get_balance(user.id)
            await update.message.reply_text(
                f"🏅 Your points\n• Earned: *{earned}*\n• Spent: *{spent}*\n• Available: *{balance}*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=main_keyboard()
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
    await update.message.reply_text(text, reply_markup=main_keyboard(), parse_mode=ParseMode.MARKDOWN)

async def cb_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        status = m.status
    except Exception:
        logger.exception("get_chat_member failed")
        await q.edit_message_text("⚠️ I couldn't check your membership. Make sure the bot is an *admin* in the channel, then tap Verify again.", parse_mode=ParseMode.MARKDOWN)
        return

    if status in ("member", "administrator", "creator"):
        referrer_id = await mark_credited(user_id)
        if referrer_id:
            earned, spent, balance = await get_balance(referrer_id)
            try:
                await context.bot.send_message(referrer_id, f"🎉 A friend joined via your link! You now have *{earned}* earned / *{spent}* spent / *{balance}* available.", parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        await q.edit_message_text("✅ Verified! Thanks for joining. Enjoy the channel 🎉")
    else:
        await q.edit_message_text("❌ Not joined yet. Tap *Join Channel* first, then press *Verify Join*.", parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard())

async def cb_points_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    earned, spent, balance = await get_balance(q.from_user.id)
    await q.message.reply_text(
        f"🏅 Your points\n• Earned: *{earned}*\n• Spent: *{spent}*\n• Available: *{balance}*",
        parse_mode=ParseMode.MARKDOWN
    )

async def cb_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    data = q.data  # e.g., "redeem_vip1"
    code = data.split("_", 1)[1] if "_" in data else ""
    reward = REWARDS.get(code)
    if not reward:
        await q.message.reply_text("⚠️ Reward not found.")
        return

    earned, spent, balance = await get_balance(user_id)
    cost = int(reward["cost"])

    # If one-time reward and already redeemed, block
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM redemptions WHERE user_id=? AND reward_code=? LIMIT 1", (user_id, code))
        exists = await cur.fetchone()
    if exists and not reward.get("repeatable", False):
        await q.message.reply_text("✅ You already unlocked this reward earlier.")
        return

    if balance < cost:
        await q.message.reply_text(f"❌ Not enough points.\nNeeded: *{cost}* • Available: *{balance}*\nInvite more friends and try again.", parse_mode=ParseMode.MARKDOWN)
        return

    # Deduct: record a redemption row (atomic enough for this use)
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO redemptions(user_id, reward_code, cost) VALUES (?, ?, ?)", (user_id, code, cost))
            await db.commit()
        except Exception as e:
            # If UNIQUE violation due to double tap on non-repeatable, treat as success
            await q.message.reply_text("⚠️ Looks like you already redeemed this.")
            return

    # Send reward
    payload = reward["payload"]
    await q.message.reply_text(f"🎁 Unlocked!\nHere is your reward:\n{payload}")

async def link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_username = (await context.bot.get_me()).username
    my_link = f"https://t.me/{bot_username}?start={user.id}"
    await update.message.reply_text(f"🔗 Your invite link:\n`{my_link}`", parse_mode=ParseMode.MARKDOWN)

async def points_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    earned, spent, balance = await get_balance(update.effective_user.id)
    await update.message.reply_text(
        f"🏅 Your points\n• Earned: *{earned}*\n• Spent: *{spent}*\n• Available: *{balance}*",
        parse_mode=ParseMode.MARKDOWN
    )

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
        "/points – show your points (earned / spent / available)\n"
        "/top – leaderboard\n\n"
        "Tap the 🎁 button to redeem rewards with your points."
    )

# ---------- Admin tools (optional; keep if you already use) ----------
async def is_channel_admin(user_id:int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

async def userpoints_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    earned, spent, balance = await get_balance(uid)
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
        detail = "\n".join(f"• @{row[2]}" if row[2] else f"• {int(row[0])} — {str(row[1])[:10]}" for row in rows)
        msg = f"📊 {label}\n🏅 Earned: *{earned}*  Spent: *{spent}*  Available: *{balance}*\n\nRecent:\n{detail}"
    else:
        msg = f"📊 {label}\n🏅 Earned: *{earned}*  Spent: *{spent}*  Available: *{balance}*\n\n(No credited referrals yet.)"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def exportcsv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT u.user_id, COALESCE(u.username,'') AS username,
                   COALESCE(t.earned,0) AS earned,
                   COALESCE(s.spent,0)  AS spent,
                   COALESCE(t.earned,0) - COALESCE(s.spent,0) AS balance
            FROM users u
            LEFT JOIN (
                SELECT referrer_id, COUNT(*) AS earned
                FROM referrals WHERE credited=1 GROUP BY referrer_id
            ) t ON t.referrer_id = u.user_id
            LEFT JOIN (
                SELECT user_id, COALESCE(SUM(cost),0) AS spent
                FROM redemptions GROUP BY user_id
            ) s ON s.user_id = u.user_id
            ORDER BY balance DESC, earned DESC
        """)
        rows = await cur.fetchall()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id","username","earned","spent","balance"])
    for r in rows:
        w.writerow([int(r[0]), r[1], int(r[2] or 0), int(r[3] or 0), int(r[4] or 0)])
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
    app.add_handler(CallbackQueryHandler(cb_verify,  pattern="^verify_join$"))
    app.add_handler(CallbackQueryHandler(cb_points_button, pattern="^my_points$"))
    app.add_handler(CallbackQueryHandler(cb_redeem, pattern="^redeem_"))
    logging.info("Bot starting…")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=["message","callback_query"])
    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(runner())
