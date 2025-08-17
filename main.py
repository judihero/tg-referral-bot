import os, io, csv, html, logging, asyncio, aiosqlite
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
REWARDS = {
    "vip1": {
        "label": "🎁 Unlock VIP Pack (2 points)",
        "cost": 2,
        "payload": "https://t.me/lexxis00",
        "repeatable": True
    },
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
                UNIQUE(user_id, reward_code)
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
        await db.execute(
            "INSERT OR IGNORE INTO referrals(referrer_id, referee_id, credited) VALUES (?, ?, 0)",
            (referrer_id, referee_id)
        )
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
    for code, r in REWARDS.items():
        rows.append([InlineKeyboardButton(r["label"], callback_data=f"redeem_{code}")])
    return InlineKeyboardMarkup(rows)

def _label(uid: int, users: dict) -> str:
    uname = users.get(int(uid), "")
    return f"@{uname}" if uname else f"User {uid}"

async def is_channel_admin(user_id:int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await context.bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

async def resolve_target_user(arg: str):
    if not arg: return None
    async with aiosqlite.connect(DB_PATH) as db:
        if arg.startswith("@"):
            uname = arg[1:].lower()
            cur = await db.execute(
                "SELECT user_id, COALESCE(username,'') FROM users WHERE lower(username)=? LIMIT 1", (uname,)
            )
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

# ---------- User Handlers ----------
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
        await q.edit_message_text(
            "⚠️ I couldn't check your membership. Make sure the bot is an *admin* in the channel, then tap Verify again.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if status in ("member", "administrator", "creator"):
        referrer_id = await mark_credited(user_id)
        if referrer_id:
            earned, spent, balance = await get_balance(referrer_id)
            try:
                await context.bot.send_message(
                    referrer_id,
                    f"🎉 A friend joined via your link! You now have *{earned}* earned / *{spent}* spent / *{balance}* available.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
        await q.edit_message_text("✅ Verified! Thanks for joining. Enjoy the channel 🎉")
    else:
        await q.edit_message_text(
            "❌ Not joined yet. Tap *Join Channel* first, then press *Verify Join*.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_keyboard()
        )

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

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM redemptions WHERE user_id=? AND reward_code=? LIMIT 1", (user_id, code))
        exists = await cur.fetchone()
    if exists and not reward.get("repeatable", False):
        await q.message.reply_text("✅ You already unlocked this reward earlier.")
        return

    if balance < cost:
        await q.message.reply_text(
            f"❌ Not enough points.\nNeeded: *{cost}* • Available: *{balance}*",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("INSERT INTO redemptions(user_id, reward_code, cost) VALUES (?, ?, ?)", (user_id, code, cost))
            await db.commit()
        except Exception:
            await q.message.reply_text("⚠️ Looks like you already redeemed this.")
            return

    await q.message.reply_text(f"🎁 Unlocked!\nHere is your reward:\n{reward['payload']}")

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
        "/points – show your points (earned/spent/available)\n"
        "/top – leaderboard\n\n"
        "Admins: /dashboard, /allpoints [N], /recent [N], /whoinvited <@user|id>, /table [page] [size], /exportcsv\n"
        "Tap the 🎁 button to redeem rewards."
    )

# ---------- Admin-only Insights ----------
async def dashboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total_users = int((await cur.fetchone())[0])

        cur = await db.execute("SELECT COUNT(*) FROM referrals WHERE credited=1")
        total_credited = int((await cur.fetchone())[0])

        cur = await db.execute("SELECT COUNT(*) FROM referrals WHERE credited=0")
        total_pending = int((await cur.fetchone())[0])

        cur = await db.execute("""
            SELECT referrer_id, COUNT(*) AS pts
            FROM referrals
            WHERE credited=1
            GROUP BY referrer_id
            ORDER BY pts DESC
            LIMIT 10
        """)
        top_rows = await cur.fetchall()

        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users_map = {int(r[0]): (r[1] or "") for r in await cur.fetchall()}

    lines = [
        "📊 *Dashboard*",
        f"• Users: *{total_users}*",
        f"• Credited referrals: *{total_credited}*",
        f"• Pending referrals: *{total_pending}*",
        "",
        "🏆 *Top 10*"
    ]
    if not top_rows:
        lines.append("No data yet.")
    else:
        for i, (uid, pts) in enumerate(top_rows,  start=1):
            lines.append(f"{i}. {_label(int(uid), users_map)} — {int(pts)}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def allpoints_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return
    try:
        limit = min(max(int(context.args[0]), 1), 200) if context.args else 50
    except ValueError:
        limit = 50

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT referrer_id, COUNT(*) AS earned
            FROM referrals
            WHERE credited=1
            GROUP BY referrer_id
            ORDER BY earned DESC
            LIMIT ?
        """, (limit,))
        earned_rows = await cur.fetchall()

        cur = await db.execute("""
            SELECT user_id, COALESCE(SUM(cost),0) AS spent
            FROM redemptions GROUP BY user_id
        """)
        spent_map = {int(r[0]): int(r[1]) for r in await cur.fetchall()}

        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users_map = {int(r[0]): (r[1] or "") for r in await cur.fetchall()}

    if not earned_rows:
        await update.message.reply_text("No referrals yet.")
        return

    lines = ["📋 *All Points* (top)", "`rank  user                earned  spent  balance`"]
    rank = 1
    for uid, earned in earned_rows:
        uid = int(uid); earned = int(earned)
        spent = spent_map.get(uid, 0)
        bal = max(0, earned - spent)
        label = _label(uid, users_map)
        lines.append(f"`{rank:>4}  {label:<18}  {earned:>6}  {spent:>5}  {bal:>7}`")
        rank += 1
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def recent_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return
    try:
        limit = min(max(int(context.args[0]), 1), 200) if context.args else 30
    except ValueError:
        limit = 30

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT created_at, referrer_id, referee_id
            FROM referrals
            WHERE credited=1
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        rows = await cur.fetchall()
        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users_map = {int(r[0]): (r[1] or "") for r in await cur.fetchall()}

    if not rows:
        await update.message.reply_text("No credited referrals yet.")
        return

    lines = ["🕒 *Recent referrals*"]
    for created, referrer, referee in rows:
        lines.append(f"{str(created)[:16]} — {_label(int(referrer), users_map)} → {_label(int(referee), users_map)}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def whoinvited_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return
    if not context.args:
        await update.message.reply_text("Usage: /whoinvited <@username or user_id>")
        return

    target = " ".join(context.args).strip()
    resolved = await resolve_target_user(target)
    if not resolved:
        await update.message.reply_text("User not found in my records.")
        return
    uid, label = resolved

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT referrer_id, created_at, credited
            FROM referrals
            WHERE referee_id=?
            ORDER BY created_at ASC
            LIMIT 1
        """, (uid,))
        row = await cur.fetchone()

    if not row:
        await update.message.reply_text(f"{label} has no referral record.")
        return

    referrer_id, created_at, credited = int(row[0]), row[1], int(row[2])
    status = "credited ✅" if credited == 1 else "pending ⏳"

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, COALESCE(username,'') FROM users")
        users_map = {int(r[0]): (r[1] or "") for r in await cur.fetchall()}

    await update.message.reply_text(
        f"{label} was invited by {_label(referrer_id, users_map)} on {str(created_at)[:16]} — {status}."
    )

# ---------- NEW: Admin table like your screenshot ----------
async def table_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show table: user_id | username | earned | spent | balance (paginated)."""
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return

    # Args: /table [page] [size]
    try:
        page = max(int(context.args[0]), 1) if len(context.args) >= 1 else 1
    except ValueError:
        page = 1
    try:
        size = min(max(int(context.args[1]), 1), 200) if len(context.args) >= 2 else 20
    except ValueError:
        size = 20
    offset = (page - 1) * size

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total_users = int((await cur.fetchone())[0])

        cur = await db.execute(f"""
            SELECT u.user_id,
                   COALESCE(u.username,'') AS username,
                   COALESCE(t.earned,0)   AS earned,
                   COALESCE(s.spent,0)    AS spent,
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
            ORDER BY balance DESC, earned DESC, u.user_id ASC
            LIMIT ? OFFSET ?
        """, (size, offset))
        rows = await cur.fetchall()

    pages = max((total_users + size - 1) // size, 1)

    # Build fixed-width table (use HTML <pre> to preserve spaces)
    header = f"{'user_id':<12} {'username':<18} {'earned':>6} {'spent':>6} {'balance':>7}"
    line   = "-" * len(header)
    body_lines = []
    for user_id, uname, earned, spent, balance in rows:
        uname = (uname or "")
        # Escape for HTML and trim to width
        uname = html.escape(uname)[:18]
        body_lines.append(f"{str(user_id):<12} {uname:<18} {int(earned):>6} {int(spent):>6} {int(balance):>7}")

    table_text = header + "\n" + line + "\n" + ("\n".join(body_lines) if body_lines else "(no rows)")
    footer = f"\n\nPage {page}/{pages} • Use /table <page> <size> (e.g., /table 2 20)"

    await update.message.reply_text(
        f"<pre>{html.escape(table_text)}</pre>{footer}",
        parse_mode=ParseMode.HTML
    )

# ---------- Optional CSV ----------
async def exportcsv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_channel_admin(update.effective_user.id, context):
        await update.message.reply_text("Admins only."); return
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
    # user cmds
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("link", link_cmd))
    app.add_handler(CommandHandler("points", points_cmd))
    app.add_handler(CommandHandler("top", top_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    # admin cmds
    app.add_handler(CommandHandler("dashboard",  dashboard_cmd))
    app.add_handler(CommandHandler("allpoints",  allpoints_cmd))
    app.add_handler(CommandHandler("recent",     recent_cmd))
    app.add_handler(CommandHandler("whoinvited", whoinvited_cmd))
    app.add_handler(CommandHandler("table",      table_cmd))       # << NEW
    app.add_handler(CommandHandler("exportcsv",  exportcsv_cmd))
    # callbacks
    app.add_handler(CallbackQueryHandler(cb_verify,        pattern="^verify_join$"))
    app.add_handler(CallbackQueryHandler(cb_points_button, pattern="^my_points$"))
    app.add_handler(CallbackQueryHandler(cb_redeem,        pattern="^redeem_"))

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
