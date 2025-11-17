# codeforces_battle_bot.py
# Optimized Codeforces Battle Bot (Python + SQLite)
# Requires: python-telegram-bot >=20, aiohttp, aiosqlite
# Usage: set BOT_TOKEN env var and run: python codeforces_battle_bot.py

import os
import asyncio
import aiosqlite
import aiohttp
import random
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "battlebot.db")
CF_BASE = "https://codeforces.com/api"

# Global application
APP = None

# In-memory runtime
ACTIVE_BATTLES: Dict[int, Dict[str, Any]] = {}
PROBLEMSET_CACHE: List[dict] = []
PROBLEMSET_LAST = 0
PENDING_CANCELS: Dict[int, int] = {}  # chat_id -> message_id for cancel confirmations

# ---------- DB helpers -----------
async def init_db():
    """Initialize SQLite database with optimized schema"""
    logger.info("Initializing database...")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA cache_size=-64000;")  # 64MB cache
        
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                chat_id INTEGER,
                user_id INTEGER,
                full_name TEXT,
                handle TEXT,
                rating INTEGER,
                PRIMARY KEY(chat_id, user_id)
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_chat ON users(chat_id)")
        
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS points(
                chat_id INTEGER,
                user_id INTEGER,
                pts INTEGER DEFAULT 0,
                battles INTEGER DEFAULT 0,
                first_solves INTEGER DEFAULT 0,
                PRIMARY KEY(chat_id, user_id)
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_points_chat ON points(chat_id)")
        
        await db.commit()
    logger.info("Database initialized successfully")

async def upsert_user(chat_id: int, user_id: int, full_name: str, handle: str, rating: int):
    """Insert or update user with optimized query"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users(chat_id, user_id, full_name, handle, rating) VALUES(?,?,?,?,?)",
            (chat_id, user_id, full_name, handle, rating),
        )
        await db.execute(
            "INSERT OR IGNORE INTO points(chat_id, user_id, pts, battles, first_solves) VALUES(?,?,0,0,0)",
            (chat_id, user_id),
        )
        await db.commit()

async def get_user(chat_id: int, user_id: int) -> Optional[Tuple]:
    """Get user info with single query"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT full_name, handle, rating FROM users WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        return await cur.fetchone()

async def get_users(chat_id: int) -> List[Tuple]:
    """Get all users in a group, sorted by rating"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT full_name, handle, rating, user_id FROM users WHERE chat_id=? ORDER BY rating DESC",
            (chat_id,)
        )
        return await cur.fetchall()

async def add_points(chat_id: int, user_id: int, pts: int, first_solve: bool = False):
    """Add points with optimized update"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO points(chat_id, user_id, pts, battles, first_solves) VALUES(?,?,0,0,0)",
            (chat_id, user_id),
        )
        if first_solve:
            await db.execute(
                "UPDATE points SET pts=pts+?, first_solves=first_solves+1 WHERE chat_id=? AND user_id=?",
                (pts, chat_id, user_id)
            )
        else:
            await db.execute(
                "UPDATE points SET pts=pts+? WHERE chat_id=? AND user_id=?",
                (pts, chat_id, user_id)
            )
        await db.commit()

async def increment_battles(chat_id: int, user_ids: List[int]):
    """Increment battle count for multiple users"""
    async with aiosqlite.connect(DB_PATH) as db:
        for uid in user_ids:
            await db.execute(
                "UPDATE points SET battles=battles+1 WHERE chat_id=? AND user_id=?",
                (chat_id, uid)
            )
        await db.commit()

async def get_leaderboard(chat_id: int) -> List[Tuple]:
    """Get leaderboard with optimized join"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT u.full_name, u.handle, u.rating, COALESCE(p.pts,0) as pts
            FROM users u LEFT JOIN points p ON u.chat_id=p.chat_id AND u.user_id=p.user_id
            WHERE u.chat_id=?
            ORDER BY pts DESC, u.rating DESC, u.handle ASC
            """,
            (chat_id,),
        )
        return await cur.fetchall()

async def get_user_stats(chat_id: int, user_id: int) -> Optional[Tuple]:
    """Get user statistics"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT pts, battles, first_solves FROM points WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)
        )
        return await cur.fetchone()

# ---------- Codeforces API helpers ----------
async def cf_api(path: str, params: dict = None, timeout: int = 15):
    """Call Codeforces API with error handling and timeout"""
    url = f"{CF_BASE}/{path}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"CF API {path} returned status {resp.status}")
                return None
    except asyncio.TimeoutError:
        logger.error(f"CF API {path} timeout")
        return None
    except Exception as e:
        logger.error(f"CF API {path} error: {e}")
        return None

async def cf_user_info(handle: str) -> Optional[dict]:
    """Get user info from CF API"""
    data = await cf_api("user.info", {"handles": handle})
    if data and data.get("status") == "OK":
        return data["result"][0]
    return None

async def cf_user_status(handle: str, count: int = 1000) -> List[dict]:
    """Get user submissions from CF API"""
    data = await cf_api("user.status", {"handle": handle, "count": count})
    if data and data.get("status") == "OK":
        return data["result"]
    return []

async def cf_problemset() -> List[dict]:
    """Get problemset with caching (6 hour refresh)"""
    global PROBLEMSET_CACHE, PROBLEMSET_LAST
    now = time.time()
    
    if not PROBLEMSET_CACHE or now - PROBLEMSET_LAST > 60 * 60 * 6:
        logger.info("Refreshing problemset cache...")
        data = await cf_api("problemset.problems")
        if data and data.get("status") == "OK":
            PROBLEMSET_CACHE = data["result"]["problems"]
            PROBLEMSET_LAST = now
            logger.info(f"Problemset cached: {len(PROBLEMSET_CACHE)} problems")
        else:
            logger.error("Failed to fetch problemset")
    
    return PROBLEMSET_CACHE

# ---------- Points mapping ----------
POINT_MAP = [
    ((800, 899), 1), ((900, 999), 1), ((1000, 1099), 2), ((1100, 1199), 2),
    ((1200, 1299), 3), ((1300, 1399), 3), ((1400, 1499), 4), ((1500, 1599), 5),
    ((1600, 1699), 6), ((1700, 1799), 7), ((1800, 1899), 8), ((1900, 1999), 10),
    ((2000, 2099), 12), ((2100, 2199), 15), ((2200, 2299), 18), ((2300, 2399), 22),
    ((2400, 2499), 27), ((2500, 2599), 33), ((2600, 2699), 40), ((2700, 2799), 48),
    ((2800, 2899), 58), ((2900, 2999), 70), ((3000, 3099), 85),
]

def rating_to_points(rating: int) -> int:
    """Convert problem rating to points"""
    if not isinstance(rating, int):
        return 0
    for (a, b), v in POINT_MAP:
        if a <= rating <= b:
            return v
    return 0

# ---------- Battle flow ----------
def parse_create_args(args: List[str]) -> Tuple[int, List[int]]:
    """Parse /createbattle arguments with better error handling"""
    if not args:
        raise ValueError("missing args")
    
    if len(args) < 2:
        raise ValueError("need at least 2 arguments")
    
    num = int(args[0])
    if num < 1 or num > 10:
        raise ValueError("number of problems must be 1-10")
    
    ratings = [int(x) for x in args[1:]]
    
    # Validate ratings
    for r in ratings:
        if r < 800 or r > 3500:
            raise ValueError("ratings must be 800-3500")
    
    return num, ratings

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    await update.message.reply_text(
        "🤖 Codeforces Battle Bot\n\n"
        "Use /help to see available commands."
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command with all available commands"""
    help_text = """
📚 **Codeforces Battle Bot Commands**

**User Setup:**
/sethandle <handle> - Set your Codeforces handle
/listusers - Show all registered users in this group

**League System:**
/league - View group leaderboard
/userstats - View your personal statistics

**Battle Commands:**
/createbattle <num> <ratings...> - Create a battle
  Examples:
  • /createbattle 3 1200 1400 1600 (3 problems at specific ratings)
  • /createbattle 5 1000 1500 (5 problems, random between 1000-1500)

/cancelbattle - Cancel active battle (creator only)
/cancelround - Skip current round (creator only)

**Other:**
/help - Show this help message

**Points System:**
800-999: 1pt | 1000-1199: 2pts | 1200-1399: 3pts
1400-1499: 4pts | 1500-1599: 5pts | 1600-1699: 6pts
1700-1799: 7pts | 1800-1899: 8pts | 1900-1999: 10pts
2000+: 12-85pts (increases with difficulty)
"""
    await update.message.reply_text(help_text)

async def cmd_sethandle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set Codeforces handle"""
    if not context.args:
        return await update.message.reply_text("Usage: /sethandle <handle>")
    
    handle = context.args[0]
    logger.info(f"User {update.effective_user.id} setting handle: {handle}")
    
    info = await cf_user_info(handle)
    if not info:
        return await update.message.reply_text("❌ Codeforces handle not found")
    
    full = update.effective_user.full_name
    rating = info.get("rating", 0) or 0
    
    await upsert_user(update.effective_chat.id, update.effective_user.id, full, handle, rating)
    await update.message.reply_text(f"✅ Set handle: {handle} (Rating: {rating})")

async def cmd_listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all users in the group"""
    rows = await get_users(update.effective_chat.id)
    if not rows:
        return await update.message.reply_text("No users registered in this group.")
    
    lines = ["👥 **Registered Users**\n"]
    for full, handle, rating, uid in rows:
        lines.append(f"• {full} — {handle} — ⭐{rating}")
    
    await update.message.reply_text("\n".join(lines))

async def cmd_league(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show leaderboard"""
    rows = await get_leaderboard(update.effective_chat.id)
    if not rows:
        return await update.message.reply_text("No leaderboard data yet")
    
    lines = ["🏆 **Leaderboard**\n"]
    for i, (full, handle, rating, pts) in enumerate(rows, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        lines.append(f"{medal} {full} ({handle}) — {pts}pts — ⭐{rating or 0}")
    
    await update.message.reply_text("\n".join(lines))

async def cmd_userstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics"""
    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    
    stats = await get_user_stats(chat_id, uid)
    if not stats:
        return await update.message.reply_text("No stats yet. Join a battle to start!")
    
    pts, battles, first_solves = stats
    await update.message.reply_text(
        f"📊 **Your Statistics**\n\n"
        f"Points: {pts}\n"
        f"Battles: {battles}\n"
        f"First Solves: {first_solves}"
    )

async def cmd_createbattle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Create a new battle"""
    chat_id = update.effective_chat.id
    user = update.effective_user
    
    if chat_id in ACTIVE_BATTLES:
        return await update.message.reply_text("⚠️ A battle is already active in this chat.")
    
    u = await get_user(chat_id, user.id)
    if not u:
        return await update.message.reply_text("❌ You must set your CF handle first with /sethandle")
    
    try:
        num, ratings = parse_create_args(context.args)
    except Exception as e:
        return await update.message.reply_text(
            f"❌ Invalid arguments: {e}\n\n"
            "Usage: /createbattle <num_problems> <rating1> [rating2 ...]\n"
            "Examples:\n"
            "• /createbattle 3 1200 1400 1600\n"
            "• /createbattle 5 1000 1500"
        )
    
    logger.info(f"Battle created by {u[1]} in chat {chat_id}: {num} problems, ratings {ratings}")
    
    state = {
        "creator": user.id,
        "creator_handle": u[1],
        "participants": {user.id: u[1]},
        "num_problems": num,
        "ratings": ratings,
        "selected_problems": [],
        "round": 0,
        "message_id": None,
        "status": "joining",
        "round_winner": None,
        "round_finished": False,
    }
    ACTIVE_BATTLES[chat_id] = state
    
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Ready", callback_data=f"ready:{chat_id}")]])
    msg = await APP.bot.send_message(
        chat_id,
        f"⚔️ **Battle Created!**\n\n"
        f"Creator: {u[1]}\n"
        f"Problems: {num}\n"
        f"Ratings: {', '.join(map(str, ratings))}\n\n"
        f"Click **Ready** to join!\n"
        f"⏱️ Countdown: 30 seconds",
        reply_markup=kb
    )
    state["message_id"] = msg.message_id
    
    # Start countdown
    asyncio.create_task(battle_join_countdown(chat_id, 30))

async def battle_join_countdown(chat_id: int, seconds: int = 30):
    """Countdown timer for battle joining phase"""
    ticks = 6  # Update every 5 seconds
    interval = seconds // ticks
    
    for i in range(ticks):
        await asyncio.sleep(interval)
        st = ACTIVE_BATTLES.get(chat_id)
        if not st or st["status"] != "joining":
            return
        
        try:
            joined = ", ".join(st["participants"].values())
            remaining = seconds - (i + 1) * interval
            text = (
                f"⚔️ **Battle Joining**\n\n"
                f"Participants ({len(st['participants'])}): {joined}\n"
                f"⏱️ Time left: ~{remaining}s"
            )
            await APP.bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=st["message_id"],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Ready", callback_data=f"ready:{chat_id}")]])
            )
        except Exception as e:
            logger.warning(f"Countdown update failed: {e}")
    
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return
    
    if len(st["participants"]) < 2:
        try:
            await APP.bot.send_message(chat_id, "❌ Battle cancelled — not enough participants (minimum 2).")
        except Exception:
            pass
        ACTIVE_BATTLES.pop(chat_id, None)
        logger.info(f"Battle cancelled in chat {chat_id}: insufficient participants")
        return
    
    st["status"] = "running"
    logger.info(f"Battle starting in chat {chat_id} with {len(st['participants'])} participants")
    asyncio.create_task(run_battle(chat_id))

async def select_problems_for_battle(chat_id: int, ratings: List[int], participants: Dict[int, str], num: int) -> List[dict]:
    """Select problems that haven't been attempted by participants"""
    problems = await cf_problemset()
    if not problems:
        return []
    
    # Fetch submissions for all participants in parallel
    attempted = set()
    tasks = [cf_user_status(handle) for handle in participants.values()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for subs in results:
        if isinstance(subs, list):
            for s in subs:
                pr = s.get("problem", {})
                key = f"{pr.get('contestId')}-{pr.get('index')}"
                attempted.add(key)
    
    logger.info(f"Chat {chat_id}: {len(attempted)} problems attempted by participants")
    
    selected = []
    
    # Handle different rating modes
    if len(ratings) == 2 and num > 2:
        # Range mode: random problems between min and max
        min_r, max_r = min(ratings), max(ratings)
        pool = [p for p in problems if isinstance(p.get("rating"), int) and min_r - 50 <= p["rating"] <= max_r + 50]
        random.shuffle(pool)
        
        for p in pool:
            key = f"{p.get('contestId')}-{p.get('index')}"
            if key not in attempted:
                selected.append(p)
                if len(selected) >= num:
                    break
    else:
        # Specific ratings mode
        for r in ratings:
            lo, hi = r - 50, r + 50
            pool = [p for p in problems if isinstance(p.get("rating"), int) and lo <= p["rating"] <= hi]
            random.shuffle(pool)
            
            found = None
            for p in pool:
                key = f"{p.get('contestId')}-{p.get('index')}"
                if key not in attempted:
                    found = p
                    break
            
            if found:
                selected.append(found)
            
            if len(selected) >= num:
                break
    
    logger.info(f"Chat {chat_id}: Selected {len(selected)}/{num} problems")
    return selected

async def run_battle(chat_id: int):
    """Main battle execution loop"""
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return
    
    # Select problems
    st["selected_problems"] = await select_problems_for_battle(
        chat_id, st["ratings"], st["participants"], st["num_problems"]
    )
    
    if not st["selected_problems"] or len(st["selected_problems"]) < st["num_problems"]:
        try:
            await APP.bot.send_message(
                chat_id,
                f"❌ Failed to select enough problems (found {len(st.get('selected_problems', []))} of {st['num_problems']}). Battle cancelled."
            )
        except Exception:
            pass
        ACTIVE_BATTLES.pop(chat_id, None)
        logger.error(f"Chat {chat_id}: Insufficient problems selected")
        return
    
    # Increment battle count for all participants
    await increment_battles(chat_id, list(st["participants"].keys()))
    
    # Run rounds
    for idx, prob in enumerate(st["selected_problems"], start=1):
        st["round"] = idx
        st["round_winner"] = None
        st["round_finished"] = False
        
        await post_round(chat_id, idx, prob)
        
        # Wait for round to finish
        while True:
            await asyncio.sleep(1)
            if st.get("round_finished") or st.get("status") == "cancelled":
                st["round_finished"] = False
                break
        
        if st.get("status") == "cancelled":
            logger.info(f"Chat {chat_id}: Battle cancelled during round {idx}")
            return
        
        # 10 second delay before next round
        if idx < len(st["selected_problems"]):
            await asyncio.sleep(10)
            
            # Tag all participants for next round
            tags = " ".join([f"@{st['participants'][uid]}" for uid in st["participants"]])
            try:
                await APP.bot.send_message(chat_id, f"⏭️ Next round starting! {tags}")
            except Exception:
                pass
    
    await finalize_battle(chat_id)

async def post_round(chat_id: int, round_idx: int, problem: dict):
    """Post a new round with problem details"""
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return
    
    name = problem.get("name")
    contest = problem.get("contestId")
    index = problem.get("index")
    rating = problem.get("rating") or 0
    pts = rating_to_points(rating)
    url = f"https://codeforces.com/problemset/problem/{contest}/{index}"
    
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Finished", callback_data=f"finished:{chat_id}:{round_idx}")]])
    
    try:
        msg = await APP.bot.send_message(
            chat_id,
            f"📝 **Round {round_idx}/{st['num_problems']}**\n\n"
            f"**Problem:** {name}\n"
            f"**Link:** {url}\n"
            f"**Rating:** ⭐{rating}\n"
            f"**Points:** {pts}\n\n"
            f"Solve on Codeforces and click **Finished**!",
            reply_markup=kb,
        )
        st["current_problem"] = problem
        st["current_round_msg"] = msg.message_id
        logger.info(f"Chat {chat_id}: Round {round_idx} posted - {name} ({rating})")
    except Exception as e:
        logger.error(f"Failed to post round: {e}")

async def handle_finished_callback(chat_id: int, user_id: int, round_idx: int) -> Tuple[bool, Any]:
    """Handle when a user clicks Finished button"""
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return False, "no battle"
    
    if user_id not in st["participants"]:
        return False, "not participant"
    
    if st.get("round_winner"):
        return False, "already_won"
    
    handle = st["participants"][user_id]
    logger.info(f"Chat {chat_id}: Checking submissions for {handle}")
    
    subs = await cf_user_status(handle, count=100)
    
    for s in subs:
        pr = s.get("problem", {})
        if (
            pr.get("contestId") == st["current_problem"].get("contestId")
            and pr.get("index") == st["current_problem"].get("index")
            and s.get("verdict") == "OK"
        ):
            st["round_winner"] = (user_id, handle)
            pts = rating_to_points(st["current_problem"].get("rating") or 0)
            await add_points(chat_id, user_id, pts, first_solve=True)
            st["round_finished"] = True
            logger.info(f"Chat {chat_id}: {handle} won round {round_idx} (+{pts}pts)")
            return True, (handle, pts)
    
    return False, "no AC"

async def finalize_battle(chat_id: int):
    """Finalize battle and show results"""
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return
    
    try:
        await APP.bot.send_message(
            chat_id,
            f"🎉 **Battle Finished!**\n\n"
            f"All {st['num_problems']} rounds completed.\n"
            f"Use /league to see updated standings!"
        )
    except Exception:
        pass
    
    ACTIVE_BATTLES.pop(chat_id, None)
    logger.info(f"Chat {chat_id}: Battle finalized")

async def cmd_cancelbattle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel battle with confirmation"""
    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return await update.message.reply_text("❌ No active battle")
    
    if st["creator"] != uid:
        return await update.message.reply_text("❌ Only the creator can cancel the battle")
    
    # Send confirmation
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Cancel", callback_data=f"confirmcancel:{chat_id}"),
            InlineKeyboardButton("❌ No, Continue", callback_data=f"nocancel:{chat_id}")
        ]
    ])
    
    msg = await update.message.reply_text(
        "⚠️ Are you sure you want to cancel this battle?\n"
        "All progress will be lost.",
        reply_markup=kb
    )
    PENDING_CANCELS[chat_id] = msg.message_id

async def cmd_cancelround(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel current round and move to next"""
    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return await update.message.reply_text("❌ No active battle")
    
    if st["creator"] != uid:
        return await update.message.reply_text("❌ Only the creator can cancel rounds")
    
    if st["status"] != "running":
        return await update.message.reply_text("❌ Battle is not running")
    
    current_round = st.get("round", 0)
    if current_round == 0:
        return await update.message.reply_text("❌ No active round")
    
    # Mark round as finished without awarding points
    st["round_finished"] = True
    st["round_winner"] = None
    
    logger.info(f"Chat {chat_id}: Round {current_round} cancelled by creator")
    
    await update.message.reply_text(
        f"⏭️ Round {current_round} cancelled. Moving to next round..."
    )

# ---------- Callback handler ----------
async def cb_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries"""
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    
    try:
        if data.startswith("ready:"):
            await handle_ready_callback(q, data)
        elif data.startswith("finished:"):
            await handle_finished_button(q, data)
        elif data.startswith("confirmcancel:"):
            await handle_confirm_cancel(q, data)
        elif data.startswith("nocancel:"):
            await handle_no_cancel(q, data)
    except Exception as e:
        logger.error(f"Callback error: {e}", exc_info=True)
        await q.answer("An error occurred", show_alert=True)

async def handle_ready_callback(q, data: str):
    """Handle Ready button click"""
    _, chat_s = data.split(":", 1)
    chat_id = int(chat_s)
    st = ACTIVE_BATTLES.get(chat_id)
    
    if not st:
        return await q.edit_message_text("❌ Battle expired or not found")
    
    if st["status"] != "joining":
        return await q.answer("Battle already started", show_alert=True)
    
    uid = q.from_user.id
    u = await get_user(chat_id, uid)
    
    if not u:
        return await q.answer("You must /sethandle first", show_alert=True)
    
    if uid in st["participants"]:
        return await q.answer("You already joined!", show_alert=False)
    
    st["participants"][uid] = u[1]
    joined = ", ".join(st["participants"].values())
    
    try:
        await q.edit_message_text(
            f"⚔️ **Battle Joining**\n\n"
            f"Participants ({len(st['participants'])}): {joined}\n\n"
            f"Click **Ready** to join!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Ready", callback_data=f"ready:{chat_id}")]])
        )
        logger.info(f"Chat {chat_id}: {u[1]} joined battle")
    except Exception as e:
        logger.warning(f"Failed to update join message: {e}")

async def handle_finished_button(q, data: str):
    """Handle Finished button click"""
    parts = data.split(":")
    chat_id = int(parts[1])
    round_idx = int(parts[2])
    
    ok, res = await handle_finished_callback(chat_id, q.from_user.id, round_idx)
    
    if ok:
        handle, pts = res
        try:
            await q.edit_message_text(
                f"🎉 **Round {round_idx} Complete!**\n\n"
                f"Winner: {handle}\n"
                f"Points: +{pts}"
            )
        except Exception as e:
            logger.warning(f"Failed to update round message: {e}")
        
        st = ACTIVE_BATTLES.get(chat_id)
        if st:
            names = ", ".join(st["participants"].values())
            try:
                await APP.bot.send_message(
                    chat_id,
                    f"✅ Round {round_idx} won by {handle}! (+{pts}pts)"
                )
            except Exception:
                pass
    else:
        if res == "already_won":
            await q.answer("Round already completed", show_alert=False)
        elif res == "not participant":
            await q.answer("You're not in this battle", show_alert=True)
        else:
            await q.answer("No AC submission found. Keep trying!", show_alert=True)

async def handle_confirm_cancel(q, data: str):
    """Handle cancel confirmation"""
    _, chat_s = data.split(":", 1)
    chat_id = int(chat_s)
    
    st = ACTIVE_BATTLES.get(chat_id)
    if not st:
        return await q.edit_message_text("❌ Battle no longer active")
    
    # Cancel the battle
    st["status"] = "cancelled"
    st["round_finished"] = True
    ACTIVE_BATTLES.pop(chat_id, None)
    PENDING_CANCELS.pop(chat_id, None)
    
    await q.edit_message_text("🛑 Battle cancelled by creator")
    logger.info(f"Chat {chat_id}: Battle cancelled by user confirmation")

async def handle_no_cancel(q, data: str):
    """Handle cancel rejection"""
    _, chat_s = data.split(":", 1)
    chat_id = int(chat_s)
    
    PENDING_CANCELS.pop(chat_id, None)
    await q.edit_message_text("✅ Battle continues!")

# ---------- Error handler ----------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors"""
    logger.error(f"Exception while handling update: {context.error}", exc_info=context.error)

# ---------- Entrypoint ----------
async def startup(app):
    """Startup tasks"""
    await init_db()
    logger.info("Bot started successfully")

async def shutdown(app):
    """Cleanup on shutdown"""
    logger.info("Bot shutting down...")
    ACTIVE_BATTLES.clear()
    PENDING_CANCELS.clear()

def main():
    """Main entry point"""
    global APP
    
    if not BOT_TOKEN:
        print("❌ Set BOT_TOKEN environment variable before running.")
        return
    
    logger.info("Starting Codeforces Battle Bot...")
    
    # Build application
    APP = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Register handlers
    APP.add_handler(CommandHandler("start", cmd_start))
    APP.add_handler(CommandHandler("help", cmd_help))
    APP.add_handler(CommandHandler("sethandle", cmd_sethandle))
    APP.add_handler(CommandHandler("listusers", cmd_listusers))
    APP.add_handler(CommandHandler("league", cmd_league))
    APP.add_handler(CommandHandler("createbattle", cmd_createbattle))
    APP.add_handler(CommandHandler("userstats", cmd_userstats))
    APP.add_handler(CommandHandler("cancelbattle", cmd_cancelbattle))
    APP.add_handler(CommandHandler("cancelround", cmd_cancelround))
    
    APP.add_handler(CallbackQueryHandler(cb_query))
    
    # Error handler
    APP.add_error_handler(error_handler)
    
    # Startup and shutdown hooks
    APP.post_init = startup
    APP.post_shutdown = shutdown
    
    # Run bot
    logger.info("Bot is now running. Press Ctrl+C to stop.")
    APP.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()