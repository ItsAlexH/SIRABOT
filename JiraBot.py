import discord
from discord.ext import commands
import datetime
import pytz
import os
from dotenv import load_dotenv
import logging
import uuid
import json

# Initial Imports
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event
from gcsa.recurrence import Recurrence, DAILY, SU, SA

from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import GridRangeType
from gcsa.calendar import Calendar

import numpy as np
import pandas as pd
import datetime as datetime
import gspread
from beautiful_date import Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sept, Oct, Nov, Dec
from BotScript import update_or_create_discord_event, eastern
import asyncio
import datetime
import os
from dotenv import load_dotenv
import json

from OrgParse import conversion_excel_date, parse_times, get_color, post_events, get_event_by_search_query, update_events_by_id, Reorganize_Sheet, Verbose_Sheet, update_events_submitted, get_event_submitted
Missing_color = 1
from FSI_Programming import Import_Prog, Reorganize_Sheet_Import

# TODO:
# 0. Editing events on external SOG whenver Deploy_SOG or Edit_SOG are called.
# 1. Import_Prog not importing Sunday events (ONLY IF THE ONGOING CHALLENGES MISSING)
# 2. Weekly Events
# 3. Import_Prog using diff Organize for now (in FSI_Programming) because the other does not delete rows, but DOES the rest of the import stuff right.
# 4. Organize (in OrgParse) not deleting extra rows.
# 5. Merging it all.

# =======================
# External function stubs
# =======================
import os
import gspread
import pandas as pd
import datetime
from gcsa.google_calendar import GoogleCalendar
from typing import Any, MutableMapping

# Assume conversion_excel_date, parse_times, get_color, and post_events are defined elsewhere

async def Deploy_SOG(bot, program: str, week_number: int) -> str:
    print(f'Deploying events for Week #{week_number}...')
    
    if program == "Residential":
        gc = gspread.service_account(filename='service_account.json')
        wks = gc.open(os.getenv("RES_SOG_TOKEN"))
        calendar = GoogleCalendar(os.getenv("RES_CALENDAR_ID"), credentials_path=r'credentials.json')
    elif program == "Online":
        gc = gspread.service_account(filename='service_account.json')
        wks = gc.open(os.getenv("ONLINE_SOG_TOKEN"))
        calendar = GoogleCalendar(os.getenv("ONLINE_CALENDAR_ID"), credentials_path=r'credentials.json')
    elif program == "SIFP":
        gc = gspread.service_account(filename='service_account.json')
        wks = gc.open(os.getenv("SIFP_SOG_TOKEN"))
        calendar = GoogleCalendar(os.getenv("SIFP_CALENDAR_ID"), credentials_path=r'credentials.json')

    cal_data = pd.DataFrame(wks.get_worksheet(week_number + 2).get_all_values(value_render_option='UNFORMATTED_VALUE'))[2:][:]
    headers = cal_data.iloc[0].values
    cal_data.columns = headers
    cal_data = cal_data[1:]

    Dates = cal_data['Date'].tolist()
    Titles = cal_data['Workshop Title'].tolist()

    for j in range(len(Dates)):
        if isinstance(Dates[j], (int, float)):
            Dates[j] = conversion_excel_date(Dates[j])

    last_valid_date = None
    for j in range(len(Dates)):
        if isinstance(Dates[j], datetime.datetime):
            last_valid_date = Dates[j]
        elif Dates[j] == '' and last_valid_date is not None:
            Dates[j] = last_valid_date
        else:
            Dates[j] = None

    Leaders = cal_data['Led By'].tolist()
    Descriptions = cal_data['Description'].tolist()
    Locations = cal_data['Location/Link'].tolist()
    Categories = cal_data['Category'].tolist()
    
    Start_Times = parse_times(Dates, cal_data['Start Time'].tolist())
    End_Times = parse_times(Dates, cal_data['End Time'].tolist())
    Event_IDs = cal_data['Event ID'].tolist()
    Colors = get_color(Categories)
    
    print(f"Event IDs = {Event_IDs}")
    Descriptions_mask = [val == '' for val in Descriptions]
    Locations_mask = [val == '' for val in Locations]
    Leaders_mask = [val == '' for val in Leaders]

    if program == "Online":
        IDCol = 10
    else: 
        IDCol = 9
    
    await post_events(bot, wks.get_worksheet(week_number + 2), week_number, IDCol, program, calendar, p=(Titles, Leaders, Leaders_mask, Dates, 
                                                                                                                 Start_Times, End_Times, Locations, Locations_mask, Descriptions, Descriptions_mask, Categories, Event_IDs, Colors))
    ## Deploy external SOG
    source_wks = wks.get_worksheet(week_number + 2)
    new_sheet_info = source_wks.copy_to(os.getenv("SIFP_SOG_EXTERNAL_TOKEN"))
    
    # Open the destination spreadsheet
    destination_spreadsheet = gc.open_by_key(os.getenv("SIFP_SOG_EXTERNAL_TOKEN"))
    
    # Get the newly created worksheet
    new_wks = destination_spreadsheet.get_worksheet_by_id(new_sheet_info['sheetId'])
    
    # Conditionally delete and re-order the sheets
    worksheets = destination_spreadsheet.worksheets()

    # We check if the number of worksheets is equal to week_number + 1 because we have a "Welcome Sheet" at index 0
    if len(worksheets) == week_number + 2:
        # Delete the worksheet at index 1 (the 2nd sheet), since we don't want to touch the welcome sheet
        destination_spreadsheet.del_worksheet(worksheets[1])

    # Update the title of the new worksheet to match the original
    new_wks.update_title(source_wks.title)

    # Delete column K
    request = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        'sheetId': new_sheet_info['sheetId'],
                        "dimension": "COLUMNS",
                        "startIndex": 10,
                        "endIndex": 11
                    }
                }
            }
        ]
    }
    destination_spreadsheet.batch_update(request)

    # Move the new worksheet to the first position after the welcome sheet
    # The new index is 1, as the welcome sheet is at index 0.
    request = {
        'updateSheetProperties': {
            'properties': {
                'sheetId': new_sheet_info['sheetId'],
                'index': 1,
            },
            'fields': 'index',
        }
    }
    destination_spreadsheet.batch_update({'requests': [request]})

    return f"SOG deployed for {program} (Week {week_number})."

def Import_Programming(program: str, week_number: int, import_type: int) -> str:
    gc = gspread.service_account(filename='service_account.json')
    if(program == "Residential"):
        wks_PROG = gc.open(os.getenv("RES_PROG_TOKEN"))
        wks_SOG = gc.open(os.getenv("RES_SOG_TOKEN"))
    elif(program == "Online"):
        wks_PROG = gc.open(os.getenv("ONLINE_PROG_TOKEN"))
        wks_SOG = gc.open(os.getenv("ONLINE_SOG_TOKEN"))
    elif(program == "SIFP"):
        wks_PROG = gc.open(os.getenv("SIFP_PROG_TOKEN"))
        wks_SOG = gc.open(os.getenv("SIFP_SOG_TOKEN"))

    Import_Prog(program, wks_PROG, wks_SOG, week_number, import_type)
    Reorganize_Sheet_Import(program, wks_SOG, week_number)
    if(import_type == 0):
        label = "Programming"
    elif(import_type == 1):
        label = "Cocurricular"
    elif(import_type == 2):
        label = "All"
    return f"Imported {label} for {program} (Week {week_number})."

def Submit_Event(program: str, payload: dict) -> str:
    gc = gspread.service_account(filename='service_account.json')
    wks = gc.open(os.getenv("SUBMITTED_EVENTS_TOKEN"))
    if(program == "Residential"):
        wks_prog =  wks.get_worksheet(0)
    elif(program == "Online"):
        wks_prog =  wks.get_worksheet(1)
    elif(program == "SIFP"):
        wks_prog =  wks.get_worksheet(0)

    hosts_string = ", ".join(payload.get('hosts', []))

    row_data = [
        payload.get('date'), 
        payload.get('start_time'), 
        payload.get('end_time'),
        payload.get('title'), 
        hosts_string,
        payload.get('description'), 
        payload.get('halps'),
        payload.get('location'),
        payload.get('recurrence')
    ]
    wks_prog.append_row(row_data)
    return f"Event '{payload.get('title','(untitled)')}' submitted for {program}."
    
################################################################################################
######################## Bot Configuration & Setup #############################################
################################################################################################

load_dotenv()
EASTERN_TZ = pytz.timezone('US/Eastern')
BOT_TOKEN = os.getenv("SIRA_BOT_TOKEN")

USER_USERID = {}
map_str = os.getenv("USER_MAP")
if map_str:
    try:
        pairs = map_str.split(',')
        for pair in pairs:
            user_name, user_id_str = pair.split(':')
            USER_USERID[user_name.strip()] = int(user_id_str.strip())
    except ValueError:
        logging.error("Invalid format for USER_USERID in .env file. Please use 'user_name:user_id, user_name:user_id'")

USERS = set(USER_USERID.keys())
USER_IDS = set(USER_USERID.values())
USERID_USER = {user_id: user_name for user_name, user_id in USER_USERID.items()}

ROLE_CHANNEL_MAP = {}
map_str = os.getenv("ROLE_CHANNEL_MAP")
if map_str:
    try:
        pairs = map_str.split(',')
        for pair in pairs:
            role_id, channel_id = map(int, pair.split(':'))
            ROLE_CHANNEL_MAP[role_id] = channel_id
    except ValueError:
        logging.error("Invalid format for ROLE_CHANNEL_MAP in .env file. Please use 'role_id:channel_id,role_id:channel_id'")
NOTIFICATION_ROLE_IDS = set(ROLE_CHANNEL_MAP.keys())

log_handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='a')
logging.basicConfig(level=logging.INFO, handlers=[log_handler])
logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True

bot = commands.Bot(command_prefix='!', intents=intents)
bot.owner_id = USER_USERID.get("ALEX", None)

################################################################################################
######################## Bot Cog ###############################################################
################################################################################################

class SIRA_BOT(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.tracked_dm_ids = set()
        self.tracked_channel_message_ids = set()

    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f'Logged in as {self.bot.user.name} (ID: {self.bot.user.id})')
        print(f'{self.bot.user.name} is live & connected to Discord.')

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.id == self.bot.user.id:
            return

        for mentioned_member in message.mentions:
            if mentioned_member.id in USER_IDS:
                await self.send_tag_notification_dm(
                    message,
                    mentioned_member.id,
                    USERID_USER[mentioned_member.id]
                )

        for role in message.role_mentions:
            if role.id in ROLE_CHANNEL_MAP:
                channel_id = ROLE_CHANNEL_MAP[role.id]
                await self.notify_channel(message, role, channel_id)

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, reactor: discord.Member | discord.User):
        if reactor.id == self.bot.user.id:
            return

        if str(reaction.emoji) == '👍' and isinstance(reaction.message.channel, discord.DMChannel):
            if reactor.id in USER_IDS and reaction.message.id in self.tracked_dm_ids:
                logger.info(f"{USERID_USER[reactor.id]} reacted 👍 to trackable DM (ID: {reaction.message.id}). Deleting...")
                try:
                    await reaction.message.delete()
                    self.tracked_dm_ids.discard(reaction.message.id)
                    logger.info(f"DM (ID: {reaction.message.id}) deleted successfully.")
                except (discord.Forbidden, discord.HTTPException) as e:
                    logger.error(f"Error deleting DM (ID: {reaction.message.id}): {e}")

        if str(reaction.emoji) == '👍' and isinstance(reaction.message.channel, discord.TextChannel):
            if reaction.message.id in self.tracked_channel_message_ids:
                member = reaction.message.guild.get_member(reactor.id)
                if member and member.guild_permissions.manage_messages:
                    logger.info(f"Member {member.display_name} reacted 👍 to trackable channel message (ID: {reaction.message.id}). Deleting...")
                    try:
                        await reaction.message.delete()
                        self.tracked_channel_message_ids.discard(reaction.message.id)
                        logger.info(f"Channel message (ID: {reaction.message.id}) deleted successfully.")
                    except (discord.Forbidden, discord.HTTPException) as e:
                        logger.error(f"Error deleting channel message (ID: {reaction.message.id}): {e}")
                else:
                    logger.info(f"Member {reactor.display_name} reacted but lacks permissions to delete message.")

    async def send_tag_notification_dm(self, message: discord.Message, user_id: int, name: str):
        if user_id == self.bot.user.id:
            return

        logger.info(f"Tag detected for {name} (ID: {user_id}).")
        guild_name = message.guild.name if message.guild else "Direct Message"
        channel_name = message.channel.name if isinstance(message.channel, discord.TextChannel) else "Direct Message"
        current_time = datetime.datetime.now(EASTERN_TZ).strftime('%Y-%m-%d %I:%M %p')
        hyper_link = message.jump_url if message.guild else "N/A (Direct Message)"

        dm_content = (
            f"**You were tagged, {name}!**\n"
            f"**Time:** {current_time}\n"
            f"**Server:** {guild_name}\n"
            f"**Channel:** #{channel_name}\n"
            f"**Tagged By:** {message.author.display_name}\n"
            f"**Message:** ```{message.clean_content}```\n"
            f"[View Message]({hyper_link})\n\n"
            f"*React with 👍 to delete this message.*"
        )
        user = self.bot.get_user(user_id)
        if user:
            try:
                sent_dm = await user.send(dm_content)
                self.tracked_dm_ids.add(sent_dm.id)
                logger.info(f"DM sent to {name} (ID: {user_id}). Tracking DM ID: {sent_dm.id}")
            except discord.Forbidden:
                logger.warning(f"Could not DM {name} (ID: {user_id}).")
            except Exception as e:
                logger.error(f"Error sending DM to {name} (ID: {user_id}): {e}")
        else:
            logger.warning(f"Could not find user for ID: {user_id}.")

    async def notify_channel(self, message: discord.Message, role: discord.Role, channel_id: int):
        channel = self.bot.get_channel(channel_id)
        if not channel:
            logger.error(f"Channel with ID {channel_id} not found.")
            return

        notification_content = (
            f"A message tagged the role {role.mention}!\n"
            f"**Tagged By:** {message.author.mention}\n"
            f"**In Channel:** {message.channel.mention}\n"
            f"**Original Message:** ```{message.clean_content}```\n"
            f"[Jump to Message]({message.jump_url})\n\n"
            f"*React with 👍 to delete this message.*"
        )
        try:
            sent_message = await channel.send(notification_content)
            self.tracked_channel_message_ids.add(sent_message.id)
            logger.info(f"Notification sent to channel {channel.name} for role tag: {role.name}. Tracking ID: {sent_message.id}")
        except discord.Forbidden:
            logger.error(f"Bot lacks permissions to send messages to channel {channel.name}.")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending channel notification: {e}")

    @commands.command(name='cleardms')
    async def clear_bot_dms(self, ctx: commands.Context):
        user_to_clear = ctx.author
        if user_to_clear.dm_channel is None:
            await user_to_clear.create_dm()

        dm_channel = user_to_clear.dm_channel
        if not dm_channel:
            await ctx.send(f"Error: Could not access your DM channel, {user_to_clear.mention}. DM cleaning failed.")
            return

        initial_message = await ctx.send(f"Alright {ctx.author.mention}, I'm starting to clear all DMs I've sent you. This might take a moment...")
        deleted_count = 0
        try:
            async for message in dm_channel.history(limit=None):
                if message.author.id == self.bot.user.id:
                    try:
                        await message.delete()
                        deleted_count += 1
                        self.tracked_dm_ids.discard(message.id)
                    except (discord.Forbidden, discord.NotFound):
                        pass
                    except Exception as e:
                        logger.error(f"Error deleting DM message (ID: {message.id}): {e}")

            await initial_message.edit(content=f"✅ Finished! I deleted **{deleted_count}** of my DMs from our chat, {ctx.author.mention}.")
            logger.info(f"Successfully deleted {deleted_count} DMs for user {ctx.author.display_name}.")
        except discord.Forbidden:
            await initial_message.edit(content=f"I don't have permission to read/delete messages in our DMs. Please check my privacy settings to allow DMs from server members/bots.")
        except discord.HTTPException as e:
            await initial_message.edit(content=f"An error occurred while accessing DMs: {e}")
        except Exception as e:
            await initial_message.edit(content=f"An unexpected error occurred: {e}")

    @commands.command(name='commands')
    async def help_command(self, ctx: commands.Context):
        sent_message = await ctx.send("(1) **!cleardms**: clear the user's SIRABOT direct messages")
        self.tracked_channel_message_ids.add(sent_message.id)

    @commands.command(name='events')
    async def events(self, ctx: commands.Context):
        await self.bot.wait_until_ready()
        author = ctx.author
        channel = ctx.channel

        try:
            await ctx.message.delete()
        except Exception:
            pass

        def make_check():
            return lambda m: (m.author.id == author.id and m.channel.id == channel.id)

        async def ask(prompt: str, validate=None, parse=None, timeout: int = 120):
            msg = await channel.send(prompt)
            self.tracked_channel_message_ids.add(msg.id)
            try:
                reply = await self.bot.wait_for('message', check=make_check(), timeout=timeout)
            except asyncio.TimeoutError:
                await channel.send("⏰ Timed out. Please run `!events` again.")
                return None
            content = reply.content.strip()
            if content.lower() == "cancel":
                await channel.send("❌ Cancelled.")
                return None
            
            if parse:
                try:
                    parsed_content = parse(content)
                    content = parsed_content
                except ValueError as e:
                    await channel.send(f"⚠️ Invalid format: {e}. Please try again or type `cancel`.")
                    return await ask(prompt, validate, parse, timeout)
            
            if validate and not validate(content):
                await channel.send("⚠️ Invalid choice. Please try again or type `cancel`.")
                return await ask(prompt, validate, parse, timeout)
            return content

        # programs = {"1": "Residential", "2": "Online", "3": "SIFP",
        #             "residential": "Residential", "online": "Online", "sifp": "SIFP"}
        # program = await ask(
        #     "**Which Program?**\n(1) Residential\n(2) Online\n(3) SIFP\n\nType a number or name (or `cancel`).",
        #     validate=lambda x: str(x).lower() in programs,
        #     parse=lambda s: programs[s.lower()]
        # )
        # if program is None:
        #     return
        
        program = "SIFP"
        if author.id in USER_IDS:
            tasks = {
                "1": "Deploy SOG", "2": "Import Programming", "3": "Submit Event",
                "4": "Edit Event", "5": "Cancel Event", "6": "Update Tokens",
                "deploy sog": "Deploy SOG", "import programming": "Import Programming",
                "submit event": "Submit Event", "edit event": "Edit Event",
                "cancel event": "Cancel Event", "update tokens": "Update Tokens"
            }
            task_prompt = f"**What do you want to do for _{program}_?**\n(1) Deploy SOG\n(2) Import Programming\n(3) Submit Event\n(4) Edit Event\n(5) Cancel Event\n\nType a number or name (or `cancel`)."
        else:
            tasks = {
                "1": "Submit Event", "2": "Edit Event",
                "submit event": "Submit Event", "edit event": "Edit Event",
            }
            task_prompt = f"**What do you want to do for _{program}_?**\n(1) Submit Event\n(2) Edit Event\n\nType a number or name (or `cancel`)."

        task = await ask(
            task_prompt,
            validate=lambda x: str(x).lower() in tasks,
            parse=lambda s: tasks[s.lower()]
        )
        if task is None:
            return

        def parse_week_number(s: str) -> int:
            n = int(s)
            if n <= 0:
                raise ValueError("Week number must be a positive integer.")
            return n

        def parse_prog_type(s: str) -> int:
            s = s.strip().lower()
            if s in ("p", "programming", "0"): return 0
            if s in ("c", "cocurricular", "1"): return 1
            if s in ("a", "all", "2"): return 2
            raise ValueError("Enter 'Programming' or 'Cocurricular' (or P/C).")

        def parse_date_mmddyy(s: str) -> datetime.date:
            s = s.strip()
            for fmt in ("%m/%d/%y", "%m/%d/%Y"):
                try:
                    return datetime.datetime.strptime(s, fmt).date()
                except ValueError: continue
            raise ValueError("Use MM/DD/YY or MM/DD/YYYY.")

        def parse_time_ampm(s: str) -> datetime.time:
            s = s.strip().upper().replace(".", "")
            tried = ["%I:%M %p", "%I %p"]
            for fmt in tried:
                try:
                    return datetime.datetime.strptime(s, fmt).time()
                except ValueError: continue
            raise ValueError("Use times like '9:00 AM' or '1 PM'.")
            
        def is_valid_uuid(uuid_to_test, version=4):
            try:
                uuid.UUID(uuid_to_test, version=version)
            except ValueError:
                return False
            return True

        if task == "Deploy SOG":
            week = await ask(
                f"**Deploy SOG** for _{program}_ — enter **Week Number** (positive integer):",
                parse=parse_week_number
            )
            if week is None: return
            await channel.send(f"🚀 Deploying SOG for **{program}**, Week **{week}**...")
            result = await Deploy_SOG(self.bot, program, week)
            await channel.send(f"✅ {result}")
            return

        if task == "Import Programming":
            week = await ask(
                f"**Import Programming** for _{program}_ — enter **Week Number** (positive integer):",
                parse=parse_week_number
            )
            if(program != "SIFP"):
                if week is None: return
                typ = await ask(
                    "**Import Programming, Cocurricular, or All?** (P/C/A or full word):",
                    parse=parse_prog_type
                )
                if typ is None: return
                label = "Cocurricular" if typ == 1 else "Programming"
                await channel.send(f"📥 Importing **{label}** for **{program}**, Week **{week}**...")
                result = Import_Programming(program, week, typ)
            else:
                typ = 0
                label = "Programming"
                await channel.send(f"📥 Importing **{label}** for **{program}**, Week **{week}**...")
                result = Import_Programming(program, week, typ)
            await channel.send(f"✅ {result}")
            return

        if task == "Submit Event":
            date = await ask("Enter **Event Date** (e.g., `09/30/25`):", parse=parse_date_mmddyy)
            if date is None: return
            start_t = await ask("Enter **Start Time** (e.g., `9:00 AM`):", parse=parse_time_ampm)
            if start_t is None: return
            end_t = await ask("Enter **End Time** (e.g., `10:00 AM`):", parse=parse_time_ampm)
            if end_t is None: return
            title = await ask("Enter **Event Title**:")
            if title is None: return
            description = await ask("Enter **Event Description** (you can paste multi-line text):")
            if description is None: return
            hosts = await ask("Enter **Host & CoHosts** (comma-separated):")
            if hosts is None: return
            halps = await ask("Enter **Suggested HALPS Category** (`H`, `A`, `L`, `P`, or `S`):",
                            validate=lambda s: s.strip().upper() in {"H", "A", "L", "P", "S"},
                            parse=lambda s: s.strip().upper())
            if halps is None: return
            location = await ask("Enter **Location**:")
            if location is None: return
            recurrence = await ask("Is this event recurring? (No, Weekly, Biweekly)")

            payload = {
                "date": date.strftime('%m/%d/%Y'), "start_time": start_t.strftime("%I:%M %p"),
                "end_time": end_t.strftime("%I:%M %p"), "title": title,
                "description": description, "hosts": [h.strip() for h in hosts.split(",") if h.strip()],
                "halps": halps, "location": location, "recurrence": recurrence
            }

            await channel.send(f"📝 Submitting event **{title}** for **{program}**...")
            result = Submit_Event(program, payload)
            await channel.send(f"✅ {result}")
            return
    
        if task == "Edit Event":
            event_stage = await ask(
                "**What is the stage of the event?**\n(1) Deployed (live on the SOG)\n(2) Submitted"
            )
            if event_stage is None: return
            
            if event_stage.lower() in ("submitted", "2"):
                search_query = await ask(
                    "Enter the **Event Title**:",
                )
                if search_query is None: return

                gc = gspread.service_account(filename='service_account.json')
                wks = gc.open(os.getenv("SUBMITTED_EVENTS_TOKEN"))
                if program == "Residential" or program == "SIFP":
                    wks_prog = wks.get_worksheet(0)
                elif program == "Online":
                    wks_prog = wks.get_worksheet(1)
                else:
                    await channel.send("Invalid program for submitted events. Aborting.")
                    return

                events_to_edit = get_event_submitted(wks_prog, search_query)
                
                if not events_to_edit:
                    await channel.send(f"⚠️ No event found for '{search_query}'. Please try again or type `cancel`.")
                    return

                event_to_edit = None
                if len(events_to_edit) > 1:
                    message_content = "Which one do you want to edit?\n\n"
                    for i, event in enumerate(events_to_edit):
                        date_str = event.get('Event Date')
                        start_time_str = event.get('Start Time')
                        
                        if date_str and start_time_str:
                            try:
                                combined_datetime_str = f"{date_str} {start_time_str}"
                                dt_obj = datetime.datetime.strptime(combined_datetime_str, "%m/%d/%Y %I:%M %p")
                                formatted_display = dt_obj.strftime("%m/%d/%y @ %I:%M %p").lstrip("0").replace(" 0", " ")
                            except ValueError:
                                formatted_display = f"{date_str} @ {start_time_str}"
                        else:
                            formatted_display = "Date/Time not available"

                        message_content += f"**({i+1})** `{event.get('Event Title')}` on `{formatted_display}`\n"
                    
                    choice = await ask(message_content, validate=lambda x: str(x).isdigit() and 1 <= int(x) <= len(events_to_edit), parse=int)
                    if choice:
                        event_to_edit = events_to_edit[choice - 1]
                    else:
                        return
                else:
                    event_to_edit = events_to_edit[0]
                
                await channel.send(f"Found event: **{event_to_edit.get('Event Title', 'N/A')}** on **{event_to_edit.get('Event Date', 'N/A')}**.")
                
                fields_to_edit_raw = await ask(
                    "Which fields would you like to edit? Type `all` or a comma-separated list of numbers/names:\n"
                    "(1) title\n(2) date\n(3) start time\n(4) end time\n"
                    "(5) hosts\n(6) description\n(7) halps\n(8) location"
                )
                if fields_to_edit_raw is None: return

                field_options = {
                    "1": "title", "title": "title", "2": "date", "date": "date",
                    "3": "start_time", "start time": "start_time", "4": "end_time", "end time": "end_time",
                    "5": "hosts", "hosts": "hosts", "6": "description", "description": "description",
                    "7": "halps", "halps": "halps", "8": "location", "location": "location",
                    "all": "all"
                }

                fields_to_edit = []
                if "all" in fields_to_edit_raw.lower():
                    fields_to_edit = list(field_options.values())[:-1]
                else:
                    for part in fields_to_edit_raw.split(','):
                        field_key = field_options.get(part.strip().lower())
                        if field_key and field_key not in fields_to_edit:
                            fields_to_edit.append(field_key)

                if not fields_to_edit:
                    await channel.send("No valid fields selected. Aborting.")
                    return

                update_args = {}
                for field in fields_to_edit:
                    prompt = f"Enter the new value for **{field}**:"
                    
                    if field == "date":
                        new_value = await ask(f"{prompt} (e.g., `09/30/25`):", parse=parse_date_mmddyy)
                        if new_value: update_args[field] = new_value.strftime("%m/%d/%Y")
                    elif field in ["start_time", "end_time"]:
                        new_value = await ask(f"{prompt} (e.g., `9:00 AM`):", parse=parse_time_ampm)
                        if new_value: update_args[field] = new_value.strftime("%I:%M %p").lstrip('0')
                    else:
                        new_value = await ask(prompt)
                        if new_value: update_args[field] = new_value

                if not update_args:
                    await channel.send("No new values were provided. Aborting.")
                    return

                await channel.send(f"📝 Updating submitted event **{event_to_edit.get('Event Title', 'N/A')}**...")
                
                update_events_submitted(wks_prog, event_to_edit, update_args)
                
                await channel.send(f"✅ Event updated successfully!")

            elif event_stage.lower() in ("deployed", "1"):
                ## Search for deployed event
                search_query = await ask(
                    "Enter the **Event Title**, **Discord ID**, **Calendar ID**, or **UUID** to find the event:",
                )
                if search_query is None: return

                event_to_edit = get_event_by_search_query(search_query)

                if isinstance(event_to_edit, list):
                    if not event_to_edit:
                        await channel.send(f"⚠️ No event found for '{search_query}'. Please try again or type `cancel`.")
                        return
                    message_content = "Which one do you want to edit?\n\n"
                    for i, event in enumerate(event_to_edit):
                        try:
                            date_obj = datetime.datetime.fromisoformat(event['date'])
                            start_time_obj = datetime.datetime.fromisoformat(event['start_time'])
                            combined_dt = datetime.datetime.combine(date_obj.date(), start_time_obj.time())
                            formatted_display = combined_dt.strftime("%m/%d/%y @ %I:%M %p").lstrip("0").replace(" 0", " ")
                        except (ValueError, TypeError):
                            formatted_display = f"{event.get('date')} @ {event.get('start_time')}"
                        message_content += f"**({i+1})** `{event.get('title')}` on `{formatted_display}`\n"
                    
                    choice = await ask(message_content, validate=lambda x: str(x).isdigit() and 1 <= int(x) <= len(event_to_edit), parse=int)
                    if choice:
                        event_to_edit = event_to_edit[choice - 1]
                    else:
                        return
                
                if event_to_edit is None:
                    await channel.send(f"⚠️ Event with that ID or title was not found. Please try again.")
                    return

                await channel.send(f"Found event: **{event_to_edit.get('title', 'N/A')}** on **{event_to_edit.get('date', 'N/A')}**.")
                
                fields_to_edit_raw = await ask(
                    "Which fields would you like to edit? Type `all` or a comma-separated list of numbers/names:\n"
                    "(1) title\n(2) date\n(3) start time\n(4) end time\n"
                    "(5) leaders\n(6) location\n(7) category\n"
                    "(8) description\n(9) recording\n(10) status\n"
                    "(11) duration\n"
                )
                if fields_to_edit_raw is None: return

                field_options = {
                    "1": "title", "title": "title", "2": "date", "date": "date",
                    "3": "start_time", "start time": "start_time", "4": "end_time", "end time": "end_time",
                    "5": "leaders", "leaders": "leaders", "6": "location", "location": "location",
                    "7": "category", "category": "category", "8": "description", "description": "description",
                    "9": "recording", "recording": "recording", "10": "status", "status": "status",
                    "11": "duration", "duration": "duration",
                    "all": "all"
                }

                fields_to_edit = []
                if "all" in fields_to_edit_raw.lower():
                    fields_to_edit = list(field_options.values())[:-1]
                else:
                    for part in fields_to_edit_raw.split(','):
                        field_key = field_options.get(part.strip().lower())
                        if field_key and field_key not in fields_to_edit:
                            fields_to_edit.append(field_key)

                if not fields_to_edit:
                    await channel.send("No valid fields selected. Aborting.")
                    return

                update_args = {}
                for field in fields_to_edit:
                    prompt = f"Enter the new value for **{field}**:"
                    
                    if field == "date":
                        new_value = await ask(f"{prompt} (e.g., `09/30/25`):", parse=parse_date_mmddyy)
                        if new_value: update_args[field] = new_value
                    elif field == "start_time":
                        new_value = await ask(f"{prompt} (e.g., `9:00 AM`):", parse=parse_time_ampm)
                        if new_value:
                            update_args[field] = new_value
                            duration_str = await ask("Enter new **duration** in minutes (e.g., `60`):", parse=int)
                            if duration_str is not None:
                                duration = datetime.timedelta(minutes=duration_str)
                                event_date_obj = datetime.datetime.fromisoformat(event_to_edit["date"]).date()
                                start_datetime = datetime.datetime.combine(event_date_obj, new_value)
                                end_datetime = start_datetime + duration
                                update_args["end_time"] = end_datetime.time()
                    elif field == "end_time":
                        new_value = await ask(f"{prompt} (e.g., `10:00 AM`):", parse=parse_time_ampm)
                        if new_value:
                            update_args[field] = new_value
                            duration_str = await ask("Enter new **duration** in minutes (e.g., `60`):", parse=int)
                            if duration_str is not None:
                                duration = datetime.timedelta(minutes=duration_str)
                                event_date_obj = datetime.datetime.fromisoformat(event_to_edit["date"]).date()
                                end_datetime = datetime.datetime.combine(event_date_obj, new_value)
                                start_datetime = end_datetime - duration
                                update_args["start_time"] = start_datetime.time()
                    elif field == "duration":
                        new_value = await ask(f"{prompt} (in minutes, e.g., `60`):", parse=int)
                        if new_value is not None:
                            duration = datetime.timedelta(minutes=new_value)
                            reference_time_str = await ask("Adjust based on **start time** or **end time**? (`start`/`end`)", validate=lambda x: x.lower() in ("start", "end"))
                            if reference_time_str:
                                event_date_obj = datetime.datetime.fromisoformat(event_to_edit["date"]).date()
                                if reference_time_str.lower() == "start":
                                    start_time = datetime.datetime.fromisoformat(event_to_edit["start_time"]).time()
                                    start_datetime = datetime.datetime.combine(event_date_obj, start_time)
                                    end_datetime = start_datetime + duration
                                    update_args["end_time"] = end_datetime.time()
                                else:
                                    end_time = datetime.datetime.fromisoformat(event_to_edit["end_time"]).time()
                                    end_datetime = datetime.datetime.combine(event_date_obj, end_time)
                                    start_datetime = end_datetime - duration
                                    update_args["start_time"] = start_datetime.time()
                    else:
                        new_value = await ask(prompt)
                        if new_value: update_args[field] = new_value

                if not update_args:
                    await channel.send("No new values were provided. Aborting.")
                    return
                
                if 'update_events_by_id' not in globals():
                    await channel.send("Error: 'update_events_by_id' function is not available. Check bot configuration.")
                    return

                await channel.send(f"🔄 Updating event **{event_to_edit.get('title', 'N/A')}**...")
                
                if(program == "Residential"):
                    gc = gspread.service_account(filename='service_account.json')
                    wks = gc.open(os.getenv("RES_SOG_TOKEN"))
                    calendar = GoogleCalendar(os.getenv("RES_CALENDAR_ID"), credentials_path=r'credentials.json')
                elif(program == "Online"):
                    gc = gspread.service_account(filename='service_account.json')
                    wks = gc.open(os.getenv("ONLINE_SOG_TOKEN"))
                    calendar = GoogleCalendar(os.getenv("ONLINE_CALENDAR_ID"), credentials_path=r'credentials.json')
                elif(program == "SIFP"):
                    gc = gspread.service_account(filename='service_account.json')
                    wks = gc.open(os.getenv("SIFP_SOG_TOKEN"))
                    calendar = GoogleCalendar(os.getenv("SIFP_CALENDAR_ID"), credentials_path=r'credentials.json')
                    
                await update_events_by_id(self.bot, wks, program, calendar, event_to_edit["id"], update_args)
                
                week_number = event_to_edit["week"]
                ## Deploy external SOG
                source_wks = wks.get_worksheet(week_number + 2)
                new_sheet_info = source_wks.copy_to(os.getenv("SIFP_SOG_EXTERNAL_TOKEN"))
                
                # Open the destination spreadsheet
                destination_spreadsheet = gc.open_by_key(os.getenv("SIFP_SOG_EXTERNAL_TOKEN"))
                
                # Get the newly created worksheet
                new_wks = destination_spreadsheet.get_worksheet_by_id(new_sheet_info['sheetId'])
                
                # Conditionally delete and re-order the sheets
                worksheets = destination_spreadsheet.worksheets()

                # We check if the number of worksheets is equal to week_number + 1 because we have a "Welcome Sheet" at index 0
                if len(worksheets) == week_number + 2:
                    # Delete the worksheet at index 1 (the 2nd sheet), since we don't want to touch the welcome sheet
                    destination_spreadsheet.del_worksheet(worksheets[1])

                # Update the title of the new worksheet to match the original
                new_wks.update_title(source_wks.title)

                # Delete column K
                request = {
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    'sheetId': new_sheet_info['sheetId'],
                                    "dimension": "COLUMNS",
                                    "startIndex": 10,
                                    "endIndex": 11
                                }
                            }
                        }
                    ]
                }
                destination_spreadsheet.batch_update(request)

                # Move the new worksheet to the first position after the welcome sheet
                # The new index is 1, as the welcome sheet is at index 0.
                request = {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': new_sheet_info['sheetId'],
                            'index': 1,
                        },
                        'fields': 'index',
                    }
                }
                destination_spreadsheet.batch_update({'requests': [request]})
                
                await channel.send(f"✅ Event updated successfully!")
                return
        
        if task == "Cancel Event":
            return
        if task == "Update Tokens":
            return
async def main():
    await bot.add_cog(SIRA_BOT(bot))
    await bot.start(BOT_TOKEN)

if __name__ == '__main__':
    try:
        import asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot shutdown detected.")