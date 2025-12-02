import discord
from discord.utils import utcnow
import datetime
import pytz
import os
from dotenv import load_dotenv
import asyncio

################################################################################################
######################## Discord Event Sequence ############################################
################################################################################################
eastern = pytz.timezone('US/Eastern')
# Load environment variables from .env file
load_dotenv()
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))


# --- Core Function to Update/Create Discord Event ---
async def update_or_create_discord_event(bot, program:str, event_name: str, event_description: str, event_start_time: datetime.datetime,
    event_end_time: datetime.datetime,  event_location: str, discord_id = None, status = None):

    # await bot_ready_event.wait()
    # print("Bot confirmed ready before event operation.")

    if(program == "Residential"):
        guild = bot.get_guild(int(os.getenv("RES_DISCORD_GUILD_ID")))
        
    elif(program == "Online"):
        guild = bot.get_guild(int(os.getenv("ONLINE_DISCORD_GUILD_ID")))
        
    elif(program == "SIFP"):
        guild = bot.get_guild(int(os.getenv("SIFP_DISCORD_GUILD_ID")))
        
        
    if not guild: # Add a check to handle the case where the guild is not found
        print(f"Error: Guild not found.")
        return False

    intents = discord.Intents.default()
    intents.guilds = True
    intents.guild_scheduled_events = True # Crucial for scheduled events

    external_event_type = discord.EntityType.external
    scheduled_events = await guild.fetch_scheduled_events()
    found_event = None ## update to updating with event_id

    current_time = eastern.localize(datetime.datetime.now())
    
    if(discord_id != None):
        for event in scheduled_events:
            if event.id == discord_id:
                found_event = event
                break
    try:
        if found_event:
            print(f'Editing Existing Event: {event_name} (ID: {found_event.id})!')
            # Compare already localized times
            if event_start_time > current_time:
                if(status != "Canceled"):
                    found_event = await found_event.edit(
                            name=event_name,
                            description=event_description,
                            location=event_location,
                            start_time=event_start_time,
                            end_time=event_end_time,
                            privacy_level=discord.PrivacyLevel.guild_only)
                    print(f'Successfully updated event: {event_name}')
                    return found_event.id
                else:
                    found_event = await found_event.delete()
                    print(f'Successfully deleted event: {event_name}')
                    return found_event.id
            print(f'Event in the past!')
            return None
        else:
            print(f'Creating New Event: {event_name}!')
            # Compare already localized times
            if event_start_time > current_time:
                created_event = await guild.create_scheduled_event(
                    name=event_name,
                    description=event_description,
                    start_time=event_start_time,
                    entity_type=external_event_type,
                    privacy_level=discord.PrivacyLevel.guild_only,
                    location=event_location,
                    end_time=event_end_time,
                    reason="New event created via external Redis call")
                print(f'Successfully created event: {event_name}')
                return created_event.id
            print(f'Event in the past!')
            return None
    except discord.HTTPException as e:
        print(f"Discord API Error during event operation: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during event operation: {e}")
        return None