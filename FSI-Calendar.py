# Initial Imports
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event
from gcsa.recurrence import Recurrence, DAILY, SU, SA

from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import GridRangeType
from gcsa.calendar import Calendar

import time
import numpy as np
import pandas as pd
import datetime as datetime
from datetime import date, timedelta
import gspread
from beautiful_date import Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sept, Oct, Nov, Dec
from BotScript import client, update_or_create_discord_event, eastern, bot_ready_event
import asyncio
import datetime
import os
from dotenv import load_dotenv
import sys

from OrgParse import conversion_excel_date, parse_times, get_color, clear_dates, post_events

Missing_color = 1

# Load Necessary Files & Connect to GCal
calendar = GoogleCalendar(os.getenv("CALENDARID"), credentials_path=r'credentials.json')
gc = gspread.service_account(filename='service_account.json')
wks = gc.open(os.getenv("WKSSOGTOKEN"))

async def main():
    # Week to be printed given via args
    if len(sys.argv) > 1:
        try:
            n = int(sys.argv[1])
        except ValueError:
            print("Invalid argument for 'n'. Please provide an integer.")
            sys.exit(1) 
    else:
        raise ValueError("Error: No 'n' value provided as a command-line argument. 'n' is required.")
        
    print(f'Printing events for Week #{n}...')

    cal_data = pd.DataFrame(wks.get_worksheet(n+1).get_all_values(value_render_option='UNFORMATTED_VALUE'))[2:][:]
    headers = cal_data.iloc[0].values
    cal_data.columns = headers
    cal_data = cal_data[1:]

    Dates = cal_data['Date'].tolist()
    Titles = cal_data['Workshop Title'].tolist()

    ### Datetime objects are in Excel format & need to be converted.
    for j in range(0, len(Dates)):
        if isinstance(Dates[j], (int, float)):
            Dates[j] = conversion_excel_date(Dates[j])

    ### Determine ranges for specific dates from the SOG (as it goes Date .... Date.... Date....)
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
    Colors = get_color(Categories)
    
    Descriptions_mask = [val == '' for val in Descriptions]
    Locations_mask = [val == '' for val in Locations]
    Leaders_mask = [val == '' for val in Leaders]

    Initial_date, Final_date = clear_dates(Dates)

    print(f"Clearing Google Calendar events from {Initial_date.date()} to {Final_date.date()}...")
    events_to_delete = calendar.get_events(Initial_date, Final_date, order_by='updated')
    for event in events_to_delete:
        calendar.delete_event(event)
    print("Old events cleared.")

    ## Print Events
    await post_events(calendar, p = (Titles, Leaders, Leaders_mask, Dates, 
                                     Start_Times, End_Times, Locations, Locations_mask, Descriptions, Descriptions_mask, Categories, Colors))
    print('Printing completed.')
    # print('Printing completed. Sleeping for 60 minutes')
    # time.sleep(60 * 60)


if __name__ == "__main__":
    async def run_all():
        asyncio.create_task(client.start(os.getenv("DISCORD_BOT_TOKEN")))
        await bot_ready_event.wait()
        print("Discord bot has successfully connected and is ready.")
        await main()

    asyncio.run(run_all())