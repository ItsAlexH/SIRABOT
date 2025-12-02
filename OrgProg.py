import time
import numpy as np
import pandas as pd
import datetime as datetime
from datetime import date, timedelta
import gspread
from gspread.exceptions import APIError
import datetime
from dotenv import load_dotenv
# from OrgParse import conversion_excel_date, parse_times, get_color, post_events
import os
import re  # ADDED: Import the re module to fix NameError

def prog_weeks(Weeks_arr):
    ii_w = []
    i0 = 0
    i1 = 0
    for i in range(0, len(Weeks_arr)):
        
        if(i == 0):
            i0 = i
        elif(Weeks_arr[i] != '' and not (isinstance(Weeks_arr[i],int))):
            # print(f'Weeks_arr[i] = {Weeks_arr[i]}')
            ii_w.append([i0, i1])
            i0 = i
        else:
            i1 = i
    if i0 <= i1:
        ii_w.append([i0, i1])

    return ii_w

def sog_days(Dates_arr_SOG):
    ii_d_SOG = []
    i0 = 0
    i1 = 0
    for j in range(0, len(Dates_arr_SOG)):
        if (j == 0):
            i0 = j
        elif (Dates_arr_SOG[j] != ''):
            if(j == i0+1 and Dates_arr_SOG[i0] != ''):
                ii_d_SOG.append([i0, i0])
            elif (Dates_arr_SOG[j] == 'Ongoing Challenges'):
                ii_d_SOG.append([i0, i1-1])
            else:
                ii_d_SOG.append([i0, i1])
            i0 = j
        else:
            i1 = j
    if i0 <= i1:
            ii_d_SOG.append([i0, i1])
    return ii_d_SOG

def get_programming(cal_data, ii):
    Date_arr = cal_data["Date"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Start_arr = cal_data["Start Time"][ii[0]:ii[1] + 1].reset_index(drop=True)
    End_arr = cal_data["End Time"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Host_arr = cal_data["Host"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Name_arr = cal_data["Name"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Description_arr = cal_data["Description"][ii[0]:ii[1] + 1].reset_index(drop=True)
    HALPS_arr = cal_data["HALPS Category"][ii[0]:ii[1] + 1].reset_index(drop=True)
    Location_arr = cal_data["Location"][ii[0]:ii[1] + 1].reset_index(drop=True)
    return Date_arr, Start_arr, End_arr, Host_arr, Name_arr, Description_arr, HALPS_arr, Location_arr

def clean_headers(raw_headers_list, prefix="Unnamed"):
    """
    Cleans a list of headers by:
    1. Stripping whitespace.
    2. Replacing empty strings with a unique placeholder (e.g., 'Unnamed 0').
    3. Handling duplicate names by appending a counter (e.g., 'Date_1').
    """
    cleaned = []
    seen_headers = {}
    for i, h in enumerate(raw_headers_list):
        # Convert non-string headers to string for consistency if needed,
        # but for column names, they usually come in as strings or numbers that should be strings.
        header_str = str(h).strip()

        if not header_str: # If header is empty after stripping
            header_str = f"{prefix}_{i}" # Use a unique unnamed placeholder

        original_header_str = header_str
        count = seen_headers.get(original_header_str, 0)
        if count > 0:
            header_str = f"{original_header_str}_{count}"
        seen_headers[original_header_str] = count + 1 # Increment for the next potential duplicate

        cleaned.append(header_str)
    return cleaned