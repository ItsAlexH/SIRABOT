from gcsa.google_calendar import GoogleCalendar
import os
calendar = GoogleCalendar(os.getenv("SIFP_CALENDAR_ID"), credentials_path=r'credentials.json', open_browser=False, authentication_flow_port=0)