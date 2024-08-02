from datetime import timedelta
import datetime

dt = datetime.datetime.today()
dttemp = datetime.datetime.strftime(dt , '%Y-%m-%d 00:00')

print(dttemp)