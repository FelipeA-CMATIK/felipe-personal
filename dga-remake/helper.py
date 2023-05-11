from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os


class Helper:
    def resolve_ids_for_query(self, ids):
        if len(ids) == 1:
            return f"({ids[0]})"
        else:
            return tuple(ids)

    def fix_timezone(self, date):
        return date.astimezone(ZoneInfo(os.environ.get('TIMEZONE')))
        # redep

    def pluck(self, lst, key):
        return [x.get(key) for x in lst]

    def unique(self, list_to_unique):
        unique_list = []
        for x in list_to_unique:
            if x not in unique_list:
                unique_list.append(x)
        return unique_list

    def date_range(self, start_date, end_date):
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        delta = end - start  # as timedelta
        days = [datetime.strftime(start + timedelta(days=i), '%Y-%m-%d') for i in range(delta.days + 1)]
        return days
