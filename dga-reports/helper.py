from datetime import datetime
from zoneinfo import ZoneInfo
import os
import uuid


class Helper:
    def resolve_ids_for_query(self, ids):
        if len(ids) == 1:
            return f"({ids[0]})"
        else:
            return tuple(ids)     

    def pluck(self, lst, key):
        return [x.get(key) for x in lst] 

    def unique(self, list_to_unique):
        unique_list = []
        for x in list_to_unique:
            if x not in unique_list:
                unique_list.append(x)  
        return unique_list                 

    def fix_timezone(self, date):
        return date.astimezone(ZoneInfo(os.environ.get('TIMEZONE'))) 

    def print(self, message, level):
        if level <= int(os.environ.get('LOG_LEVEL',0)) and int(os.environ.get('LOG_LEVEL',0)) != 0:
            print(message)

    def generate_uuid(self, keyword):
        return uuid.uuid3(uuid.NAMESPACE_DNS, keyword)
