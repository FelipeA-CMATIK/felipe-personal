from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import operator


class Frequency:
    def __init__(self, frequency, date_format='%Y-%m-%d', hour_format='%H:%M:%S'):
        self.frequency = frequency
        self.date_format = date_format
        self.hour_format = hour_format
        self.now = self.fix_timezone(datetime.now())

    def switch_frequencies(self):
        frequencies = {
            "today": self.calculate_dates_from_days(start_days=0, end_days=0),
            "yesterday": self.calculate_dates_from_days(start_days=1, end_days=1),
            "last-7-days": self.calculate_dates_from_days(start_days=0, end_days=7),
            "last-15-days": self.calculate_dates_from_days(start_days=0, end_days=15),
            "last-30-days": self.calculate_dates_from_days(start_days=0, end_days=30),
            "last-180-days": self.calculate_dates_from_days(start_days=0, end_days=180),
            "this-week": self.calculate_dates_from_weeks(weeks=0),
            "past-week": self.calculate_dates_from_weeks(weeks=1),
            "this-month": self.calculate_month(),
            "last-month": self.calculate_last_month(),
            "this-year": self.calculate_year(),
            "last-year": self.calculate_year(1)
        }
        if 'name' in self.frequency and self.frequency['name'] in frequencies:
            return frequencies[self.frequency['name']]
        else:
            return frequencies['today']

    def start_of_day_format(self):
        return self.date_format + " " + "00:00:00"

    def end_of_day_format(self):
        return self.date_format + " " + "23:59:50"

    # years
    def calculate_year(self, year_sub=0, date=None):
        if date is None:
            date = self.now

        return {
            "start_date": date.replace(
                year=date.year - year_sub,
                month=1,
                day=1
            ).strftime(self.start_of_day_format()),
            "end_date": date.replace(
                year=date.year - year_sub,
                month=12,
                day=31
            ).strftime(self.end_of_day_format()),
        }

    # months
    def calculate_month(self, date=None):
        if date is None:
            date = self.now

        return {
            "start_date": date.replace(
                day=1
            ).strftime(self.start_of_day_format()),
            "end_date": (
                (date.replace(day=1) + timedelta(days=32)).replace(day=1)
                -
                timedelta(days=1)
            ).strftime(self.end_of_day_format())
        }

    def calculate_last_month(self):
        return self.calculate_month(
              (datetime.now().replace(day=1)-timedelta(days=3))
        )

    # weeks
    def calculate_dates_from_weeks(self, weeks, date=None):
        if date is None:
            date = self.now

        date = date - timedelta(days=weeks*7)
        start = date - timedelta(days=date.weekday())
        return {
            "start_date": start.strftime(self.start_of_day_format()),
            "end_date": (start + timedelta(days=6)).strftime(self.end_of_day_format())
        }

    # days
    def calculate_dates_from_days(self, start_days, end_days, date=None):
        if date is None:
            date = self.now

        return {
            "end_date": self.sub_days(sub_days=start_days, date=date).strftime(self.end_of_day_format()),
            "start_date": self.sub_days(sub_days=end_days, date=date).strftime(self.start_of_day_format())
        }

    def sub_days(self, sub_days, date=None):
        if date is None:
            date = self.now

        return date - timedelta(days=sub_days)

    # helpers
    @staticmethod
    def fix_timezone(date):
        return date.astimezone(ZoneInfo('America/Santiago'))

    # date comparison
    @staticmethod
    def diff_in_days(start_date, end_date):
        return (end_date - start_date).days

    def hour_comparison(self, start_date, end_date):
        return self.date_comparison(start_date, end_date, '%H')

    def day_comparison(self,start_date, end_date):
        return self.date_comparison(start_date, end_date, '%d')

    def month_comparison(self,start_date, end_date):
        return self.date_comparison(start_date, end_date, '%m')

    def year_comparison(self,start_date, end_date):
        return self.date_comparison(start_date, end_date, '%Y')

    @staticmethod
    def date_comparison(start_date, end_date, format):
        return int(end_date.strftime(format)) - int(start_date.strftime(format))

    @staticmethod
    def check_condition(inp, relate, cut):
        if relate == 0:
            return True

        ops = {
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '==': operator.eq
        }

        return ops[relate](inp, cut)

    def check_frequency(self, sd, ed):
        configuration = self.frequency['configuration']
        diff_in_hour = self.hour_comparison(sd, ed)
        diff_in_day = self.day_comparison(sd, ed)
        diff_in_month = self.month_comparison(sd, ed)
        diff_in_year = self.year_comparison(sd, ed)

        if not self.check_condition(diff_in_year, configuration['year'][1], configuration['year'][0]):
            return False
        else:
            if configuration['year'][2] and diff_in_year > 0:
                return True

        if not self.check_condition(diff_in_month, configuration['month'][1], configuration['month'][0]):
            return False
        else:
            if configuration['month'][2] and diff_in_month > 0:
                return True

        if not self.check_condition(diff_in_day, configuration['day'][1], configuration['day'][0]) and configuration['day'][2] != 'diff':
            return False
        else:
            if configuration['day'][2] is True and diff_in_day > 0:
                return True
            else:
                if configuration['day'][2] == 'diff' and self.check_condition(self.diff_in_days(sd, ed), configuration['day'][1],configuration['day'][0]):
                    return True

        if not self.check_condition(diff_in_hour, configuration['hour'][1], configuration['hour'][0]):
            return False

        return True
