# coding: utf-8

import datetime
from dateutil import relativedelta

class Period:
    def __init__(self, start_datetime, end_datetime):
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        
        self.start_day = start_datetime.day
        self.start_month = start_datetime.month
        self.start_year = start_datetime.year

        self.end_day = end_datetime.day
        self.end_month = end_datetime.month
        self.end_year = end_datetime.year
        
    def split_by_year(self):
        splits = []

        for year in range(self.start_year, self.end_year + 1):
            first_day = datetime.datetime.strptime('{}/{}/{}'.format(self.start_day, self.start_month, year), '%d/%m/%Y')
            splits.append(first_day)

        end = datetime.datetime(self.end_year, self.end_month, self.end_day, 0, 0, 0)
        if end > splits[-1]:
            splits.append(end)

        return self.build_intervals(splits)
        
    def split_by_month(self):
        splits = []
        start = self.start_datetime
        while start < self.end_datetime: 
            splits.append(start) 
            start = start + relativedelta.relativedelta(months=+1)

        if self.end_datetime > splits[-1]:
            splits.append(self.end_datetime)
        
        return self.build_intervals(splits)
        
    def split_by_week(self):
        return self.split_by_day_ammount(7)

    def split_by_day_ammount(self, ammount):
        diff = self.end_datetime - self.start_datetime

        splits = []
        for i in range((diff.days//ammount) + 1):
            splits.append(self.start_datetime + datetime.timedelta(days=i*ammount))
        
        end = datetime.datetime(self.end_year, self.end_month, self.end_day, 0, 0, 0)
        if end > splits[-1]:
            splits.append(end)

        return self.build_intervals(splits)
        
    def split_by_day(self):
        return self.split_by_day_ammount(1)
        
    def build_intervals(self, splits):
        splitted_intervals = []
        for i in range(len(splits)-1):
            splitted_intervals.append((splits[i], splits[i+1] - datetime.timedelta(seconds=1)))
        return splitted_intervals