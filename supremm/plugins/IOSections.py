#!/usr/bin/env python
from __future__ import print_function

import datetime
import time
import numpy

from supremm.plugin import Plugin
from supremm.errors import ProcessingError


class IOSections(Plugin):

    name = property(lambda x: "iosections")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(IOSections, self).__init__(job)
        self.starttime = (job.start_datetime - datetime.datetime(1970, 1, 1)).total_seconds()
        self.endtime = (job.end_datetime - datetime.datetime(1970, 1, 1)).total_seconds()
        self.quarter = (self.endtime - self.starttime) / 4
        self.current_marker = self.starttime + self.quarter

        self.section_start_data = None
        self.section_start_timestamp = self.starttime
        self.quarter_avgs = []
        self.last_value = []


    def process(self, nodemeta, timestamp, data, description):
        mountpoint_sums = [numpy.sum(x) for x in data]
        self.last_value = mountpoint_sums
        if self.section_start_data is None:
            self.section_start_data = mountpoint_sums


        if timestamp >= self.current_marker:
            avg_read = (mountpoint_sums[0] - self.section_start_data[0]) / (timestamp - self.section_start_timestamp)
            avg_write = (mountpoint_sums[1] - self.section_start_data[1]) / (timestamp - self.section_start_timestamp)

            self.quarter_avgs.append((avg_read, avg_write))
            self.current_marker += self.quarter
            self.section_start_data = mountpoint_sums
            self.section_start_timestamp = timestamp

        return True

    def results(self):
        avg_read = (self.last_value[0] - self.section_start_data[0]) / (self.endtime - self.section_start_timestamp)
        avg_write = (self.last_value[1] - self.section_start_data[1]) / (self.endtime - self.section_start_timestamp)
        
        self.quarter_avgs.append((avg_read, avg_write))
        if len(self.quarter_avgs) != 4:
            return {"error": ProcessingError.INSUFFICIENT_DATA}
        return { 
                "1": self.quarter_avgs[0],
                "2": self.quarter_avgs[1],
                "3": self.quarter_avgs[2],
                "4": self.quarter_avgs[3]
                }
