#!/usr/bin/env python
from __future__ import print_function

import datetime
import time
import numpy

from supremm.plugin import Plugin


class IOSections(Plugin):

    name = property(lambda x: "iosections")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(IOSections, self).__init__(job)
        self.starttime = time.mktime(job.start_datetime.timetuple())
        self.endtime = time.mktime(job.end_datetime.timetuple())
        self.quarter = (self.endtime - self.starttime) / 4
        self.current_marker = self.starttime + self.quarter

        self.section_start_data = 0
        self.section_start_timestamp = self.starttime
        self.quarter_avgs = []
        self.last_value = 0


    def process(self, nodemeta, timestamp, data, description):
        mountpoint_sum = numpy.sum(data[0])
        self.last_value = mountpoint_sum

        if timestamp >= self.current_marker:
            avg = (mountpoint_sum - self.section_start_data) / (timestamp - self.section_start_timestamp)
            self.quarter_avgs.append(avg)
            self.current_marker += self.quarter
            self.section_start_data = mountpoint_sum
            self.section_start_timestamp = timestamp

        return True

    def results(self):
        self.quarter_avgs.append((self.last_value - self.section_start_data) / (self.endtime - self.section_start_timestamp))
        return { 
                "first_quarter": self.quarter_avgs[0],
                "middle_half": (self.quarter_avgs[1] + self.quarter_avgs[2]) / 2,
                "last_quarter": self.quarter_avgs[3]
                }
