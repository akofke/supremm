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

        self.nodes = {}


    def process(self, nodemeta, timestamp, data, description):
        n = nodemeta.nodename
        if n not in self.nodes:
            self.nodes[n] = {
                        "current_marker": self.starttime + self.quarter,
                        "section_start_data": None,
                        "section_start_timestamp": self.starttime,
                        "quarter_avgs": [],
                        "last_value": []
                    }

        node_data = self.nodes[n]

        mountpoint_sums = [numpy.sum(x) for x in data]
        node_data["last_value"] = mountpoint_sums
        if node_data["section_start_data"] is None:
            node_data["section_start_data"] = mountpoint_sums


        if timestamp >= node_data["current_marker"]:
            avg_read = (mountpoint_sums[0] - node_data["section_start_data"][0]) / (timestamp - node_data["section_start_timestamp"])
            avg_write = (mountpoint_sums[1] - node_data["section_start_data"][1]) / (timestamp - node_data["section_start_timestamp"])

            node_data["quarter_avgs"].append((avg_read, avg_write))
            node_data["current_marker"] += self.quarter
            node_data["section_start_data"] = mountpoint_sums
            node_data["section_start_timestamp"] = timestamp

        return True

    def results(self):

        for node, data in self.nodes.iteritems():
            avg_read = (data["last_value"][0] - data["section_start_data"][0]) / (self.endtime - data["section_start_timestamp"])
            avg_write = (data["last_value"][1] - data["section_start_data"][1]) / (self.endtime - data["section_start_timestamp"])

            data["quarter_avgs"].append((avg_read, avg_write))

            if len(data["quarter_avgs"]) != 4:
                return {"error": ProcessingError.INSUFFICIENT_DATA}

            data = {k: v for k, v in data.iteritems() if k == "quarter_avgs"}
            self.nodes[node] = data

        return self.nodes
    # {
    #             "1": self.quarter_avgs[0],
    #             "2": self.quarter_avgs[1],
    #             "3": self.quarter_avgs[2],
    #             "4": self.quarter_avgs[3]
    #             }
