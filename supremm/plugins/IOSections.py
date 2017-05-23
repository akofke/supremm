#!/usr/bin/env python

from __future__ import print_function
from supremm.plugin import Plugin

class IOSections(Plugin):
    
    name = property(lambda x: "iosections")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"]) 
  
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(IOSections, self).__init__(job)
        self.starttime = job.start_datetime
        self.endtime = job.end_datetime
        print("hello")
                
    def process(self, nodemeta, timestamp, data, description):
        print(timestamp)
        print(data)

    def results(self):
        return { "test": "test" }
 
