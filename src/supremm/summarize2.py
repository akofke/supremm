import time
import datetime

from supremm.rangechange import RangeChange
from supremm.pcp_logmerge.pcp_logmerge import MergedArchives


def datetime_to_timestamp(dt):
    return (dt - datetime.datetime.utcfromtimestamp(0)).total_seconds()


class Summarize(object):
    def __init__(self, preprocessors, analytics, job, config, fail_fast=False):

        self.preprocs = preprocessors
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.errors = {}
        self.job = job
        self.start = time.time()
        self.archives_processed = 0
        self.fail_fast = fail_fast

        self.rangechange = RangeChange(config)

    def adderror(self, category, errormsg):
        """ All errors reported with this function show up in the job summary """
        if category not in self.errors:
            self.errors[category] = set()
        if isinstance(errormsg, list):
            self.errors[category].update(set(errormsg))
        else:
            self.errors[category].add(errormsg)

    def process(self):
        success = 0
        self.archives_processed = 0

        # TODO fix methods on job class
        for node_name, job_node in self.job._nodes.iteritems():
            # process archive
            node_id = job_node.nodeindex
            node_archives = job_node.rawarchives
            pass

    def process_node(self, node_name, node_idx, archives):
        start_archive_idx, _ = self.job.get_start_archive(node_name)
        start_ts = datetime_to_timestamp(self.job.start_datetime) if start_archive_idx is None else None
        _, end_archive_name = self.job.get_end_archive(node_name)
        end_ts = datetime_to_timestamp(self.job.end_datetime) if end_archive_name is None else None
        merged_archives = MergedArchives(archives, start_archive_idx, start_ts, end_archive_name, end_ts)
        # check error codes

        pass

    def process_preprocs(self, node_name, node_idx, merged_archives):
        pass
