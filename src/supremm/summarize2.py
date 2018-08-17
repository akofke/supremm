import logging
import time
import datetime
import itertools

import numpy as np

from supremm.plugin import NodeMetadata
from supremm.rangechange import RangeChange
from supremm.pcp_logmerge.pcp_logmerge import MergedArchives


VERSION = "1.0.6"
TIMESERIES_VERSION = 4

EMPTY_DOUBLE_ARRAY = np.empty(0, dtype=np.float64)
EMPTY_I32_ARRAY = np.empty(0, dtype=np.int32)
EMPTY_OBJ_ARRAY = np.empty(0, dtype=object)

def datetime_to_timestamp(dt):
    return (dt - datetime.datetime.utcfromtimestamp(0)).total_seconds()


class NodeMeta(NodeMetadata):
    """ container for achive metadata """
    def __init__(self, nodename, nodeidx):
        self._nodename = nodename
        self._nodeidx = nodeidx

    nodename = property(lambda self: self._nodename)
    nodeindex = property(lambda self: self._nodeidx)


class Summarize(object):
    def __init__(self, preprocessors, analytics, job, config, fail_fast=False):

        self.preprocs = preprocessors
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.errors = {}
        self.job = job
        self.start = time.time()
        self.nodes_processed = 0
        self.fail_fast = fail_fast
        self._good_enough = True

        try:
            self.rangechange_config = config.getsection("normalization")
        except KeyError:
            self.rangechange_config = None

    def adderror(self, category, errormsg):
        """ All errors reported with this function show up in the job summary """
        if category not in self.errors:
            self.errors[category] = set()
        if isinstance(errormsg, list):
            self.errors[category].update(set(errormsg))
        else:
            self.errors[category].add(errormsg)

    def complete(self):
        """ A job is complete if archives exist for all assigned nodes and they have
            been processed successfully
        """
        return self.job.nodecount == self.nodes_processed

    def good_enough(self):
        """ A job is good_enough if archives for 95% of nodes have
            been processed sucessfullly
        """
        return self._good_enough

    def get(self):
        """ Return a dict with the summary information """
        output = {}
        timeseries = {}

        je = self.job.get_errors()
        if len(je) > 0:
            self.adderror("job", je)

        if self.job.nodecount > 0:
            for analytic in self.alltimestamps:
                if analytic.status != "uninitialized":
                    if analytic.mode == "all":
                        output[analytic.name] = analytic.results()
                    if analytic.mode == "timeseries":
                        timeseries[analytic.name] = analytic.results()
            for analytic in self.firstlast:
                if analytic.status != "uninitialized":
                    output[analytic.name] = analytic.results()

        output['summarization'] = {
            "version": VERSION,
            "elapsed": time.time() - self.start,
            "created": time.time(),
            "srcdir": self.job.jobdir,
            "complete": self.complete()}

        output['created'] = datetime.datetime.utcnow()

        output['acct'] = self.job.acct
        output['acct']['id'] = self.job.job_id

        if len(timeseries) > 0:
            timeseries['hosts'] = dict((str(nodedata.nodeindex), name) for name, nodedata in self.job._nodes.iteritems())
            timeseries['version'] = TIMESERIES_VERSION
            output['timeseries'] = timeseries

        for preproc in self.preprocs:
            result = preproc.results()
            if result is not None:
                output.update(result)

        for source, data in self.job.data().iteritems():
            if 'errors' in data:
                self.adderror(source, str(data['errors']))

        if len(self.errors) > 0:
            output['errors'] = {}
            for k, v in self.errors.iteritems():
                output['errors'][k] = list(v)

        return output

    def process(self):
        logging.info("Processing job %s", self.job)
        success = True
        self.nodes_processed = 0
        nodes_skipped = 0

        # TODO fix methods on job class
        for node_name, job_node in self.job._nodes.iteritems():
            # process archive
            node_id = job_node.nodeindex
            node_archives = job_node.rawarchives
            result = self.process_node(node_name, node_id, node_archives)
            if result:
                self.nodes_processed += 1
            else:
                nodes_skipped += 1
                success = False
                logging.debug("Skipping node %s for job %s", node_name, self.job)
            if nodes_skipped > 0.05 * self.job.nodecount:
                logging.debug(
                    "%s out of %s nodes skipped for job %s, aborting job",
                    nodes_skipped, self.job.nodecount, self.job
                )
                self._good_enough = False
                return False

        return success

    def process_node(self, node_name, node_idx, archives):
        logging.debug("Processing node %s (%s archives)", node_name, len(archives))
        merged_archives = MergedArchives(
            archives,
            self.job.get_start_archive(node_name),
            datetime_to_timestamp(self.job.start_datetime),
            self.job.get_end_archive(node_name),
            datetime_to_timestamp(self.job.end_datetime)
        )

        if np.all(merged_archives.get_status_codes() != 0):
            # None of the archives could be opened, indicate failure for this node
            return False

        self.process_preprocs(node_name, node_idx, merged_archives)
        merged_archives.clear_metrics_and_reset()

        self.process_analytics(node_name, node_idx, merged_archives)
        merged_archives.clear_metrics_and_reset()

        self.process_firstlast(node_name, node_idx, merged_archives)

        return True

    def process_preprocs(self, node_name, node_idx, merged_archives):
        # list of tuples of the plugin instance with its metric list that was chosen.
        preprocs_used = []
        for preproc in self.preprocs:
            preproc.hoststart(node_name)
            metrics = register_plugin(preproc, merged_archives)
            if metrics:
                preprocs_used.append((preproc, metrics))
            else:
                logging.debug(
                    "Skipping %s (%s) for node %s, required metrics not found",
                    type(preproc).__name__, preproc.name, node_name
                )
                preproc.hostend()

        if not preprocs_used:
            # TODO: No preprocessors successfully registered, skip or complain?
            pass

        preproc_status = {p[0]: False for p in preprocs_used}  # "done" status for each preproc

        for timestamp, metrics in merged_archives.iter_data():
            process_entry_preprocs(preprocs_used, preproc_status, timestamp, metrics)
            if all(preproc_status.itervalues()):
                break

        for preproc, _ in preprocs_used:
            preproc.status = "complete"
            preproc.hostend()

    def process_analytics(self, node_name, node_idx, merged_archives):
        # TODO: RangeChange - modify to use dict
        node_meta = NodeMeta(node_name, node_idx)
        plugins_used = []
        for plugin in self.alltimestamps:
            metrics = register_plugin(plugin, merged_archives)
            if metrics:
                plugins_used.append((plugin, metrics))
            else:
                logging.debug(
                    "Skipping %s (%s) for node %s, required metrics not found",
                    type(plugin).__name__, plugin.name, node_name
                )

        plugin_status = {p[0]: False for p in plugins_used}  # "done" status for each plugin
        for timestamp, metrics in merged_archives.iter_data():
            process_entry_plugins(plugins_used, plugin_status, node_meta, timestamp, metrics)
            if all(plugin_status.itervalues()):
                break

        for plugin, _ in plugins_used:
            plugin.status = "complete"

    def process_firstlast(self, node_name, node_idx, merged_archives):
        node_meta = NodeMeta(node_name, node_idx)
        plugins_used = []
        for plugin in self.firstlast:
            metrics = register_plugin(plugin, merged_archives, firstlast=True)
            if metrics:
                plugins_used.append((plugin, metrics))
            else:
                logging.debug(
                    "Skipping %s (%s) for node %s, required metrics not found",
                    type(plugin).__name__, plugin.name, node_name
                )

        first_entry, last_entry = merged_archives.get_firstlast(self.rangechange_config)

        for plugin, met_names in plugins_used:
            first_data = []
            last_data = []
            first_desc = []
            last_desc = []
            has_first = False
            has_last = False
            ts_f = None
            ts_l = None
            for met_name in met_names:
                met_f = first_entry.get(met_name)
                met_l = last_entry.get(met_name)
                # TODO: only if both exist?
                if met_f is not None and met_l is not None:
                    has_first = True
                    ts_f, vals_f, codes_f, names_f = met_f
                    first_data.append(vals_f)
                    first_desc.append((codes_f, names_f.tolist()))
                else:
                    first_data.append(EMPTY_DOUBLE_ARRAY)
                    first_desc.append((EMPTY_I32_ARRAY, []))

                if met_l is not None:
                    has_last = True
                    ts_l, vals_l, codes_l, names_l = met_l
                    last_data.append(vals_l)
                    last_desc.append((codes_l, names_l.tolist()))
                else:
                    last_data.append(EMPTY_DOUBLE_ARRAY)
                    last_data.append((EMPTY_I32_ARRAY, []))

            if ts_f is not None and ts_l is not None and ts_f == ts_l:
                continue  # TODO: log debug

            if has_first:
                plugin.process(node_meta, ts_f, first_data, first_desc)

            if has_last:
                plugin.process(node_meta, ts_l, last_data, last_desc)

            plugin.status = "complete"


def process_entry_preprocs(preprocs, preproc_status, timestamp, metrics):
    for preproc, required_metrics in preprocs:
        if preproc_status[preproc]:
            continue

        data = []
        inst_ids = []
        description = []
        has_some_data = False
        for req in required_metrics:
            met = metrics.get(req)
            if met is not None:
                has_some_data = True
                vals, inst_codes, inst_names = met

                data.append(vals)
                inst_ids.append(inst_codes)
                description.append(inst_names)

                # # TODO: check status for individual instances and use a placeholder empty array?
                #
                # # TODO: change the api and do something more sane here
                # # This keeps compatibility with the current format of passing (inst value, inst id) pairs
                # # column_stack creates an array like
                # # array([['value1', 1],
                # #        ['value2', 2],
                # #        ['value3', 3]], dtype=object)
                # stacked_data = np.column_stack((vals, inst_codes))
                # data.append(stacked_data)
                #
                # # SlurmProc has a performance issue when the keys of the description dict are numpy int32 objects
                # # instead of python integers, causing slow lookups. Since the column stack already converts the
                # # instance codes to python ints, grab those here for the dict keys.
                # description.append({stacked_data[i, 1]: inst_names[i] for i in xrange(len(inst_names)) if inst_names[i] != ""})
            else:
                # Metric is not present at this timestamp, use a placeholder
                data.append(EMPTY_DOUBLE_ARRAY)
                inst_ids.append(EMPTY_I32_ARRAY)
                description.append(EMPTY_OBJ_ARRAY)

        if has_some_data:
            # Set "done" to True iff the preproc returns False
            # preproc_status[preproc] = preproc.process(timestamp, np.array(data), description) is False
            preproc_status[preproc] = preproc.process(timestamp, np.array(data), np.array(inst_ids), np.array(description)) is False


def process_entry_plugins(plugins, plugin_status, node_meta, timestamp, metrics):
    for plugin, met_names in plugins:
        if plugin_status[plugin]:
            # plugin status is "done"
            continue

        data = []
        description = []
        has_some_data = False
        for met_name in met_names:
            met = metrics.get(met_name)
            if met is not None:
                has_some_data = True
                vals, inst_codes, inst_names = met
                data.append(vals)
                # TODO: !! tolist() very bad, but some plugins expect a python list for now
                description.append((inst_codes, inst_names.tolist()))
            else:
                # Keeps puffypcp behavior
                data.append(EMPTY_DOUBLE_ARRAY)
                description.append((EMPTY_I32_ARRAY, []))

        if has_some_data:
            # Set "done" to true iff the plugin returns False
            plugin_status[plugin] = plugin.process(node_meta, timestamp, data, description) is False


def register_plugin(plugin, merged_archives, firstlast=False):
    """
    Attemps to register the plugin's required and optional metrics with the merged archive.
    Returns the list of metrics ultimately added if it was successful (this includes optional metrics),
    or an empty list if none of the plugin's alternatives could be added.
    """
    metrics = []
    # First condition checks if requiredMetrics is empty. Shouldn't happen but
    # otherwise is an IndexError
    if plugin.requiredMetrics and isinstance(plugin.requiredMetrics[0], list):
        for alternative in plugin.requiredMetrics:
            success = merged_archives.add_metrics_required(alternative, plugin=plugin if firstlast else None)
            if success:
                metrics.extend(alternative)
                break
    else:
        success = merged_archives.add_metrics_required(plugin.requiredMetrics, plugin=plugin if firstlast else None)
        if success:
            metrics.extend(plugin.requiredMetrics)

    if plugin.optionalMetrics:
        added = merged_archives.add_metrics_optional(plugin.optionalMetrics)
        metrics.extend(added)

    return metrics




