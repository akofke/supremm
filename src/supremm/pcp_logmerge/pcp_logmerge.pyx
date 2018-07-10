# cython: profile=False
import math

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
cimport cython
from libc.stdlib cimport free
from libc.stdint cimport int32_t, uint32_t, int64_t, uint64_t
import heapq
import logging

import numpy as np
cimport numpy as np
np.import_array()

import cpmapi as c_pmapi
cimport cpcp

cdef np.ndarray EMPTY_I32_ARR = np.empty(0, dtype=np.int32)
cdef np.ndarray EMPTY_STR_ARR = np.empty(0, dtype=np.object_)


cdef class Pool:
    cdef list addresses

    def __cinit__(self):
        self.addresses = []

    def __dealloc__(self):
        cdef size_t addr
        for addr in self.addresses:
            if addr != <size_t>NULL:
                PyMem_Free(<void *>addr)

    cdef void *malloc(self, size_t size) except NULL:
        cdef void *p = PyMem_Malloc(size)
        if p == NULL:
            raise MemoryError("Error allocating {} bytes".format(size))
        self.addresses.append(<size_t>p)
        return p

    cdef void add(self, void* p) except *:
        if p == NULL:
            raise MemoryError("Invalid pointer")
        self.addresses.append(<size_t>p)


cdef class MergedArchives:
    cdef dict archives
    cdef np.ndarray status_codes
    cdef object start_archive
    cdef double start_ts
    cdef object end_archive
    cdef double end_ts
    cdef bint at_start

    def __cinit__(self, list archives, start_archive, start_timestamp, end_archive, end_timestamp):
        cdef tuple all_archives = tuple(ArchiveFetchGroup(path) for path in archives)
        cdef ArchiveFetchGroup fg
        self.status_codes = np.array(tuple(fg.creation_status for fg in all_archives))

        # map each successful archive path to its fetchgroup
        self.archives = {fg.archive_path: fg for fg in all_archives if fg.creation_status == 0}

        self.start_archive = start_archive if start_archive in self.archives else None
        self.start_ts = start_timestamp
        self.end_archive = end_archive if end_archive in self.archives else None
        cdef ArchiveFetchGroup end_fg
        cdef cpcp.timeval tv
        if self.end_archive is not None:
            end_fg = self.archives[self.end_archive]
            cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(end_fg.fg))
            cpcp.pmGetArchiveEnd(&tv)
            # TODO: this emulates buggy code from the old adjust_job_start_end. Fix later
            self.end_ts = math.ceil(cpcp.pmtimevalToReal(&tv))
        else:
            # This will already be per-second granularity
            self.end_ts = end_timestamp

        self.at_start = False

    cpdef np.ndarray get_status_codes(self):
        return self.status_codes

    cpdef add_metrics_required(self, list metrics):
        """
        Attempts to add the given list of metrics as a whole to the archive fetchgroups.
        Returns true if every metric was successfully added to at least one archive fetchgroup
        (i.e. that archive contains metadata for the metric). 
        """
        # TODO: possibly keep a set of known failed metrics in case of duplicates across plugins, avoiding pmapi calls

        # for each metric, success flag for each archive
        cdef np.ndarray metric_success = np.full((len(metrics), len(self.archives)), False)
        cdef int err

        cdef Py_ssize_t i_arch
        cdef Py_ssize_t i_met
        cdef ArchiveFetchGroup arch_fg
        for i_arch, arch_fg in enumerate(self.archives.itervalues()):
            for i_met, metric in enumerate(metrics):
                err = arch_fg.maybe_add_metric(metric)
                if err == 0:
                    metric_success[i_met, i_arch] = True

        success = np.all(np.any(metric_success, axis=1))
        cdef np.ndarray extend_errors
        if success:
            for arch_fg in self.archives.itervalues():
                extend_errors = arch_fg.finalize_pending_metrics()
                if np.any(extend_errors != 0):
                    logging.warning("Unexpected error extending fetchgroup for archive %s: %s", arch_fg.archive_path, extend_errors)
                    return False  # Note in this case we will have "orphan" Metrics (registered but will never be used)

        else:
            for arch_fg in self.archives.itervalues():
                arch_fg.abort_pending_metrics()

        return success

    cpdef add_metrics_optional(self, list metrics):
        cdef list successful_metrics = []
        cdef np.ndarray success = np.full(len(metrics), False)
        cdef ArchiveFetchGroup fg
        cdef int sts
        cdef int i
        for fg in self.archives.itervalues():
            for i, metric in enumerate(metrics):
                sts = fg.add_metric(metric)
                if sts == 0:
                    success[i] = True
        for i in range(len(success)):
            if success[i]:
                successful_metrics.append(metrics[i])

        return successful_metrics

    def clear_metrics_and_reset(self):
        cdef ArchiveFetchGroup fg
        for fg in self.archives.itervalues():
            fg.clear_metrics()
        self.reset_to_start()

    cpdef void reset_to_start(self):
        if self.at_start:
            return
        cdef ArchiveFetchGroup start_archive
        cdef cpcp.timeval start_ts
        if self.start_archive is not None:
            # If the start archive was given, grab the precise start time of that archive
            # and use it as the start time for all archives
            start_archive = self.archives[self.start_archive]
            # TODO: this emulates buggy behavior
            start_ts = start_archive.get_start_from_loglabel()
            cpcp.pmtimevalFromReal(math.floor(cpcp.pmtimevalToReal(&start_ts)), &start_ts)
        else:
            # otherwise fall back to the less precise job start time from the database (only second precision)
            cpcp.pmtimevalFromReal(self.start_ts, &start_ts)

        cdef Py_ssize_t i
        cdef ArchiveFetchGroup fg
        for fg in self.archives.itervalues():
            fg.set_start_ts(&start_ts)

        self.at_start = True


    def iter_data(self):
        self.reset_to_start()
        self.at_start = False

        cdef list archive_queue = []

        cdef Py_ssize_t i
        cdef ArchiveFetchGroup fg
        for fg in self.archives.itervalues():
            fg.fetch()
            heapq.heappush(archive_queue, (cpcp.pmtimevalToReal(&fg.timestamp), fg))

        cdef double timestamp
        cdef int fetch_err
        cdef dict metrics = {}
        cdef Metric metric
        while archive_queue:  # while there are archives in the queue
            timestamp, fg = heapq.heappop(archive_queue)  # get the fetchgroup with the lowest timestamp

            metrics.clear()  # Clear since all its data will become invalid on next fetch anyways
            for i in range(len(fg.metrics)):
                metric = fg.metrics[i]
                if metric.out_num > 0 and metric.out_status == 0:
                    # TODO: do something when *all* metrics have errors?
                    metric.check_cached_length()
                    metrics[fg.metric_names[i]] = metric.get_data()

            yield (timestamp, metrics)

            fetch_err = fg.fetch()
            if fetch_err >= 0 and cpcp.pmtimevalToReal(&fg.timestamp) <= self.end_ts:
                # If the fetch was successful and the next timestamp is not past the
                # job end timestamp, add it back to the queue. If the archive hits EOL
                # or its next data point is past the job end, don't add it back. Eventually
                # this will cause the queue to be empty and processing is finished
                heapq.heappush(archive_queue, (cpcp.pmtimevalToReal(&fg.timestamp), fg))
            elif fetch_err < 0 and fetch_err != cpcp.PM_ERR_EOL:
                # This shouldn't really happen for archive context
                logging.error("Unexpected severe fetch error for %s", fg.archive_path)


cdef class ArchiveFetchGroup:
    cdef archive_path
    cdef readonly int creation_status
    cdef cpcp.pmFG fg
    cdef dict indom_sizes
    cdef list metrics
    cdef list metric_names
    cdef cpcp.timeval timestamp
    cdef list pending_metrics

    # Map indom id to a dict of instance id -> instance name (as python string)
    cdef dict indom_names

    def __cinit__(self, archive_path):
        self.creation_status = cpcp.pmCreateFetchGroup(&self.fg, c_pmapi.PM_CONTEXT_ARCHIVE, archive_path)
        self.archive_path = archive_path
        self.indom_sizes = {}
        self.metrics = []
        self.metric_names = []
        self.pending_metrics = []
        self.indom_names = {cpcp.PM_INDOM_NULL: {}}

        if not self.creation_status < 0:
            cpcp.pmExtendFetchGroup_timestamp(self.fg, &self.timestamp)

    def __dealloc__(self):
        if self.fg != <cpcp.pmFG>NULL:
            cpcp.pmDestroyFetchGroup(self.fg)

    def __str__(self):
        return "FetchGroup(archive={}, status={}, num={}, ts={})".format(self.archive_path, self.creation_status, len(self.metrics), cpcp.pmtimevalToReal(&self.timestamp))

    cdef cpcp.timeval get_start_from_loglabel(self):
        # TODO: cache timeval
        cdef cpcp.pmLogLabel label
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))
        cpcp.pmGetArchiveLabel(&label)
        return label.ll_start

    cdef int set_start(self, double time):
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))
        cdef cpcp.timeval tv
        cpcp.pmtimevalFromReal(time, &tv)
        return cpcp.pmSetMode(c_pmapi.PM_MODE_FORW, &tv, 0)

    cdef int set_start_ts(self, cpcp.timeval *tv):
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))
        return cpcp.pmSetMode(c_pmapi.PM_MODE_FORW, tv, 0)

    cdef int fetch(self):
        return cpcp.pmFetchGroup(self.fg)

    cdef int get_indom_size(self, cpcp.pmInDom indom):
        cdef int num_instances
        cdef int *instlist
        cdef char **namelist
        cdef int i
        cdef dict instance_names
        if indom == cpcp.PM_INDOM_NULL:
            return 1
        if indom in self.indom_sizes:
            num_instances = self.indom_sizes[indom]
        else:
            num_instances = cpcp.pmGetInDomArchive(indom, &instlist, &namelist)
            if num_instances < 0:
                return num_instances  # no allocation happens if error
            elif num_instances >= 1:
                # no allocation if num_instances is less than one, but otherwise we need to free
                if indom not in self.indom_names:
                    instance_names = {}
                    for i in range(num_instances):
                        instance_names[instlist[i]] = namelist[i]  # copies char * to python str
                    self.indom_names[indom] = instance_names

                free(instlist)
                free(namelist)
            self.indom_sizes[indom] = num_instances

        return num_instances

    cdef int clear_metrics(self):
        # TODO: pmClearFetchGroup is not available in 3.12
        # pass
        cdef int sts = cpcp.pmClearFetchGroup(self.fg)
        cpcp.pmExtendFetchGroup_timestamp(self.fg, &self.timestamp)
        self.metrics = []
        self.metric_names = []
        return sts

    cdef int add_metric(self, metric_name):
        # Note about metric_name string: when cython implicitly converts a python string to a char *, the pointer
        # is only valid for the lifetime of the string object. We pass the char * to extendFG_indom, but the source
        # shows that they don't hold on to it, so it's safe to let the string go out of scope after this method.

        # maybe it's faster to check the current context first? pmUseContext doesn't appear to do that.
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))

        cdef cpcp.pmID pmid
        cdef char *name = metric_name
        cdef int name_sts = cpcp.pmLookupName(1, &name, &pmid)
        if name_sts < 0:
            return name_sts

        cdef cpcp.pmDesc desc
        cdef int desc_sts = cpcp.pmLookupDesc(pmid, &desc)
        if desc_sts < 0:
            return desc_sts

        cdef int num_instances = self.get_indom_size(desc.indom)
        if num_instances < 0:
            return num_instances

        cdef Metric metric = Metric.create(self.fg, name, num_instances, desc.type, desc.indom, self.indom_names.get(desc.indom, {}))
        if metric.creation_status < 0:
            return metric.creation_status  # error extending, return the error code and the Metric will go out of scope now
        else:
            self.metrics.append(metric)
            self.metric_names.append(metric_name)
            return 0

    cdef int maybe_add_metric(self, metric_name):
        """
        Add a metric in a "transaction" manner. Looks up the metric name and indom and adds the metric information
        to a pending list, but does not create the Metric struct. The most common errors (metric does not exist in
        the archive) will be caught and returned here, where the adding code can cancel the transaction with
        abort_pending_metrics if needed. If the transaction succeeds, call finalize_pending_metrics to complete
        adding the metrics to the fetchgroup.
        """
        # maybe it's faster to check the current context first? pmUseContext doesn't appear to do that.
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))

        cdef cpcp.pmID pmid
        cdef char *name = metric_name
        cdef int name_sts = cpcp.pmLookupName(1, &name, &pmid)
        if name_sts < 0:
            return name_sts

        cdef cpcp.pmDesc desc
        cdef int desc_sts = cpcp.pmLookupDesc(pmid, &desc)
        if desc_sts < 0:
            return desc_sts

        cdef int num_instances = self.get_indom_size(desc.indom)
        if num_instances < 0:
            return num_instances

        self.pending_metrics.append((metric_name, num_instances, desc.type, desc.indom))
        return 0

    cdef void abort_pending_metrics(self):
        self.pending_metrics = []

    cdef np.ndarray finalize_pending_metrics(self):
        cdef int num_instances, val_type
        cdef cpcp.pmInDom indom
        cdef Metric metric
        cdef np.ndarray errors = np.zeros(len(self.pending_metrics))
        cdef Py_ssize_t i
        for i, (metric_name, num_instances, val_type, indom) in enumerate(self.pending_metrics):
            metric = Metric.create(self.fg, metric_name, num_instances, val_type, indom, self.indom_names.get(indom, {}))
            errors[i] = metric.creation_status
            if metric.creation_status == 0:
                self.metrics.append(metric)
                self.metric_names.append(metric_name)
            else:
                pass # do something else?

        self.pending_metrics = []
        return errors


cdef np.ndarray NULL_INDOM_INST = np.full(1, -1, dtype=np.int32)

cdef class Metric:
    cdef int num_instances
    cdef int val_type
    cdef cpcp.pmInDom indom
    cdef Pool pool
    cdef int creation_status

    cdef int *out_inst_codes
    cdef char **out_inst_names
    cdef cpcp.pmAtomValue *out_values
    cdef int *out_statuses
    cdef unsigned int out_num
    cdef int out_status

    cdef unsigned int last_len
    cdef np.ndarray inst_codes_np
    cdef np.ndarray inst_names_np
    cdef object[::1] inst_names_view
    cdef np.ndarray values_np

    cdef dict inst_names

    def __str__(self):
        return "Metric(max_size={}, num={})".format(self.num_instances, self.out_num)

    cdef tuple get_data(self):
        return self.get_values(), self.get_inst_codes(), self.get_inst_names()


    @cython.profile(False)
    cdef void check_cached_length(self):
        if self.out_num != self.last_len:
            self._remake_numpy_arrays()
            self.last_len = self.out_num

    @cython.profile(False)
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline void _remake_numpy_arrays(self):
        cdef int[::1] inst_codes_view = <int[:self.out_num]>self.out_inst_codes
        self.inst_codes_np = np.asarray(inst_codes_view, dtype=np.int32)

        self.inst_names_np = np.empty(self.out_num, dtype=np.object_)
        self.inst_names_view = self.inst_names_np

        if self.val_type == cpcp.PM_TYPE_STRING:
            self.values_np = np.empty(self.out_num, dtype=np.object_)
        else:
            # TODO: Change this when we aren't converting everything to double
            self.values_np = np.empty(self.out_num, dtype=np.double)


    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.profile(False)
    cdef inline np.ndarray get_inst_codes(self):
        if self.indom == cpcp.PM_INDOM_NULL:
            # TODO: this emulates what puffypcp does
            return NULL_INDOM_INST
        else:
            return self.inst_codes_np

    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.profile(False)
    cdef inline np.ndarray get_inst_names(self):
        if self.indom == cpcp.PM_INDOM_NULL:
            return EMPTY_STR_ARR
        cdef int i
        cdef int inst_code
        for i in range(self.out_num):
            # inst_code = self.out_inst_codes[i]
            # if inst_code not in self.inst_names:
            #     self.inst_names_view[i] = ""
            # else:
            #     self.inst_names_view[i] = self.inst_names[inst_code]

            if self.out_inst_names[i] is NULL:
                # TODO: what to do here?
                self.inst_names_view[i] = ""
            else:
                # This copies the char * to a new python string
                # TODO: keep a dict of codes -> names (as preallocated python strings) and avoid making new ones?
                self.inst_names_view[i] = <object>self.out_inst_names[i]
        return self.inst_names_np

    cdef int[:] get_statuses(self):
        if self.out_num == 0:
            return None
        cdef int [:] view = <int[:self.out_num]>self.out_statuses
        return view

    @cython.profile(False)
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef np.ndarray get_values(self):

        # TODO: everything is converted to doubles to remain consistent with the current mechanism for now.
        # As a separate change, fix this to return arrays of the correct integer types
        if self.val_type == cpcp.PM_TYPE_32:
            _fill_i32(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_U32:
            _fill_u32(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_64:
            _fill_i64(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_U64:
            _fill_u64(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_FLOAT:
            _fill_float(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_DOUBLE:
            _fill_double(self.out_num, self.out_values, self.values_np)

        elif self.val_type == cpcp.PM_TYPE_STRING:
            _fill_string(self.out_num, self.out_values, self.values_np)

        return self.values_np


    @staticmethod
    cdef create(cpcp.pmFG fg, char *metric_name, int num_instances, int val_type, cpcp.pmInDom indom, dict instance_names):
        cdef Metric metric = Metric.__new__(Metric)
        metric.num_instances = num_instances
        metric.val_type = val_type
        metric.indom = indom
        metric.pool = Pool()

        metric.out_inst_codes = <int *>metric.pool.malloc(num_instances * sizeof(int))
        metric.out_inst_names = <char **>metric.pool.malloc(num_instances * sizeof(char *))
        metric.out_values = <cpcp.pmAtomValue *>metric.pool.malloc(num_instances * sizeof(cpcp.pmAtomValue))
        metric.out_statuses = <int *>metric.pool.malloc(num_instances * sizeof(int))

        metric.last_len = 0
        metric.inst_codes_np = None
        metric.inst_names_np = None
        metric.values_np = None

        metric.inst_names = instance_names

        metric.creation_status = cpcp.pmExtendFetchGroup_indom(
            fg,
            metric_name,
            "instant",  # Do not attempt to do rate-conversion or convert units/scale
            metric.out_inst_codes,
            # NULL,
            metric.out_inst_names,
            metric.out_values,
            val_type,
            metric.out_statuses,
            num_instances,
            &metric.out_num,
            &metric.out_status
        )
        return metric


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_i32(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].l

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_u32(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ul

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_i64(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ll

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_u64(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ull

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_float(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].f

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_double(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].d

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.profile(False)
cdef inline _fill_string(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef object[::1] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <object>values[i].cp


def get_stuff2():
    # cdef ArchiveFetchGroup fg = ArchiveFetchGroup("/user/adkofke/pcplogs/20161230.00.10")
    # cdef ArchiveFetchGroup fg = ArchiveFetchGroup("/dev/shm/supremm-adkofke/mae/972366/cpn-p26-07")
    cdef ArchiveFetchGroup fg = ArchiveFetchGroup("/user/adkofke/pcplogs/20161229.00.10")
    fg.set_start(1)
    # cdef int s1 = fg.add_metric("hotproc.io.write_bytes")
    # print cpcp.pmErrStr(s1)
    #
    # s1 = fg.add_metric("hinv.map.cpu_node")
    # print cpcp.pmErrStr(s1)
    #
    # cdef int s2 = fg.add_metric("kernel.all.uptime")
    # print cpcp.pmErrStr(s2)
    #
    # cdef int s3 = fg.add_metric("nfs4.client.reqs")
    # print cpcp.pmErrStr(s3)

    cdef int s4 = fg.add_metric("hotproc.psinfo.environ")
    print cpcp.pmErrStr(s4)
    #
    # cdef int s5 = fg.add_metric("cgroup.cpuset.cpus")
    # print cpcp.pmErrStr(s5)
    #
    # s5 = fg.add_metric("nvidia.memtotal")
    # print cpcp.pmErrStr(s5)

    cdef int fetch_sts

    cdef Metric m
    cdef unsigned int n
    while True:
        fetch_sts = fg.fetch()
        if fetch_sts < 0:
            print "Fetch: {}".format(cpcp.pmErrStr(fetch_sts))
            break

        print "Timestamp {}".format(cpcp.pmtimevalToReal(&fg.timestamp))

        for m, mname in zip(fg.metrics, fg.metric_names):
            n = m.out_num
            if n > 0:
                print "{}: Num: {}, status: {}, error codes: {} <{}>, data: {}".format(
                    mname, n, cpcp.pmErrStr(m.out_status), np.asarray(m.get_statuses()), cpcp.pmErrStr(m.get_statuses()[0]), m.get_values()
                )
            else:
                print "{}: No instances".format(mname)

        print "===="







