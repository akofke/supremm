from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.stdlib cimport free
from libc.stdint cimport int32_t, uint32_t, int64_t, uint64_t

import numpy as np
cimport numpy as np
np.import_array()

import cpmapi as c_pmapi
cimport cpcp


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
    cdef tuple archives

    def __cinit__(self, list archives):
        self.archives = (ArchiveFetchGroup(path) for path in archives)


    cpdef add_metrics_required(self, list metrics):
        pass



cdef class ArchiveFetchGroup:
    cdef int creation_status
    cdef cpcp.pmFG fg
    cdef dict indom_sizes
    cdef list metrics
    cdef list metric_names
    cdef cpcp.timeval timestamp

    def __cinit__(self, str archive_path):
        self.creation_status = cpcp.pmCreateFetchGroup(&self.fg, c_pmapi.PM_CONTEXT_ARCHIVE, archive_path)
        self.indom_sizes = {}
        self.metrics = []
        self.metric_names = []

        if not self.creation_status < 0:
            cpcp.pmExtendFetchGroup_timestamp(self.fg, &self.timestamp)

    def __dealloc__(self):
        if self.fg != <cpcp.pmFG>NULL:
            cpcp.pmDestroyFetchGroup(self.fg)


    cdef int set_start(self, double time):
        cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(self.fg))
        cdef cpcp.timeval tv
        cpcp.pmtimevalFromReal(time, &tv)
        return cpcp.pmSetMode(c_pmapi.PM_MODE_FORW, &tv, 0)

    cdef int fetch(self):
        return cpcp.pmFetchGroup(self.fg)

    cdef int get_indom_size(self, cpcp.pmInDom indom):
        cdef int num_instances
        cdef int *instlist
        cdef char **namelist
        if indom == c_pmapi.PM_INDOM_NULL:
            return 1
        if indom in self.indom_sizes:
            num_instances = self.indom_sizes[indom]
        else:
            num_instances = cpcp.pmGetInDomArchive(indom, &instlist, &namelist)
            if num_instances < 0:
                return num_instances  # no allocation happens if error
            elif num_instances > 1:
                # no allocation if num_instances is less than one, but otherwise we need to free
                free(instlist)
                free(namelist)
            self.indom_sizes[indom] = num_instances
            return num_instances

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

        cdef Metric metric = Metric.create(self.fg, name, num_instances, desc.type)
        if metric.creation_status < 0:
            return metric.creation_status  # error extending, return the error code and the Metric will go out of scope now
        else:
            self.metrics.append(metric)
            self.metric_names.append(metric_name)
            return 0


cdef class Metric:
    cdef int num_instances
    cdef int val_type
    cdef Pool pool
    cdef int creation_status

    cdef int *out_inst_codes
    cdef char **out_inst_names
    cdef cpcp.pmAtomValue *out_values
    cdef int *out_statuses
    cdef unsigned int out_num
    cdef int out_status

    cdef get_statuses(self):
        cdef int[:] view
        if self.out_num == 0:
            return None
        view = <int[:self.out_num]>self.out_statuses
        return view

    cdef np.ndarray get_values(self):
        cdef np.ndarray val_arr

        # TODO: everything is converted to doubles to remain consistent with the current mechanism for now.
        # As a separate change, fix this to return arrays of the correct integer types
        if self.val_type == c_pmapi.PM_TYPE_32:
            # val_arr = np.empty(self.out_num, dtype=np.int32)
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_i32(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_U32:
            # val_arr = np.empty(self.out_num, dtype=np.uint32)
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_u32(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_64:
            # val_arr = np.empty(self.out_num, dtype=np.int64)
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_i64(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_U64:
            # val_arr = np.empty(self.out_num, dtype=np.uint64)
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_u64(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_FLOAT:
            # val_arr = np.empty(self.out_num, dtype=np.single)
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_float(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_DOUBLE:
            val_arr = np.empty(self.out_num, dtype=np.double)
            _fill_double(self.out_num, self.out_values, val_arr)

        elif self.val_type == c_pmapi.PM_TYPE_STRING:
            val_arr = np.empty(self.out_num, dtype=np.object_)
            _fill_string(self.out_num, self.out_values, val_arr)

        return val_arr


    @staticmethod
    cdef create(cpcp.pmFG fg, char *metric_name, int num_instances, int val_type):
        cdef Metric metric = Metric.__new__(Metric)
        metric.num_instances = num_instances
        metric.val_type = val_type
        metric.pool = Pool()

        metric.out_inst_codes = <int *>metric.pool.malloc(num_instances * sizeof(int))
        metric.out_inst_names = <char **>metric.pool.malloc(num_instances * sizeof(char *))
        metric.out_values = <cpcp.pmAtomValue *>metric.pool.malloc(num_instances * sizeof(cpcp.pmAtomValue))
        metric.out_statuses = <int *>metric.pool.malloc(num_instances * sizeof(int))

        metric.creation_status = cpcp.pmExtendFetchGroup_indom(
            fg,
            metric_name,
            NULL,
            metric.out_inst_codes,
            metric.out_inst_names,
            metric.out_values,
            val_type,
            metric.out_statuses,
            num_instances,
            &metric.out_num,
            &metric.out_status
        )
        return metric


cdef _fill_i32(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].l

cdef _fill_u32(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ul

cdef _fill_i64(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ll

cdef _fill_u64(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].ull

cdef _fill_float(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].f

cdef _fill_double(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef double[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <double>values[i].d

cdef _fill_string(unsigned int n, cpcp.pmAtomValue *values, np.ndarray arr):
    cdef object[:] view = arr
    cdef unsigned int i
    for i in range(n):
        view[i] = <object>values[i].cp


def get_stuff():
    cdef char *archive = "/user/adkofke/pcplogs/20161230.00.10"
    cdef char *metric = "kernel.all.load"
    cdef cpcp.pmFG fg

    cdef int sts = cpcp.pmCreateFetchGroup(&fg, c_pmapi.PM_CONTEXT_ARCHIVE, archive)
    print "Creating fg: {}".format(cpcp.pmErrStr(sts))

    cpcp.pmUseContext(cpcp.pmGetFetchGroupContext(fg))

    cdef cpcp.pmLogLabel log_label
    cpcp.pmGetArchiveLabel(&log_label)

    cdef cpcp.timeval start
    cpcp.pmtimevalFromReal(1, &start)
    cpcp.pmSetMode(c_pmapi.PM_MODE_FORW, &start, 0)

    cdef cpcp.timeval tv
    cpcp.pmExtendFetchGroup_timestamp(fg, &tv)


    cdef int out_inst_codes[5]
    cdef char* out_inst_names[5]
    cdef cpcp.pmAtomValue out_values[5]
    cdef int out_statuses[5]
    cdef unsigned int out_num
    cdef int out_status
    cdef int sts1 = cpcp.pmExtendFetchGroup_indom(
        fg,
        metric,
        NULL,
        out_inst_codes,
        out_inst_names,
        out_values,
        c_pmapi.PM_TYPE_DOUBLE,
        out_statuses,
        5,
        &out_num,
        &out_status
    )

    print "Extend fg indom: {}".format(cpcp.pmErrStr(sts1))

    cdef int sts2
    cdef double ts
    # while True:
    #     sts2 = cpcp.pmFetchGroup(fg)
    #     print "Fetch group: {}".format(cpcp.pmErrStr(sts2))
    #     if sts2 == c_pmapi.PM_ERR_EOL:
    #         break
    #
    #     ts = cpcp.pmtimevalToReal(&tv)
    #     print "Timestamp: {}".format(ts)
    #     for i in xrange(out_num):
    #         print "{} status {}, value {}".format(
    #             out_inst_names[i] if out_inst_names[i] is not NULL else "Only instance",
    #             cpcp.pmErrStr(out_statuses[i]),
    #             out_values[i]
    #         )
    #
    #     print "===="


def get_stuff2():
    # cdef ArchiveFetchGroup fg = ArchiveFetchGroup("/user/adkofke/pcplogs/20161230.00.10")
    cdef ArchiveFetchGroup fg = ArchiveFetchGroup("/user/adkofke/pcplogs/job-972366-begin-20161229.23.06.00")
    fg.set_start(1)
    # cdef int s1 = fg.add_metric("hinv.map.cpu_node")
    # print cpcp.pmErrStr(s1)
    #
    # cdef int s2 = fg.add_metric("kernel.all.uptime")
    # print cpcp.pmErrStr(s2)

    cdef int s3 = fg.add_metric("nfs4.client.reqs")
    print cpcp.pmErrStr(s3)

    # cdef int s4 = fg.add_metric("hotproc.psinfo.environ")
    # print cpcp.pmErrStr(s4)

    cdef int fetch_sts

    cdef Metric m
    cdef unsigned int n
    while True:
        fetch_sts = fg.fetch()
        if fetch_sts < 0:
            print "Fetch: {}".format(cpcp.pmErrStr(fetch_sts))
            break

        print "Timestamp {}".format(cpcp.pmtimevalToReal(&fg.timestamp))

        for m in fg.metrics:
            n = m.out_num
            print "Num: {}, status: {}, error codes: {}, data: {}".format(n, cpcp.pmErrStr(m.out_status), np.asarray(m.get_statuses()), m.get_values())







