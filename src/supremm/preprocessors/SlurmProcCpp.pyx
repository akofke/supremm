import re
from collections import Counter
import itertools

from libc.stdint cimport int64_t, int32_t
from cpython.ref cimport PyObject
cimport cython

import numpy as np
cimport numpy as np

from supremm.errors import ProcessingError
from supremm.linuxhelpers import parsecpusallowed

np.import_array()

cdef GROUP_RE = re.compile(r"cpuset:/slurm/uid_\d+/job_\d+/")

@cython.final
cdef class SlurmProcCpp:

    name = property(lambda self: "proc")
    mode = property(lambda self: "timeseries")
    requiredMetrics = property(lambda self: [
        [ "proc.psinfo.cpusallowed",
        "proc.id.uid_nm",
        "proc.psinfo.cgroups" ],
        [ "hotproc.psinfo.cpusallowed",
        "hotproc.id.uid_nm",
        "hotproc.psinfo.cgroups" ]
        ])

    optionalMetrics = property(lambda self: ["cgroup.cpuset.cpus"])
    derivedMetrics = property(lambda self: [])

    @property
    def status(self):
        """ The status state is used by the framework to decide whether to include the
            plugin data in the output """
        return self._status

    @status.setter
    def status(self, value):
        """ status can be set by the framework """
        self._status = value

    @staticmethod
    cdef inline bint slurmcgroupparser(str s):
        """ Parse linux cgroup string for slurm-specific settings and extract
            the UID and jobid of each job
        """

        m = GROUP_RE.search(s)
        if m:
            return True
        else:
            return False

    cdef object _job
    cdef object _status

    cdef dict output
    cdef object cgrouppath
    cdef object expected_cgroup
    cdef object job_username

    cdef set cpus_allowed
    cdef set cgroup_cpuset
    cdef str hostname

    def __cinit__(self, object job):

        self.cgrouppath = "/slurm/uid_" + str(job.acct['uid']) + "/job_" + job.job_id
        self.expected_cgroup = "cpuset:" + self.cgrouppath
        self.job_username = job.acct['user']

        self.cpus_allowed = None
        self.cgroup_cpuset = None
        self.hostname = None

        self.output = {"procDump": {"constrained": Counter(), "unconstrained": Counter()}, "cpusallowed": {}}

    def __init__(self, job):
        self._job = job
        self._status = "uninitialized"

    def logerror(self, info):
        if 'errors' not in self.output:
            self.output['errors'] = {}
        if self.hostname not in self.output['errors']:
            self.output['errors'][self.hostname] = []
        self.output['errors'][self.hostname].append(info)

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def process(self, double timestamp, np.ndarray data, np.ndarray inst_ids, np.ndarray inst_names):
        # TODO: doesn't even need to be a map? just list of commands
        # cdef map[int32_t, char *] constrained_procs
        # cdef map[int32_t, char *] unconstrained_procs
        cdef list constrained_commands = []
        cdef list unconstrained_commands = []
        cdef list cgroupedprocs = []

        cdef int64_t[:] user_pid_indices = np.nonzero(data[1] == self.job_username)[0]
        cdef int64_t i, idx
        cdef int32_t pid
        cdef str[:] proc_names = inst_names[0]
        cdef str[:] proc_cgroups = data[2]
        cdef int32_t[:] pids = inst_ids[0]
        cdef str s, command
        for i in range(user_pid_indices.size):
            idx = user_pid_indices[i]
            pid = pids[idx]


            if proc_names[idx] == "":
                self.logerror("missing process name for pid {}".format(inst_ids[0][idx]))

            s = proc_names[idx]
            command = s[s.find(" ") + 1:]


            if self.expected_cgroup in proc_cgroups[idx]:
                # constrained_procs[pid] = command
                constrained_commands.append(command)
                if self.cpus_allowed is None:
                    cgroupedprocs.append(data[0][idx])
            else:
                if SlurmProcCpp.slurmcgroupparser(proc_cgroups[idx]):
                    pass
                else:
                    # unconstrained_procs[pid] = command
                    unconstrained_commands.append(command)

        if len(data) > 3 and self.cgroup_cpuset is None:
            for cpuset, cgroup in zip(data[3], inst_names[3]):
                if cgroup == self.cgrouppath:
                    self.cgroup_cpuset = parsecpusallowed(cpuset)
                    break

        if self.cpus_allowed is None:
            allcores = set()
            for cpuset in cgroupedprocs:
                allcores |= parsecpusallowed(cpuset)
            if len(allcores) > 0:
                self.cpus_allowed = allcores

        for procname in constrained_commands:#constrained_procs:
            self.output['procDump']['constrained'][procname] += 1

        for procname in unconstrained_commands:
            self.output['procDump']['unconstrained'][procname] += 1

        return True

    def hostend(self):

        if self.cgroup_cpuset is not None:
            self.output['cpusallowed'][self.hostname] = list(self.cgroup_cpuset)
        elif self.cpus_allowed is not None:
            self.output['cpusallowed'][self.hostname] = list(self.cpus_allowed)

        self.cgroup_cpuset = None
        self.cpus_allowed = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def results(self):

        constrained = [x[0] for x in self.output['procDump']['constrained'].most_common()]
        unconstrained = [x[0] for x in self.output['procDump']['unconstrained'].most_common()]

        result = {"constrained": constrained,
                  "unconstrained": unconstrained,
                  "cpusallowed": {}}

        sizelimit = 150
        if len(result["constrained"]) > sizelimit:
            result["constrained"] = result["constrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)
        if len(result["unconstrained"]) > sizelimit:
            result["unconstrained"] = result["unconstrained"][0:sizelimit]
            result["error"] = "process list limited to {0} procs".format(sizelimit)

        i = 0
        for nodename, cpulist in self.output['cpusallowed'].iteritems():
            if 'error' in cpulist:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'error': cpulist['error']}
            else:
                result['cpusallowed']['node{0}'.format(i)] = {'node': nodename, 'cpu_list': ','.join(str(cpu) for cpu in cpulist)}
            i += 1

        return {'procDump': result}
