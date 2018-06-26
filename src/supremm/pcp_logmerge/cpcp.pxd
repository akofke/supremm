from posix.types cimport pid_t

cdef extern from "sys/time.h":
    cdef struct timeval:
        long tv_sec
        long tv_usec

cdef inline double pmtimevalToReal(const timeval *val):
    return val.tv_sec + (<long double>val.tv_usec / <long double>1000000)


cdef inline void pmtimevalFromReal(double secs, timeval *val):
    val.tv_sec = <long>secs
    val.tv_usec = <long>(<long double>(secs - val.tv_sec) * <long double>1000000 + <long double>0.5)

cdef extern from "pcp/pmapi.h":

    const int PM_TZ_MAXLEN = 40
    const int PM_LOG_MAXHOSTLEN = 64
    ctypedef struct pmLogLabel:
        int		ll_magic
        pid_t	ll_pi
        timeval	ll_start
        char	ll_hostname[PM_LOG_MAXHOSTLEN]
        char	ll_tz[PM_TZ_MAXLEN]

    ctypedef unsigned int pmID
    ctypedef unsigned int pmInDom

    ctypedef struct pmUnits:
        pass

    ctypedef struct pmDesc:
        pmID pmid
        int type
        pmInDom indom
        int sem
        pmUnits units

    ctypedef struct pmFG:
        pass

    ctypedef union pmAtomValue:
        # TODO use <inttypes.h> types instead of simple long etc.
        char* cp
        long l
        unsigned long ul
        long long ll
        unsigned long long ull
        float f
        double d

    # double pmtimevalToReal(const timeval *val)
    #
    # void pmtimevalFromReal(double secs, timeval *val)

    char* pmErrStr(int status)

    int pmUseContext(int ctx)

    int pmSetMode(int mode, const timeval* when, int delta)

    int pmGetArchiveLabel(pmLogLabel *lp)

    int pmLookupName(int numpmid, char **namelist, pmID *pmidlist)

    int pmLookupDesc(pmID pmid, pmDesc *desc)

    int pmGetInDomArchive(pmInDom indom, int **instlist, char ***namelist)

    int pmCreateFetchGroup(pmFG* ptr, int type, const char* name)

    int pmGetFetchGroupContext(pmFG pmfg)

    int pmExtendFetchGroup_indom(
            pmFG pmfg,
            const char *metric,
            const char *scale,
            int[] out_inst_codes,
            char *out_inst_names[],
            pmAtomValue out_values[],
            int out_type,
            int[] out_statuses,
            unsigned int out_maxnum,
            unsigned int *out_num,
            int *out_status
    )

    int pmExtendFetchGroup_timestamp(pmFG pmfg, timeval* out_value)

    int pmFetchGroup(pmFG pmfg)

    int pmDestroyFetchGroup(pmFG pmfg)
