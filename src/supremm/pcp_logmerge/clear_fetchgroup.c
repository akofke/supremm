#include "pcp/pmapi.h"

#include <math.h>
#include <assert.h>
#include <inttypes.h>
#include <limits.h>
#include <float.h>

/*
 * An individual fetch-group is a linked list of requests tied to a
 * particular pcp context (which the fetchgroup owns).	NB: This is
 * opaque to the PMAPI client.
 */
struct __pmFetchGroup {
    int	ctx;			/* our pcp context */
    int wrap;			/* wrap-handling flag, set at fg-create-time */
    pmResult *prevResult;
    struct __pmFetchGroupItem *items;
    pmID *unique_pmids;
    size_t num_unique_pmids;
};


/*
 * Common data to describe value/scale conversion.
 */
struct __pmFetchGroupConversionSpec {
    unsigned rate_convert : 1;
    unsigned unit_convert : 1;
    pmUnits output_units;	/* NB: same dim* as input units; maybe different scale */
    double output_multiplier;
};
typedef struct __pmFetchGroupConversionSpec *pmFGC;

struct __pmFetchGroupItem {
    struct __pmFetchGroupItem *next;
    enum { pmfg_item, pmfg_indom, pmfg_event, pmfg_timestamp } type;

    union {
	struct {
	    pmID metric_pmid;
	    pmDesc metric_desc;
	    int metric_inst;	/* unused if metric_desc.indom == PM_INDOM_NULL */
	    struct __pmFetchGroupConversionSpec conv;
	    pmAtomValue *output_value;	/* NB: may be NULL */
	    int output_type;	/* PM_TYPE_* */
	    int *output_sts;	/* NB: may be NULL */
	} item;
	struct {
	    pmID metric_pmid;
	    pmDesc metric_desc;
	    int *indom_codes;	/* saved from pmGetInDom */
	    char **indom_names;
	    unsigned indom_size;
	    struct __pmFetchGroupConversionSpec conv;
	    int *output_inst_codes;	/* NB: may be NULL */
	    char **output_inst_names;	/* NB: may be NULL */
	    pmAtomValue *output_values;	/* NB: may be NULL */
	    int output_type;
	    int *output_stss;	/* NB: may be NULL */
	    int *output_sts;	/* NB: may be NULL */
	    unsigned output_maxnum;
	    unsigned *output_num;	/* NB: may be NULL */
	} indom;
	struct {
	    pmID metric_pmid;
	    pmDesc metric_desc;
	    int metric_inst;
	    pmID field_pmid;
	    pmDesc field_desc;
	    struct __pmFetchGroupConversionSpec conv;
	    struct timespec *output_times; /* NB: may be NULL */
	    pmAtomValue *output_values;	/* NB: may be NULL */
	    pmResult **unpacked_usec_events; /* NB: may be NULL */
	    pmHighResResult **unpacked_nsec_events; /* NB: may be NULL */
	    int output_type;
	    int *output_stss;	/* NB: may be NULL */
	    int *output_sts;	/* NB: may be NULL */
	    unsigned output_maxnum;
	    unsigned *output_num;	/* NB: may be NULL */
	} event;
	struct {
	    struct timeval *output_value;	/* NB: may be NULL */
	} timestamp;
    } u;
};
typedef struct __pmFetchGroupItem *pmFGI;


/*
 * Reinitialize the given pmAtomValue, freeing up any prior dynamic content,
 * marking it "empty".
 */
void
__pmReinitValue(pmAtomValue *oval, int otype)
{
    switch (otype) {
	case PM_TYPE_FLOAT:
	    oval->f = (float)0.0 / (float)0.0; /* nanf(""); */
	    break;
	case PM_TYPE_DOUBLE:
	    oval->d = (double)0.0 / (double)0.0; /* nan(""); */
	    break;
	case PM_TYPE_STRING:
	    free(oval->cp);
	    oval->cp = NULL;
	    break;
	case PM_TYPE_32:
	case PM_TYPE_U32:
	case PM_TYPE_64:
	case PM_TYPE_U64:
	default:
	    memset(oval, -1, sizeof(*oval));
	    break;
    }
}

static void
pmfg_reinit_item(pmFGI item)
{
    pmAtomValue *out_value;

    assert(item != NULL);
    assert(item->type == pmfg_item);

    out_value = item->u.item.output_value;
    if (item->u.item.output_value)
	__pmReinitValue(out_value, item->u.item.output_type);

    if (item->u.indom.output_sts)
	*item->u.indom.output_sts = PM_ERR_VALUE;
}

static void
pmfg_reinit_indom(pmFGI item)
{
    unsigned i;

    assert(item != NULL);
    assert(item->type == pmfg_indom);

    if (item->u.indom.output_values)
	for (i = 0; i < item->u.indom.output_maxnum; i++)
	    __pmReinitValue(&item->u.indom.output_values[i], item->u.indom.output_type);

    if (item->u.indom.output_inst_names)
	for (i = 0; i < item->u.indom.output_maxnum; i++)
	    item->u.indom.output_inst_names[i] = NULL;	/* break ref into indom_names[] */

    if (item->u.indom.output_stss)
	for (i = 0; i < item->u.indom.output_maxnum; i++)
	    item->u.indom.output_stss[i] = PM_ERR_VALUE;

    if (item->u.indom.output_num)
	*item->u.indom.output_num = 0;
}

static int
pmfg_clear_profile(pmFG pmfg)
{
    int sts;

    sts = pmUseContext(pmfg->ctx);
    if (sts != 0)
	return sts;

    /*
     * Wipe clean all instances; we'll add them back incrementally as
     * the fetchgroup is extended.  This cannot fail for the manner in
     * which we call it - see pmDelProfile(3) man page discussion.
     */
    return pmDelProfile(PM_INDOM_NULL, 0, NULL);
}


/*
 * Clear the fetchgroup of all items, keeping the PMAPI context alive.
 */
int
pmClearFetchGroup(pmFG pmfg)
{
    pmFGI item;

    if (pmfg == NULL)
	return -EINVAL;

    /* Walk the items carefully since we're deleting them. */
    item = pmfg->items;
    while (item) {
	pmFGI next_item = item->next;
	switch (item->type) {
	    case pmfg_item:
		pmfg_reinit_item(item);
		break;
	    case pmfg_indom:
		pmfg_reinit_indom(item);
		free(item->u.indom.indom_codes);
		free(item->u.indom.indom_names);
		break;
	    case pmfg_event:
	    assert(0);
//		pmfg_reinit_event(item);
		break;
	    case pmfg_timestamp:
		/* no dynamically allocated content. */
		break;
	    default:
		assert(0);	/* can't happen */
	}
	free(item);
	item = next_item;
    }
    pmfg->items = NULL;

    if (pmfg->prevResult)
	pmFreeResult(pmfg->prevResult);
    pmfg->prevResult = NULL;
    if (pmfg->unique_pmids)
	free(pmfg->unique_pmids);
    pmfg->unique_pmids = NULL;
    pmfg->num_unique_pmids = 0;

    return pmfg_clear_profile(pmfg);
}

void patchDiscrete(pmFG pmfg) {
	pmFGI item;

	for (item = pmfg->items; item; item = item->next) {
		switch (item->type) {
			case pmfg_timestamp:
			break;

			case pmfg_item:
			if (item->u.item.metric_desc.sem == PM_SEM_DISCRETE)
				item->u.item.metric_desc.sem = -4;
			break;

			case pmfg_indom:
			if (item->u.indom.metric_desc.sem == PM_SEM_DISCRETE)
				item->u.item.metric_desc.sem = -4;
			break;

			case pmfg_event:
			break;

			default:
		assert(0);	/* can't happen */
		}
	}
}

void unPatchDiscrete(pmFG pmfg) {
	pmFGI item;
	
	for (item = pmfg->items; item; item = item->next) {
		switch (item->type) {
			case pmfg_timestamp:
			break;

			case pmfg_item:
			if (item->u.item.metric_desc.sem == -4)
				item->u.item.metric_desc.sem = PM_SEM_DISCRETE;
			break;

			case pmfg_indom:
			if (item->u.indom.metric_desc.sem == -4)
				item->u.item.metric_desc.sem = PM_SEM_DISCRETE;
			break;

			case pmfg_event:
			break;

			default:
		assert(0);	/* can't happen */
		}
	}
}
