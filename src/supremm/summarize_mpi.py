# pylint: disable=import-error
import itertools
import logging
import sys

from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor

from supremm import outputter
from supremm.account import DbAcct
from supremm.config import Config
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.proc_common import getoptions, override_defaults, filter_plugins
from supremm.scripthelpers import setuplogger
from supremm.summarize_jobs import get_jobs, iter_jobs, do_summarize, process_summary, clean_jobdir
from supremm.xdmodaccount import XDMoDAcct

if MPI.COMM_WORLD.Get_rank() != 0:
    # get the "global var" set when the pool spawns worker processes
    _supremm_log_level = sys.modules['__main__']._supremm_log_level
    rank = MPI.COMM_WORLD.Get_rank()
    log_file = "mpiOutput-{}.log".format(rank)
    setuplogger(_supremm_log_level, log_file, _supremm_log_level)

    logging.info("Worker process {} starting, nodename {}".format(rank, MPI.Get_processor_name()))


CHUNK_SIZE = 50  # TODO: tune this


def chunked_iter(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def process_jobs_mpi(config, opts, allpreprocs, allplugins):
    max_workers = opts['threads'] if opts['threads'] > 1 else None

    with MPIPoolExecutor(max_workers, globals={"_supremm_log_level": opts['log']}) as pool:

        for r, resconf in config.resourceconfigs():
            if opts['resource'] is None or opts['resource'] == r or opts['resource'] == str(resconf['resource_id']):
                logging.info("Processing resource %s", r)
            else:
                continue

            resconf = override_defaults(resconf, opts)

            preprocs, plugins = filter_plugins(resconf, allpreprocs, allplugins)

            logging.debug("Using %s preprocessors", len(preprocs))
            logging.debug("Using %s plugins", len(plugins))

            process_resource_mpi(resconf, preprocs, plugins, config, opts, pool)


def process_resource_mpi(resconf, preprocs, plugins, config, opts, pool):
    with outputter.factory(config, resconf, dry_run=opts['dry_run']) as m:
        if resconf['batch_system'] == "XDMoD":
            dbif = XDMoDAcct(resconf['resource_id'], config)
        else:
            dbif = DbAcct(resconf['resource_id'], config)

        jobs = get_jobs(opts, dbif)
        jobs_iter = iter_jobs(jobs, config, resconf, plugins, preprocs, opts)
        for job_chunk in chunked_iter(CHUNK_SIZE, jobs_iter):
            for job, result, summarize_time in pool.map(do_summarize_mpi, job_chunk, unordered=True):
                if result is not None:
                    process_summary(m, dbif, opts, job, summarize_time, result)
                    clean_jobdir(opts, job)
                else:
                    clean_jobdir(opts, job)


def do_summarize_mpi(args):
    # needs to be in this module so it gets imported in the child process
    # and global setup gets called
    return do_summarize(args)


def main():
    opts = getoptions(True)

    setuplogger(opts['log'])  # TODO decide on logging scheme

    config = Config()

    allpreprocs = loadpreprocessors()
    logging.debug("Loaded %s preprocessors", len(allpreprocs))

    allplugins = loadplugins()
    logging.debug("Loaded %s plugins", len(allplugins))

    process_jobs_mpi(config, opts, allpreprocs, allplugins)


if __name__ == '__main__':
    main()
