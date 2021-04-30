#!/usr/bin/env python3

import ast
import asyncio
import pandas
import json
import logging
import sys

from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Coroutine, Dict, Iterable, List, Tuple

import aiohttp
import click
import tenacity as ten

from tdmq.client import Client, Source
from tdmq.utils import timeit

logger = logging.getLogger("ENVIROPLUS")


class RunConfig:
    def __init__(self, source: Dict):
        self._source = source
        self._last_activity = "infinity"
        self._data = dict()
        self._tdmq_source = None
        self._timezone = 'UTC'

    @property
    def source_def(self) -> Dict[str, Any]:
        return self._source

    @property
    def source_id(self) -> str:
        return self._source['id']

    @property
    def source_last_activity(self) -> datetime:
        return self._last_activity

    @source_last_activity.setter
    def source_last_activity(self, value: datetime):
        self._last_activity = value

    @property
    def controlled_properties(self) -> List[str]:
        return self._source['controlledProperties']

    def append_data(self, data: Dict):
        deep_merge_data(self._data, data)

    @property
    def source_data(self) -> Iterable:
        return self._data

    @property
    def tdmq_source(self) -> str:
        return self._tdmq_source

    @tdmq_source.setter
    def tdmq_source(self, tdmq_source: str):
        self._tdmq_source = tdmq_source

    @property
    def source_timezone(self) -> str:
        return self._timezone

    @source_timezone.setter
    def source_timezone(self, timezone: str):
        self._timezone = timezone


class RunConfigList:
    def __init__(self, sources_file: str='sources.json'):
        self._sources_file = sources_file
        self._sources = []

        with open(self._sources_file) as jf:
            _sources_list = json.load(jf)
            for _source in _sources_list:
                self._sources.append(RunConfig(_source))

    def __iter__(self):
        ''' Returns the Iterator object '''
        return RunConfigListIterator(self)

    @property
    def souce_file(self) -> str:
        return self._souce_file


class RunConfigListIterator:
    ''' Iterator class '''
    def __init__(self, run_config):
        self._run_config = run_config
        self._index = 0

    def __next__(self):
        ''''Returns the next value from RunConfigList list '''
        if self._index < len(self._run_config._sources):
            result = self._run_config._sources[self._index]
            self._index += 1
            return result

        raise StopIteration


def enviroplus_retry(fn: Coroutine) -> Coroutine:
    @wraps(fn)
    @ten.retry(retry=ten.retry_if_exception_type(
        (aiohttp.ClientError, aiohttp.http_exceptions.HttpProcessingError)),
               reraise=True,
               wait=ten.wait_random(1, 3),
               stop=ten.stop_after_attempt(3),
               before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
    async def wrapped_fn(*args, **kwargs):
        return await fn(*args, **kwargs)

    return wrapped_fn


class ENVIROPLUSclient:
    def __init__(self, endpoint: str='http://localhost/', conn_limit: int=4):
        self._url = endpoint
        self._logger = logging.getLogger("ENVIROPLUSclient")
        self._conn = aiohttp.TCPConnector(limit=conn_limit)
        self._session = aiohttp.ClientSession(connector=self._conn,
                                              raise_for_status=True)
        self._stats = dict.fromkeys((
            'downloaded_bytes',
            'downloaded_days',
            'missing_days',
            'total_requests'), 0)

    def _count_source(self, data: bytes) -> None:
        self._stats['downloaded_bytes'] += len(data)
        self._stats['downloaded_days'] += 1
        self._stats['total_requests'] += 1

    def _count_missing_days(self) -> None:
        self._stats['missing_days'] += 1
        self._stats['total_requests'] += 1

    def _count_generic_request(self, resp_size: int) -> None:
        self._stats['downloaded_bytes'] += resp_size
        self._stats['total_requests'] += 1

    @property
    def stats(self) -> Dict[str, int]:
        return self._stats

    async def close(self) -> None:
        await self._session.close()
        await self._conn.close()

    @enviroplus_retry
    async def download_source(self, source: str, when: datetime) -> bytes:
        url = f'{self._url}{"" if self._url.endswith("/") else "/"}{source}'
        headers = {'Content-Type': 'application/json'}

        today_utc = datetime.utcnow().date()

        if when < today_utc:
            request_parameter = {'date': when.strftime('%Y%m%d')}
        else:
            request_parameter = {}

        self._logger.debug(
            "Sending download request for source %s with parameter %s",
            source, request_parameter)
        try:
            async with self._session.get(url, params=request_parameter,
                                         headers=headers,
                                         raise_for_status=False) as response:
                data = await response.read()

                if response.status >= 400:
                    content = await response.json()
                    if response.status == 404:
                        self._logger.info(
                            "Source %s for timestamp %s not present",
                            source, request_parameter['date'] if 'date' in
                            request_parameter else '\'today\'')
                        self._count_missing_days()
                        return None
                    else:
                        self._logger.error(
                            "Download attempt error. Status: %s; response: %s",
                            response.status, content)
                        response.raise_for_status()
                else:
                    self._logger.debug(
                        ("Download complete for source %s "
                         "with parameter %s; %s bytes"), source,
                        request_parameter, sizeof_fmt(len(data)))
                    self._count_source(data)
                    return ast.literal_eval(data.decode('ascii'))
        except aiohttp.client_exceptions.ClientResponseError as e:
            self._logger.error(
                "Failed to download source %s with parameter %s",
                source, request_parameter)
            self._logger.exception(e)
            raise


#     @staticmethod
#     def _dt_to_timestamp(dt: datetime) -> int:
#         if dt.tzinfo is None:
#             # assume utc
#             dt = dt.replace(tzinfo=timezone.utc)
#         return int(dt.timestamp() * 1000)


def sizeof_fmt(num, suffix='B'):
    # Thanks to Sridhar Ratnakumar
    # https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def deep_merge_data(old_data: Dict, new_data: Dict, path: str=None):
    # Thanks to Andrew Cooke
    # https://stackoverflow.com/a/7205107

    if path is None:
        path = []

    if new_data is None:
        return old_data

    for _k in new_data:
        if _k in old_data:
            if isinstance(
                    old_data[_k], dict) and isinstance(new_data[_k], dict):
                deep_merge_data(old_data[_k], new_data[_k], path + [str(_k)])
            elif old_data[_k] == new_data[_k]:
                pass
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(_k)]))
        else:
            old_data[_k] = new_data[_k]

    return old_data


async def download_source_at_ts(enviroplus_client: ENVIROPLUSclient,
                                source: RunConfig,
                                ts: datetime) -> Dict[str, Any]:
    """
    Download all the records at the single specified timestamp.
    """
    downloads = await asyncio.gather(
        enviroplus_client.download_source(source.source_id, ts))

    for _d in downloads:
        if _d:
            source.append_data(_d)

    return source


async def gen_source_timestamps(sources: RunConfigList,
                                strictly_after: datetime=None):
    if not sources:
        return

    # Compare the timestamps referring to UTC timezone
    today_utc = datetime.utcnow().date()

    for s in sources:
        if s.source_last_activity is not None:
            last_timestamp = s.source_last_activity.date()
        else:
            last_timestamp = today_utc - timedelta(days=2)

        while last_timestamp <= today_utc:
            yield s, last_timestamp
            last_timestamp = last_timestamp + timedelta(days=1)


def restructure_download_results(source: RunConfig) -> Tuple[List, List]:
    df = pandas.DataFrame(source.source_data)
    replace_names = {
        'pressure': 'barometricPressure',
        'humidity': 'relativeHumidity',
        'light': 'illuminance',
        'pm1': 'PM1',
        'pm25': 'PM2.5',
        'pm10': 'PM10',
    }

    df.index = pandas.to_datetime(df.index)
    df.index = df.index.tz_localize(
        source.source_timezone).tz_convert('UTC')
    df.rename(columns=replace_names, inplace=True)

    if source.source_last_activity:
        df = df[df.index > pandas.to_datetime(source.source_last_activity)]

    times = df.index.to_list()
    products = df.to_dict(orient='records')

    return times, products


async def write_to_tdmq(source: RunConfig) -> None:
    times, products = restructure_download_results(source)

    logger.debug("Executing Source.insert_many in executor.  "
                 "Inserting %s timestamps and %s products",
                 len(times), len(products))
    # Source.ingest_many is not a coroutine.  We execute it through
    # `run_in_executor` to avoid blocking the program.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, source.tdmq_source.ingest_many, times, products)

    # Report some info to log
    logger.info("Ingested batch with %s items", len(times))
    # if logger.isEnabledFor(logging.INFO):
    #     logger.info("Ingested %s for timestamps:\n\t%s",
    #                 ' and '.join(products.keys()),
    #                 '\n\t'.join(t.isoformat() for t in times))


# async def ingest_sources(destination: List[Source], strictly_after: datetime,
#                         batch_size: int=20, max_batches: int=3) -> None:
async def ingest_sources(sources: RunConfigList, endpoint: str,
                         batch_size: int=20, max_batches: int=3) -> None:
    outstanding_batch_semaphore = asyncio.Semaphore(max_batches)
    outstanding_batches = 0

    async def finalize_batch(batch: Iterable[asyncio.Task],
                             previous_finalizer: asyncio.Task) -> None:
        nonlocal outstanding_batches
        task_name = asyncio.current_task().get_name()
        logger.debug("%s: waiting on semaphore. ", task_name)
        async with outstanding_batch_semaphore:
            outstanding_batches += 1
            try:
                logger.debug("%s: Semaphore acquired.  Outstanding batches %s",
                             task_name, outstanding_batches)
                logger.debug("awaiting on %s downloads", len(batch))
                try:
                    await asyncio.gather(*batch)
                except Exception as e:
                    logging.error("Exception from download tasks")
                    logging.exception(e)
                    cancel_all(batch)
                    # Await cancelled tasks or we'll get warnings about
                    # coroutines never having been awaited
                    await asyncio.gather(*batch, return_exceptions=True)
                    raise
                logger.debug("%s: downloads completed", task_name)
                logger.info("Downloaded batch of %s pages complete",
                            len(batch))

                if previous_finalizer:
                    # We await on the previous finalizer to ensure our writes
                    # happen in order
                    logger.debug(
                        "%s awaiting on finalizing of previous batch %s",
                        task_name, previous_finalizer.get_name())
                    await previous_finalizer
                    logger.debug(
                        "%s: Finished awaiting. Ingesting batch", task_name)
            finally:
                outstanding_batches -= 1
        logger.debug("%s: Semaphore released.  Outstanding batches %s",
                     task_name, outstanding_batches)

    enviroplus_client = ENVIROPLUSclient(endpoint)
    with timeit(logger.info, "Total ingestion time: %0.5f seconds"):
        try:
            logger.debug("Created ENVIROPLUSclient")
            batch = list()
            batch_counter = 1
            tasks = list()
            # async for s, t  in gen_source_timestamps(sources,
            #                                           strictly_after):
            async for s, t in gen_source_timestamps(sources):
                logger.debug(
                    "appending timestamp %s for source %s to batch",
                    t.isoformat(), s.source_id)
                batch.append(
                    asyncio.create_task(
                        download_source_at_ts(enviroplus_client, s, t)))
                if len(batch) >= batch_size:
                    logger.debug(
                        "Created batch of size %s. Passing to finalize",
                        len(batch))
                    # We chain the calls to finalize_batch, making each call
                    # await on the previous one.  This enforces a global time
                    # order in the products are ingested.
                    t = asyncio.create_task(
                        finalize_batch(batch, tasks[-1] if tasks else None),
                        name=f"finalize_batch_{batch_counter}")
                    tasks.append(t)
                    batch = list()
                    batch_counter += 1
            if len(batch) > 0:
                logger.debug("Finalizing last batch; size %s", len(batch))
                t = asyncio.create_task(
                    finalize_batch(batch, tasks[-1] if tasks else None),
                    name=f"finalize_batch_{batch_counter}")
                tasks.append(t)

            if tasks:
                logger.info(
                    "Waiting for download and ingestion tasks to finish")
                logger.debug(
                    "%s tasks are not yet done",
                    sum(1 for t in asyncio.all_tasks() if not t.done()))
                try:
                    await asyncio.gather(*tasks)
                except Exception as e:
                    logger.error("Caught exception at the top level!")
                    logger.exception(e)
                    logger.error("Cancelling outstanding ingestion tasks")
                    cancel_all(tasks)
                    cancel_all(batch)
                    # await cancelled tasks

                    await asyncio.gather(*tasks, return_exceptions=True)
                    await asyncio.gather(*batch, return_exceptions=True)
                    raise
        finally:
            await enviroplus_client.close()

        batch = list()
        for s in sources:
            logger.debug(
                "appending source %s to ingestion batch", s.source_id)
            batch.append(asyncio.create_task(write_to_tdmq(s)))
        try:
            await asyncio.gather(*batch)
        except Exception as ex:
            logger.error("Caught exception at during the ingestion phase!")
            logger.exception(ex)

    print_enviroplus_client_stats(enviroplus_client.stats)


def cancel_all(tasks: Iterable[asyncio.Task]) -> None:
    for t in tasks:
        logger.debug("Cancelling task %s", t)
        t.cancel()


def print_enviroplus_client_stats(stats: dict) -> None:
    logger.info("Closed ENVIROPLUSclient.  Download stats:")
    logger.info("\tdownloaded volume: %s",
                sizeof_fmt(stats['downloaded_bytes']))
    logger.info("\tdownloaded days: %s", stats['downloaded_days'])
    logger.info("\tmissing days: %s", stats['missing_days'])
    logger.info("\ttotal_requests: %s", stats['total_requests'])


def fetch_tdmq_source(client: Client, source_description: Dict) -> Source:
    sources = client.find_sources(args={'id': source_description['id']})
    if len(sources) > 1:
        raise RuntimeError(f"Bug?  Got {len(sources)} sources from tdmq query "
                           "for source id {run_conf.source_id}. Aborting")

    if len(sources) == 1:
        source = sources[0]
        logger.info("Found existing source with tdmq_id %s", source.tdmq_id)
    else:
        source = None

    return source


def register_tdmq_source(client: Client, source_definition: Dict) -> Source:
    source = client.register_source(source_definition)
    logger.info("New source registered with tdmq_id %s", source.tdmq_id)

    return source


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level)
    log_format = '[%(asctime)s] %(name)s %(levelname)s:  %(message)s'
    logging.basicConfig(level=level, format=log_format)


@click.group()
@click.argument('tdmq-endpoint', envvar='TDMQ_URL')
@click.argument('tdmq-token', envvar='TDMQ_AUTH_TOKEN')
@click.option("--log-level", envvar="ENVIROPLUS_LOG_LEVEL",
              type=click.Choice(['DEBUG', 'INFO', 'WARNING',
                                 'ERROR', 'CRITICAL']), default='INFO')
@click.pass_context
def enviroplus(ctx, tdmq_endpoint: str, tdmq_token: str, log_level) -> None:
    configure_logging(log_level)
    logger.debug("main(%s, %s, %s)",
                 tdmq_endpoint, tdmq_token, log_level)

    if not tdmq_token:
        raise ValueError("TDMQ token not provided!")

    logger.info(
        "Pointing to tdmq service %s. Auth token provided",
        tdmq_endpoint)
    tdmq_client = Client(tdmq_endpoint, auth_token=tdmq_token)

    # make the client available to subcommands
    ctx.ensure_object(dict)
    ctx.obj['tdmq_client'] = tdmq_client
    logger.debug("enviroplus finished.  Continuing in subcommand (if any)")


# ######################### High-level overview ###############################
# `ingest` is the entry point of the download and ingest process.
# This functions sets up the the work, then calls the ingest_products
# coroutine to actually do the work.
#
# ingest_products:
#   * Uses the gen_product_timestamps() generator to create the timestamps of
#     the products to be downloaded, in order.
#   * Iterates over these timestamps and, for each one, creates a download task
#     implemented by download_products_at_ts.
#   * These download tasks get put in batches.
#   * Each batch is passed to a call to finalize_batch, which awaits them and
#     then ingests their output.
#   * To enforce write order, each batch awaits on the previous one.
#
# #############################################################################
@enviroplus.command()
@click.option('--sources-file', default='sources.json',
              envvar='ENVIROPLUS_SOURCES_FILE',
              type=str, show_default=True, show_envvar=True,
              help=("File containing the definition of the sources to ingest"))
@click.option('--batch-size', default=20, envvar='ENVIRONPLUS_BATCH_SIZE',
              type=int, show_default=True, show_envvar=True,
              help=("Size of batch of products to be concurrently downloaded "
                    "and then written."))
@click.option('--max-batches', default=3, envvar='ENVIRONPLUS_MAX_BATCHES',
              type=int, show_default=True, show_envvar=True,
              help=("Max number of downloaded batches to queue up in memory "
                    "for writing to the array"))
@click.option('--strictly-after',
              # help=("Force the start timestamp for the downloaded products "
              #       "to be downloaded. ISO format."))
              help=("Not yet implemented"))
@click.option('--enviroplus-endpoint', default='http://localhost/',
              show_default=True, envvar='ENVIROPLUS_URL', show_envvar=True,
              help=("The endpoint of the Enviroplus server"))
@click.option('--from-timezone', help=("The time zone of the data"))
@click.pass_context
def ingest(click_ctx, batch_size: int, max_batches: int,
           from_timezone: str, sources_file: str,
           enviroplus_endpoint: str='http://localhost/',
           strictly_after: str=None) -> None:
    """
    Ingest data from the Enviroplus service into the TDM polystore.
    """
    run_conf_list = RunConfigList(sources_file)

    tdmq_client = click_ctx.obj['tdmq_client']

#     if strictly_after:
#         # parse user input.  Set `last_time` variable
#         strictly_after = datetime.fromisoformat(strictly_after)
#         if strictly_after.tzinfo is None:
#             strictly_after = strictly_after.replace(tzinfo=timezone.utc)
#             logger.info(
#                 "--strictly-after: Time zone not specified.  Assuming UTC")
#         logger.warning("Clipping data download to start after timestamp %s",
#                        strictly_after.isoformat())
#
#     logger.info("Ingesting %s products.", ' and '.join(run_conf.products))

    if from_timezone:
        logger.info(
            ("Timezone %s provided for the sources: "
             "timestamps will be converted to UTC."), from_timezone)
    else:
        logger.info(
            ("Timezone not provided for the sources: "
             "timestamps will be treated as UTC."))

    # Fetch or register the TDMq sources.
    for s in run_conf_list:
        source = fetch_tdmq_source(tdmq_client, s.source_def)
        if not source:
            source = register_tdmq_source(tdmq_client, s.source_def)
        # elif strictly_after:
        #     click_ctx.fail(
        #         "Can't specify --strictly-after on an existing source!")

        ts = source.get_latest_activity()
        if len(ts) > 0:
            s.source_last_activity = ts.time[0]
            logger.info(
                "Last previous activity reported by source %s: %s.",
                s.source_id, s.source_last_activity.isoformat())
        else:
            s.source_last_activity = None
            logger.info(
                "No previous activity reported by source %s.", s.source_id)

        if from_timezone:
            s.source_timezone = from_timezone
        s.tdmq_source = source

#     if strictly_after and (not last_time or strictly_after > last_time):
#         # There is previous activity that is previous to strictly_after.  We
#         # override # The ingestion start timestamp with the one provided by
#         # strictly_after
#         last_time = strictly_after
#         logger.info(
#             ("Bounding ingestion to %s as specified by "
#              "--strictly-after option"), last_time.isoformat())

        logger.info(
            "Ingesting data starting from %s",
            s.source_last_activity.isoformat() if
            s.source_last_activity else "infinity")

#     asyncio.run(
#         ingest_sources(
#             destination=source, strictly_after=last_time,
#             batch_size=batch_size, max_batches=max_batches))

    asyncio.run(
        ingest_sources(sources=run_conf_list, endpoint=enviroplus_endpoint,
                       batch_size=batch_size, max_batches=max_batches))

    logger.info("Finished ingesting.")
    logger.info("Operation complete")


@enviroplus.command()
@click.option('--sources-file', default='sources.json',
              envvar='ENVIROPLUS_SOURCES_FILE',
              type=str, show_default=True, show_envvar=True,
              help=("File containing the definition of the sources to ingest"))
@click.pass_obj
def register_source(click_obj, sources_file: str) -> None:
    """
    Register TDMq Source, if it doesn't exist.
    """
    run_conf_list = RunConfigList(sources_file)
    tdmq_client = click_obj['tdmq_client']

    # Fetch or register the TDMq sources.
    for s in run_conf_list:
        source = fetch_tdmq_source(tdmq_client, s.source_def)
        if not source:
            source = register_tdmq_source(tdmq_client, s.source_def)


@enviroplus.command()
@click.argument('source_id')
@click.pass_obj
def delete_source(click_obj, source_id: str) -> None:
    """
    Deregister the specified source
    """
    click.confirm(
        "Are you sure you want to delete the source and all associated data?",
        abort=True)
    tdmq_client = click_obj['tdmq_client']

    sources = tdmq_client.find_sources(args={'id': source_id})
    if len(sources) > 1:
        raise RuntimeError((
            f"Bug?  Got {len(sources)} sources from tdmq query "
            f"for source id {source_id}. Aborting"))

    if not sources:
        logger.error(
            ("Selected source %s not found in tdm polystore. "
             "Nothing to delete"), source_id)
        sys.exit(2)
    else:
        source = sources[0]
        tdmq_client.deregister_source(source)


if __name__ == "__main__":
    enviroplus()
