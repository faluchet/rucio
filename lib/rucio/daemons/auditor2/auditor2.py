# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Auditor2 is a daemon ... #TODO fill me
'''

import functools
import logging
import os
import threading

from typing import TYPE_CHECKING

from rucio.common.exception import (RSENotFound,
                                    VONotFound)
# from rucio.common.config import config_get, config_get_bool
from rucio.common.config import config_get_bool
from rucio.common.logging import setup_logging
from rucio.core.vo import list_vos
from rucio.core.rse import list_rses, RseData
from rucio.core.rse_expression_parser import parse_expression

from rucio.daemons.common import run_daemon


if TYPE_CHECKING:
    from typing import List, Dict, Optional, Any
    from rucio.daemons.common import HeartbeatHandler

GRACEFUL_STOP = threading.Event()


def get_rses_to_process(rses: "List[str]", include_rses: str, exclude_rses: str, vos: "List[str]") -> "List[Dict]":
    """
    Return the list of RSEs to process based on rses, include_rses and exclude_rses

    :param rses:               List of RSEs the audito should work against. If empty, it considers all RSEs.
    :param exclude_rses:       RSE expression to exclude RSEs.
    :param include_rses:       RSE expression to include RSEs.
    :param vos:                VOs on which to look for RSEs. Only used in multi-VO mode.
                               If None, we either use all VOs if run from "def"

    :returns: A list of RSEs to process
    """
    multi_vo = config_get_bool('common', 'multi_vo', raise_exception=False, default=False)
    if not multi_vo:
        if vos:
            logging.log(logging.WARNING, 'Ignoring argument vos, this is only applicable in a multi-VO setup.')
        vos = ['def']
    else:
        if vos:
            invalid = set(vos) - set([v['vo'] for v in list_vos()])
            if invalid:
                msg = 'VO{} {} cannot be found'.format('s' if len(invalid) > 1 else '', ', '.join([repr(v) for v in invalid]))
                raise VONotFound(msg)
        else:
            vos = [v['vo'] for v in list_vos()]
        logging.log(logging.INFO, 'Auditor2: This instance will work on VO%s: %s' % ('s' if len(vos) > 1 else '', ', '.join([v for v in vos])))

    pid = os.getpid()
    cache_key = 'rses_to_process_%s' % pid
    if multi_vo:
        cache_key += '@%s' % '-'.join(vo for vo in vos)

    all_rses = []
    for vo in vos:
        all_rses.extend(list_rses(filters={'vo': vo}))

    if rses:
        invalid = set(rses) - set([rse['rse'] for rse in all_rses])
        if invalid:
            msg = 'RSE{} {} cannot be found'.format('s' if len(invalid) > 1 else '',
                                                    ', '.join([repr(rse) for rse in invalid]))
            raise RSENotFound(msg)
        result = [rse for rse in all_rses if rse['rse'] in rses]
    else:
        result = all_rses

    if include_rses:
        included_rses = parse_expression(include_rses)
        result = [rse for rse in result if rse in included_rses]

    if exclude_rses:
        excluded_rses = parse_expression(exclude_rses)
        result = [rse for rse in result if rse not in excluded_rses]

    logging.log(logging.INFO, 'Auditor2: This instance will work on RSEs: %s', ', '.join([rse['rse'] for rse in result]))  # type: ignore
    return result


def auditor2(rses: "List[str]", include_rses: str, exclude_rses: str, vos: "List[str]", once: bool = False, sleep_time: int = 60) -> None:
    """
    Main loop to ...  # @TODO fill me

    :param rses:                   List of RSEs the auditor2 should work against.
                                   If empty, it considers all RSEs.
    :param include_rses:           RSE expression to include RSEs.
    :param exclude_rses:           RSE expression to exclude RSEs.
    :param vos:                    VOs on which to look for RSEs. Only used in multi-VO mode.
                                   If None, we either use all VOs if run from "def",
                                   or the current VO otherwise.
    :param sleep_time:             Time between two cycles.
    """

    executable = 'auditor2'
    # oidc_account = config_get('reaper', 'oidc_account', False, 'root')
    # oidc_scope = config_get('reaper', 'oidc_scope', False, 'delete')
    # oidc_audience = config_get('reaper', 'oidc_audience', False, 'rse')

    run_daemon(
        once=once,
        graceful_stop=GRACEFUL_STOP,
        executable=executable,
        logger_prefix=executable,
        partition_wait_time=0 if once else 10,
        sleep_time=sleep_time,
        run_once_fnc=functools.partial(
            run_once,
            rses=rses,
            include_rses=include_rses,
            exclude_rses=exclude_rses,
            vos=vos,
        )
    )


def run_once(rses: "List[str]", include_rses: str, exclude_rses: str, vos: "List[str]", heartbeat_handler: "HeartbeatHandler", **_kwargs: "Dict") -> bool:

    def _run_once(rses_to_process: "List[RseData]", heartbeat_handler: "HeartbeatHandler", **_kwargs: "Dict") -> "List":

        _, total_workers, logger = heartbeat_handler.live()
        for rse in rses_to_process:
            print(f'hello {rse}')

        rses_with_more_work = [rse for rse in rses_to_process if False]  # @TODO fill for rses with updated storage dumps
        return rses_with_more_work

    must_sleep = True

    _, total_workers, logger = heartbeat_handler.live()
    logger(logging.INFO, 'Auditor2 started')

    # # try to get auto exclude parameters from the config table. Otherwise use CLI parameters.
    # auto_exclude_threshold = config_get('reaper', 'auto_exclude_threshold', default=auto_exclude_threshold, raise_exception=False)
    # auto_exclude_timeout = config_get('reaper', 'auto_exclude_timeout', default=auto_exclude_timeout, raise_exception=False)

    rses_to_process = [RseData(id_=rse['id'], name=rse['rse'], columns=rse) for rse in get_rses_to_process(rses, include_rses, exclude_rses, vos)]
    if not rses_to_process:
        logger(logging.ERROR, 'Auditor2: No RSEs found. Will sleep for X seconds')
        return must_sleep

    while rses_to_process:
        rses_to_process = _run_once(
            rses_to_process=rses_to_process,
            heartbeat_handler=heartbeat_handler,
        )

    if rses_to_process:
        # There is still more work to be performed.
        # Inform the calling context that it must call auditor2 again (on the full list of rses)
        must_sleep = False

    return must_sleep


def stop(signum: "Optional[Any]" = None, frame: "Optional[Any]" = None) -> None:
    """
    Graceful exit.
    """
    GRACEFUL_STOP.set()


# @TODO double-check the type of 'vos'
def run(rses: "List[str]", include_rses: str, exclude_rses: str, vos: "List[str]", once: bool = False, sleep_time: int = 60, threads: int = 1) -> None:
    """
    Starts up the auditor2 threads.

    :param threads:                The total number of workers.
    :param once:                   If True, only runs one iteration of the main loop.
    :param sleep_time:             Time between two cycles.
    :param rses:                   List of RSEs the auditor2 should work against.
                                   If empty, it considers all RSEs.
    :param include_rses:           RSE expression to include RSEs.
    :param exclude_rses:           RSE expression to exclude RSEs.
    :param vos:                    VOs on which to look for RSEs. Only used in multi-VO mode.
                                   If None, we either use all VOs if run from "def",
                                   or the current VO otherwise.
    """

    setup_logging()

    logging.log(logging.INFO, 'main: starting processes')
    rses_to_process = get_rses_to_process(rses, include_rses, exclude_rses, vos)
    if not rses_to_process:
        logging.log(logging.ERROR, 'Auditor2: No RSEs found. Exiting.')
        return

    logging.log(logging.INFO, 'starting auditor2 threads')
    threads_list = [threading.Thread(target=auditor2, kwargs={
        'once': once,
        'sleep_time': sleep_time,
        'rses': rses,
        'include_rses': include_rses,
        'exclude_rses': exclude_rses,
        'vos': vos
    }) for _ in range(0, threads)]

    for thread in threads_list:
        thread.start()

    logging.log(logging.INFO, 'waiting for interrupts')

    # Interruptible joins require a timeout.
    while threads_list:
        threads_list = [thread.join(timeout=3.14) for thread in threads_list if thread and thread.is_alive()]

    return
