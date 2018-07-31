# -*- coding: utf8 -*-
from threading import Semaphore, Thread
from functools import partial
import typing as _t
import json
from contextlib import contextmanager
from datetime import datetime

import asyncio as aio
from aiohttp import web

from .kafka_reader import kafka_consumer_prepare, kafka_record_to_dict
from .websocket import stream_to_websocket


#===============================================================================
# UTIL
#===============================================================================
async def asyncmap(func, ait :_t.Union[_t.AsyncIterable, _t.Coroutine]):
    if isinstance(ait, _t.Coroutine):
        ait = await ait
    assert isinstance(ait, _t.AsyncIterable)
    ##
    async for x in ait:
        yield func(x)


#===============================================================================
# KAFKA WS READER & RUNNER
#===============================================================================
def kafka_websocket_reader_main(kafka_brokers, wss = ("0.0.0.0", 8080), *, loop, _ctx :_t.List = None, _ctx_sem = None):
    """Self-contained aiohttp.web.Application-based routine (app-main)"""
    app = web.Application(loop = loop)
    # --
    if not _ctx is None:
        _ctx.append(app)
        if isinstance(_ctx_sem, Semaphore):
            _ctx_sem.release()
    # --
    try:
        app.loop.add_signal_handler(2, lambda x: x.loop.run_until_complete(x.shutdown), app)
        app.loop.add_signal_handler(15, lambda x: x.loop.run_until_complete(x.shutdown), app)
    except NotImplementedError:
        pass
    # --
    app._router.add_route("GET", "/{topic:.+}",
          partial(stream_to_websocket,
                  lambda webreq, **webkw: asyncmap(
                        lambda kfk_rec: \
                                  json.dumps(kafka_record_to_dict(kfk_rec,
                                            compact_meta = bool(webkw.get("compact_meta", False)),
                                            coerce_value = webkw.get("coerce_value", "str"),
                                            coerce_key = webkw.get("coerce_key", "str"),
                                            kafka_timestamp = bool(webkw.get("kafka_timestamp", False)),
                                            kafka_topic = bool(webkw.get("kafka_topic", False)),),
                                   ensure_ascii = False),
                        kafka_consumer_prepare(
                          brokers = kafka_brokers,
                          topic = ("/"+webreq.path.lstrip("/")).split("/")[0:2][-1],
                          default_offspec = (list(filter(lambda o: (len(o) == 1) and o[0],
                                                    map(lambda s: s.split(":"),
                                                        webkw.get("offspec", "").split(","))))
                                                or
                                             [["+0"]])[0][-1],
                          parts_offspecs = { int(t) : o for t, o in
                                             (filter(lambda to: len(to) == 2 and all(to),
                                                map(lambda s: tuple(s.split(":")),
                                                    webkw.get("offspec", "").split(",")))) },

                          loop = app.loop),)))
    # --
    web.run_app(app, host = wss[0], port = wss[1])

@contextmanager
def kafka_websocket_reader_thread(kafka_brokers, wss :_t.Tuple[str, int] = None, *,
                                  runner = kafka_websocket_reader_main,
                                  auto_stop_runner = True,
                                  _io_threadfunc = lambda runner: runner(loop = aio.new_event_loop()),
                                  _io_use_main_thread = False, # HACK!
                                  **runner_kwargs):
    runner_args = [kafka_brokers]
    if (wss):
        runner_args.append(wss)

    # init
    app_ctx, _app_ctx_sem = [], Semaphore(0)
    app_th = Thread(target = _io_threadfunc or (lambda r: r(loop = aio.get_event_loop())),
                    args = [partial(runner, *runner_args, **runner_kwargs,
                                            _ctx = app_ctx, _ctx_sem = _app_ctx_sem)])

    if (_io_use_main_thread):
        app_th.run() ### FIXME: that would obviously block
    else:
        app_th.start()
        _app_ctx_sem.acquire()
    assert isinstance(app_ctx[0], web.Application)

    # running
    try:
        yield (app_ctx[0], app_th)

    finally:
    # stopping
        if (auto_stop_runner):
            print("[%s] Stopping..." % datetime.now())
            ev = app_ctx[0].loop
            ev.call_soon_threadsafe(ev.stop)
            # ev.shutdown_asyncgens()

            app_th.join()
            print("[%s] Stopped!" % datetime.now())


#===============================================================================
# EXPORTS
#===============================================================================
__all__ = (
    # high-level
    asyncmap.__name__,
    kafka_websocket_reader_main.__name__,
    kafka_websocket_reader_thread.__name__,

    # parts
    kafka_consumer_prepare.__name__, kafka_record_to_dict.__name__,
    stream_to_websocket.__name__,)
