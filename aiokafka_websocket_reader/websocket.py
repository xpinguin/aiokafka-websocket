# -*- coding: utf8 -*-
import typing as _t
from aiohttp import web
from urllib.parse import parse_qsl

#===============================================================================
# WEBSOCKET / ASYNC-STREAM
#===============================================================================
async def stream_to_websocket(stream_ctor :_t.Callable[[web.Request], _t.AsyncIterable],
                              req :web.Request):
    """Drain stream into websocket channel"""
    # TODO:
    # - check and apply params
    # - propagate excs to HTTP layer.
    req_GET = dict(parse_qsl(req.query_string, keep_blank_values = True))
    stream = stream_ctor(req, **req_GET)

    # upgrade to WS
    ws = web.WebSocketResponse()
    await ws.prepare(req)

    # drain stream
    try:
        async for d in stream:
            if not isinstance(d, (str, bytes)):
                d = str(d)
            if isinstance(d, str):
                ws.send_str(d)
            else:
                ws.send_bytes(d)
            # Q: shall we drain less frequently?
            await ws.drain()
    finally:
        await ws.close()
        try:
            await stream.stop()
        except AttributeError:
            pass
