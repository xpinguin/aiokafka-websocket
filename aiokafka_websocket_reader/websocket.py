# -*- coding: utf8 -*-
import typing as _t
from aiohttp import web
from urllib.parse import parse_qsl
import inspect


#===============================================================================
# WEBSOCKET / ASYNC-STREAM
#===============================================================================
async def stream_to_websocket(stream_ctor :_t.Callable[[web.Request], _t.AsyncIterable],
                              req :web.Request):
    """Drain stream into websocket channel"""
    # TODO:
    # [x/-] check and apply params
    # [-] propagate excs to HTTP layer.
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
            co = (ws.send_str(d) if isinstance(d, str) else ws.send_bytes(d))
            if co: # another aiohttp compat layer :(
                try:
                    await co
                except TypeError:
                    pass
            await ws.drain() # Q: shall we drain less frequently?
    finally:
        await ws.close()
        try:
            await stream.stop()
        except AttributeError:
            pass
