# -*- coding: utf8 -*-
import sys, os
from . import kafka_websocket_reader_thread


#===============================================================================
# USAGE
#===============================================================================
def usage():
    print("Usage: %s <kafka-broker>:<port>[,...] <websocket-host>:<port>"%sys.argv[0])


#===============================================================================
# MAIN
#===============================================================================
try:
    _main_args = [sys.argv[1]] # kafka broker
except IndexError:
    usage()
    sys.exit(1)
if (len(sys.argv) > 2):
    wss_hostport = tuple(map(str.strip, sys.argv[2].split(":"))) # websocket host:port
    if len(wss_hostport) < 2:
        try:
            _port = int(wss_hostport[0])
        except ValueError:
            print("{ERR} Invalid bind address: %s" % (wss_hostport,))
            usage()
            sys.exit(1)
        wss_hostport = ("0.0.0.0", _port)
    _main_args.append(wss_hostport)

# running
with kafka_websocket_reader_thread(*_main_args) as _loop_rdrth:
    # -- Windows NT (crippled) terminal, no signals...
    if (os.name == "nt"):
        while (True):
            try:
                input("<EOF for stop>")
            except EOFError:
                break
            except:
                pass
    # -- POSIX terminal, with signals!
    else:
        rdrth = _loop_rdrth[1]
        rdrth.join()
