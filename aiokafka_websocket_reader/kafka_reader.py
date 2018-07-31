# -*- coding: utf8 -*-
import asyncio as aio
import traceback
import json

from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRecord


#===============================================================================
# HELPERS
#===============================================================================
def kafka_record_to_dict(rec :ConsumerRecord, *,
                         coerce_value = "str", #:"json"|"str"|None
                         coerce_key = "str", #:"str"|None
                         compact_meta = False,
                         kafka_timestamp = False, kafka_topic = False) -> dict:
    """
    :param coerce_value: "json"|"str"|None - try to coerce the value type, or not (if None)

    TODO: optimize, use predefined class
    """
    # meta (top-level keys)
    msg = dict()
    if (compact_meta):
        _k_v, _k_k, _k_tm = "V", "K", "tm"
        msg["_"] = ((rec.topic + ":") if kafka_topic else "") \
                   + ("%d:%d" % (rec.partition, rec.offset))
    else:
        _k_v, _k_k, _k_tm = "message", "key", "timestamp"
        msg["offset"] = rec.offset
        msg["partition"] = rec.partition
        if (kafka_topic):
            msg["topic"] = rec.topic

    # value processing/coercion
    value = rec.value
    if (coerce_value == "json"):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            pass
    elif (coerce_value == "str") and isinstance(value, bytes):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            traceback.print_exc()
            value = value.decode(errors = "ignore")
    elif coerce_value:
        raise ValueError("Invalid argument for 'coerce_value' param: %s"%coerce_value, coerce_value)
    # --
    msg[_k_v] = value

    # key
    if (not rec.key is None):
        key = rec.key
        if isinstance(key, bytes) and (coerce_key == "str"):
            key = key.decode(errors = "ignore")
        # --
        msg[_k_k] = key

    # misc metadata
    if (kafka_timestamp):
        msg[_k_tm] = rec.timestamp

    return msg


#===============================================================================
# KAFKA
#===============================================================================
async def kafka_consumer_prepare(brokers, topic :str,
                                 default_offspec = "-0", parts_offspecs :dict = {},
                                 **consumer_kw):
    """
    :param brokers: bootstrap servers
    :param str topic: topic to consume from (... to assign with)
    :param dict parts_offspecs: (partition-id -> offset-spec), where offset-spec: "[+-]<integer>"

    :return: AIOKafkaConsumer
    """
    if isinstance(brokers, str):
        brokers = brokers.split(",")
    brokers = list(brokers)
    assert brokers, "Empty list of kafka brokers"

    # connect & fetch metadata
    c = AIOKafkaConsumer(bootstrap_servers = ",".join(brokers),
                         enable_auto_commit = False, group_id = None, **consumer_kw)
    await c.start()

    all_topics = await c.topics()
    assert topic in all_topics, str((topic, all_topics))
    topic_parts = c.partitions_for_topic(topic)

    # perform seeking for each partition
    # --
    tp_ofssp = {TopicPartition(topic, _part_id) : parts_offspecs.get(_part_id, default_offspec)
                for _part_id in topic_parts}
    c.assign(list(tp_ofssp))
    from pprint import pprint; pprint(tp_ofssp); pprint(parts_offspecs)
    await c.start() # NB: "restart" is necessary for the assignment to propagate
                    #     across all the aiokafka's (sic!) abstraction layers
    assert c.assignment().union(tp_ofssp) == tp_ofssp.keys()

    # --
    tp_ranges = {tp : [beg, None]
                 for tp, beg in (await c.beginning_offsets(list(tp_ofssp))).items()}
    for tp, end in (await c.end_offsets(list(tp_ofssp))).items():
        tp_ranges[tp][-1] = end

    for tp, offspec in tp_ofssp.items():
        beg, end = tp_ranges[tp]
        offs = None #:int
        # ---
        if isinstance(offspec, int):
            offs = int(offspec)
        elif isinstance(offspec, str):
            offs = int(offspec[1:] or offspec)
            if (len(offspec) > 1):
                if offspec[0] in ("+", " "):
                    offs += beg
                elif offspec[0] == "-":
                    offs = end - offs
                else:
                    try:
                        offs = int(offspec) # trivial case: string-encoded int
                    except ValueError:
                        raise ValueError("Invalid partition offset specifier: " 
                                     "unknown prefix '%s' (0x%.2x)" % (offspec[0], ord(offspec[0])), offspec)
        else:
            raise ValueError("Invalid partition offset specifier: "%offspec, offspec, type(offspec))
        # ---
        if (offs < beg): offs = beg
        elif (offs > end): offs = end
        # ---
        assert isinstance(offs, int)
        c.seek(tp, offs)
        #print(">> seek(%s:%d, %9d)" % (tp.topic, tp.partition, offs))

    # --
    return c

async def kafka_read_n(c :AIOKafkaConsumer, n :int):
    r = []
    async for el in c:
        r.append(el)
        n -= 1
        if (n <= 0): break
    return r


#===============================================================================
# TEST
#===============================================================================
async def _test_consumer():
    return await kafka_consumer_prepare(
        brokers = sys.argv[1],
        topic = sys.argv[2], default_offspec = "+0",
        loop = aio.get_event_loop())

async def _test1():
    c = await _test_consumer()
    try:
        return await c.getone()
    finally:
        await c.stop()

async def _test2(msgs_num = 100):
    c = await _test_consumer()
    try:
        return await kafka_read_n(c, msgs_num)
    finally:
        await c.stop()


if (__name__ == "__main__"):
    import sys
    from pprint import pprint
    from functools import partial

    ev = aio.get_event_loop()
    msgs = list(map(
        partial(kafka_record_to_dict, compact_meta = True, coerce_value = "str"),
        ev.run_until_complete(_test2(5))))

    print(json.dumps(msgs, ensure_ascii = False, indent = 2))


