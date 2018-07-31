# -*- coding: utf-8 -*-
"""
    Ensure compatibility, hot-fix buggy packages
"""
from typing import *
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka import TopicPartition


#===============================================================================
# AIOKAFKA
#===============================================================================
# - FASCIST ASSERTION #1 -
class Compat_SubscriptionState(SubscriptionState):
    def assign_from_subscribed(self, assignment: Set[TopicPartition]):
        #assert self._subscription_type in [
        #   SubscriptionType.AUTO_PATTERN, SubscriptionType.AUTO_TOPICS]

        self._subscription._assign(assignment)
        self._notify_assignment_waiters()

class Compat_AIOKafkaConsumer(AIOKafkaConsumer):
    def __init__(self, *topics, **opts):
        super().__init__(*topics, **opts)
        self._subscription = Compat_SubscriptionState(loop = opts.get("loop") or self._loop)
# -


#===============================================================================
# EXPORTS
#===============================================================================
__all__ = (
    Compat_SubscriptionState.__name__,
    Compat_AIOKafkaConsumer.__name__,
)
