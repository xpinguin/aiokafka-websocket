# -*- coding: utf-8 -*-
"""
    Ensure compatibility, hot-fix buggy packages
"""
from typing import *
from aiokafka.consumer.subscription_state import SubscriptionState, SubscriptionType, ManualSubscription
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka import TopicPartition


#===============================================================================
# AIOKAFKA
#===============================================================================
# - FASCIST ASSERTION #1 -
class Compat_SubscriptionState(SubscriptionState):
    def assign_from_subscribed(self, assignment: Set[TopicPartition]):
        self._subscription._assign(assignment)
        self._notify_assignment_waiters()

    def assign_from_user(self, partitions: Set[TopicPartition]):
        self._set_subscription_type(SubscriptionType.USER_ASSIGNED)
        self._change_subscription(Compat_ManualSubscription(partitions, loop = self._loop))
        self._notify_assignment_waiters()

# - FASCIST ASSERTION #2 -
class Compat_ManualSubscription(ManualSubscription):
    def _assign(self, topic_partitions: Set[TopicPartition]):
        pass

class Compat_AIOKafkaConsumer(AIOKafkaConsumer):
    def __init__(self, *topics, **opts):
        super().__init__(*topics, **opts)
        self._subscription = Compat_SubscriptionState(loop = opts.get("loop") or self._loop)
# -


#===============================================================================
# EXPORTS
#===============================================================================
__all__ = (
    Compat_SubscriptionState.__name__, Compat_ManualSubscription.__name__,
    Compat_AIOKafkaConsumer.__name__,
)
