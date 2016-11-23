#! -*- coding:utf-8 -*-

import select

WATCHDOG_KQ_FILTER = select.KQ_FILTER_VNODE
WATCHDOG_KQ_EV_FLAGS = select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR
WATCHDOG_KQ_FFLAGS = (
    select.KQ_NOTE_DELETE |
    select.KQ_NOTE_WRITE |
    select.KQ_NOTE_EXTEND |
    select.KQ_NOTE_ATTRIB |
    select.KQ_NOTE_LINK |
    select.KQ_NOTE_RENAME |
    select.KQ_NOTE_REVOKE
)


