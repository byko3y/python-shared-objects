#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pso
import sys, os
import time
import traceback

def log_msg(msg):
    print('{}. {}'.format(os.getpid(), msg))

class SomeSharedClass(pso.ShmObject):
    field = 2
    def inc_field(self, val):
        self.field += val

def run_child():
    root = pso.root()
    stat = root.stat
    promise = root.signal
    print('stat', stat)
    print('signal', root.signal)
    promise.wait(0)
    log_msg("Wait successfull")

    log_msg("Shared object field = {}".format(root.obj.field))
    root.obj.inc_field(5)
    log_msg("Shared object field + 5 = {}".format(root.obj.field))

    log_msg('Starting counter is {}'.format(root.counter))
    cycle_count = 0
    attempt_count = 0
    for i in range(root.cycle_count):
        with transaction:
            attempt_count += 1
            root.counter += 1
            # Uncommend this and then try to find the error line number:
            # stat.sum += counter
            stat.sum += root.counter
            stat.count = root.counter
            cycle_count += 1

    log_msg('Cycle count {}, attempted cycles {}'.format(cycle_count, attempt_count))

    log_msg('Starting counter is {}'.format(root.counter2))
    cycle_count2 = 0
    attempt_count2 = 0
    stat2 = root.stat2
    root.signal2.wait(0)
    for i in range(root.cycle_count):
        attempt_count2 += 1
        root.counter2 += 1
        # Uncommend this and then try to find the error line number:
        # stat.sum += counter
        stat2.sum += root.counter2
        stat2.count = root.counter2
        cycle_count2 += 1
    log_msg('Non-transacted cycle count {}, attempted cycles {}'.format(cycle_count2, attempt_count2))

def run_parent():
    root = pso.root()

    root.obj = SomeSharedClass()
    log_msg("Shared object field = {}".format(root.obj.field))
    root.obj.inc_field(14)
    log_msg("Shared object field + 14 = {}".format(root.obj.field))

    stat = root.stat
    log_msg('Sending promise fulfillment to child')
    promise = root.signal
    promise.signal(True)
    log_msg('Starting counter is {}'.format(root.counter))
    # pso.transaction_start()
    # root['counter'] = root['counter'] + 1
    # pso.transaction_commit()
    cycle_count = 0
    attempt_count = 0
    for i in range(root.cycle_count):
        with transaction:
            attempt_count += 1
            root.counter += 1
            stat.sum += root.counter
            stat.count = root.counter
            cycle_count += 1
    print('root.__dict__: ', len(root.__dict__))
    log_msg('Cycle count {}, attempted cycles {}'.format(cycle_count, attempt_count))

    log_msg('Sending second promise fulfillment to child')
    root.signal2.signal(True)

    log_msg('Non-transacted starting counter is {}'.format(root.counter2))
    cycle_count2 = 0
    attempt_count2 = 0
    stat2 = root.stat2
    for i in range(root.cycle_count):
        attempt_count2 += 1
        root.counter2 += 1
        stat2.sum += root.counter2
        stat2.count = root.counter2
        cycle_count2 += 1

    log_msg('Non-transacted cycle count {}, attempted cycles {}'.format(cycle_count2, attempt_count2))

    log_msg('new pso.ShmObject(): ' + str(pso.ShmObject() ) )
    repr(root.counter)
    value = pso.ShmValue('teст')
    value2 = pso.ShmValue(b'test2')
    value3 = pso.ShmValue(True)
    value4 = pso.ShmValue(None)
    log_msg('value: ' + repr(value) + ';\n  value2: ' + repr(value2) + ';\n  value3: ' + repr(value3) + ';\n  value4: ' + repr(value4) )
    l = pso.ShmList()
    l.append(value3)
    log_msg('new pso.ShmList(): ' + repr(l) )
    l.append(value4)
    log_msg('appended 1 to ShmList: ' + repr(l) )
    l.append(value2)
    l.append(value)

    with transaction:
        itr = iter(l)
        log_msg('ShmListIter: ' + repr(itr) )
        log_msg('First item: ' + repr(next(itr)) )
        next(itr)
        next(itr)
        log_msg('Fourth item: ' + repr(next(itr)) )

    log_msg('Iterating ShmList:')
    with transaction:
        for item in l:
            print(item)

    log_msg('Converting ShmList to list:')
    native_list = list(l)
    for item in native_list:
        print(item)

    list2 = pso.ShmList([1, 2, 3])
    log_msg('Iterating another ShmList:')
    with transaction:
        for item in list2:
            print(item)

    pso.root().list3 = [1, 2, 3]
    list3 = pso.root().list3
    list3[0] = 'First'
    list3_correct = ['First', 2, 3]
    for i in range(3):
        if list3_correct[i] != list3[i]:
            raise Exception('list3_correct[{}] != list3[{}]: {} != {}'.format(i, i, list3_correct[i], list3[i]))

    log_msg('Iterating implicitly created ShmList:')
    with transaction:
        for item in list3:
            print(item)

    d = pso.ShmDict()
    print('Just empty dict:', d)
    d['first'] = 'first value'
    d['second'] = 'second value'
    d['third'] = 'third value'
    d['fourth'] = 'fourth value'
    d['fifth'] = 'fifth value'
    log_msg('Iterating ShmDict:')
    with transaction:
        for item in d:
            print(item)
    log_msg('ShmDict keys: {0!r}'.format(d.keys()))
    log_msg('Converting ShmDict to dict:')
    native_dict = dict(d)
    with transaction:
        for item in native_dict:
            print(item)

    d2 = pso.ShmDict({'a': 2})
    log_msg('ShmDict keys: {0!r}'.format(d2.keys()))
    log_msg('      values: {0!r}'.format(d2.values()))

    print('dict with value:', d['first'])
    del d['first']
    try:
        print('dict empty again:', d['first'])
    except:
        traceback.print_stack()

    log_msg('Iterating tuple:')
    tpl = pso.ShmTuple((1, 2, 3,))
    with transaction:
        for item in tpl:
            print(item)

    log_msg("Final shared object field = {}".format(root.obj.field))
    root.obj.inc_field(3)
    log_msg("Final shared object field + 3 = {}".format(root.obj.field))

    # test late iterator deletion
    # del itr
    del l

    log_msg("Test complete")

