#encoding: utf-8

import sys, os
import time
import pso

def log_msg(msg):
    print('{}. {}'.format(os.getpid(), msg))

if len(sys.argv) > 1:
    print('2. my pid {}'.format(os.getpid()))
    status = pso.connect(sys.argv[1])
    print('2. Connected to coordinator {}'.format(status))
    pso.set_random_flinch(True)
    import test1
    pso.transient_start()
    test1.run_child()
else:
    import subprocess
    print('1. my pid {}'.format(os.getpid()))
    coord_name = pso.init()
    pso.set_random_flinch(True)
    pso.set_debug_reclaimer(True)
    pso.transient_start()
    pso.pso(2)
    p = pso.ShmPromise()
    root = pso.get_root()
    root.signal = p
    root.signal2 = pso.ShmPromise()
    root.counter = 0
    root.counter2 = 0
    root.cycle_count = 20*1000
    stat_dict = pso.ShmObject()
    root.stat = stat_dict
    stat_dict.sum = 0
    root.stat2 = pso.ShmObject()
    root.stat2.sum = 0

    mychild = subprocess.Popen([sys.executable, sys.argv[0], coord_name])
    with pso.Transient():
      time.sleep(0.2)
      try:
        import test1
        test1.run_parent()
      finally:
        mychild.wait(13)

      log_msg('Final transacted counter is {}, sum is {} (expected {})'.format(pso.get_root().counter, pso.get_root().stat.sum, 800020000))
      log_msg('Final non-transacted counter is {}, sum is {} (expected {})'.format(root.counter2, root.stat2.sum, 800020000))

    log_msg("Sleeping");
    time.sleep(5) # trigger coordinator debug print


