import sys
import pso
import subprocess

if len(sys.argv) != 3:
    # main process
    coord_name = pso.init()
    pso.root().requests = [{ 'idx': i, 'request': f'request {i}' }
        for i in range(10)]
    pso.root().responses = [None]*10
    workers = [subprocess.Popen([sys.executable, sys.argv[0], coord_name, str(req['idx'])])
        for req in pso.root().requests]
    statuses = [w.wait(5) for w in workers]
    for (idx, response) in pso.root().responses:
        print((idx, response))
else:
    # worker process
    pso.connect(sys.argv[1])
    myindex = int(sys.argv[2])
    myrequest = pso.root().requests[myindex]
    import time, random
    time.sleep(random.randrange(2)) # imitate some long work
    pso.root().responses[myindex] =
        (myindex, f'response {myindex} for {myrequest["request"]}')
