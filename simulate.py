import random

from matplotlib import pyplot as plt
from models import *
from concurrent.futures import ThreadPoolExecutor
from IPython import embed
from log_fmt import init_all
import time
from threading import Thread

import sys


CLIENT_SIZE = int(sys.argv[1])

s = Server()

init_all()

# buffer_size, batch_size, peer_limit, gen_interv, sendp_interv, sends_interv
clients = [Client(10, 3, 25, 20, 1, 1, 10, s, 3, False) for _ in range(CLIENT_SIZE)]

# connection_map = {c: {c2 for c2 in clients if random.randint(0, 2) and c2 != c} for c in clients}
# for v in connection_map.values():
#   random.shuffle(v)

# for c in clients:
#   c.connection_map = connection_map

# for c1 in clients:
#   for c2 in connection_map[c1]:
#     c1.add_peer(c2)

existing_clients = set()
fulled = set()
start_time = 0
duration = 0
for i, c in enumerate(clients):
  if i % 50 == 0:
    n = i // 50
    if n != 0:
      duration = duration * (n - 1) / n + (time.time() - start_time) / n
    print('\r{}/{} {}; Eta. {:.1f}s'.format(i, len(clients), len(existing_clients), ((len(clients) - i) // 50) * duration), end=' ')
    start_time = time.time()

  gonna_remove = []
  for j, e in enumerate(existing_clients):
    if c.join_group(e):
      break
    elif c.check_connect(e):
      c.add_peer(e)
    elif len(e.group_members()) >= e.grp_limit:
      gonna_remove.append(e)

  for e in gonna_remove:
    existing_clients.remove(e)

  existing_clients.add(c)

grps = dict()
for c in clients:
  grps.setdefault(c.gid, set()).add(c)

print('\ngroup count:', len(grps))
print('avg group size:', sum([len(v) for v in grps.values()]) / len(grps))

# embed()

# start = time.time()
#
# threads = []
# for c in clients:
#   t = Thread(target=c.send_vote, name=repr(c))
#   t.start()
#   threads.append(t)
#
# finished = False
# while not finished:
#   finished = all([not t.is_alive() for t in threads])
#
# print('Total time: {}'.format(time.time() - start))
send_through_group = 0

start_time = 0
duration = 0
for epoch in range(101):
  print('\r{}/{}'.format(epoch, 100), end=' ')
  if epoch % 10 == 0:
    for v in grps.values():
      node = set()
      for c in v:
        node.update(c.cold_data)
      send_through_group += len(node)

  for c in clients:
    c.update(epoch)

  for c in clients:
    c.commit()

  start_time = time.time()

print()

# for c in clients:
#   print('E', c, c.data, c.data._buffer)

# sr = []
# for v in s.data.values():
#     sr.extend(v)

# r = set(sr)

# print(len(set(filter(lambda x: not x.isFake, Client.dropped_data)) - r) / len(Client.all_data))
# print(sum([_.hop for _ in r]) / len(r))

print(send_through_group / s.recved_cnt)
print(send_through_group / len(Client.all_data))
