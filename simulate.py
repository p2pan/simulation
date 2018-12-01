import random

from matplotlib import pyplot as plt
from models import *
from IPython import embed


CLIENT_SIZE = 5

s = Server()

# buffer_size, batch_size, peer_limit, gen_interv, sendp_interv, sends_interv
clients = [Client(10, 1, 10, 1, 1, 10, s) for _ in range(CLIENT_SIZE)]

for c1 in clients:
  for c2 in clients:
    c1.add_peer(c2)

for epoch in range(11):
  # for c in clients:
  #   print(epoch, c, c.data, c.data._buffer)

  for c in clients:
    c.update(epoch)

  # for c in clients:
  #   print(epoch, c, c.data, c.data._buffer)

  for c in clients:
    c.commit()

  print()

for c in clients:
  print('E', c, c.data, c.data._buffer)

embed()
