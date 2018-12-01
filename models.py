import numpy as np
import random
import uuid

from dataclasses import dataclass
from itertools import count
from collections.abc import Sequence
from copy import deepcopy


class DataRecord:
  _cnt = count(0)

  def __init__(self, is_fake):
    self.isFake = is_fake
    self.id = next(self._cnt)
    self.hop = 0

  def __eq__(self, other):
    return self.id == other.id

  def __str__(self):
    return ('F' if self.isFake else 'T') + f'{self.id}'

  def __repr__(self):
    return self.__str__()

  def __hash__(self):
    return hash(self.id)


@dataclass
class Server:
  data = {}

  def recv(self, data, epoch):
    if epoch in self.data:
      self.data[epoch].update(data)
    else:
      self.data[epoch] = set(data)


class EpochBuffer(Sequence):
  def __init__(self, size: int, data: list = None):
    self.size = size
    self._data = [] if not data else data
    self._buffer = []
    self._trim()

  def flush(self):
    self._buffer.clear()

  def commit(self):
    self._data.extend(self._buffer)
    self.flush()
    return self._trim()

  def delete(self, size):
    ret = self._data[:size]
    self._data = self._data[size:]
    return ret

  def _trim(self):
    if len(self._data) > self.size:
      return self.delete(len(self._data) - self.size)
    else:
      return []

  def remove(self, item):
    self._data.remove(item)

  def append(self, item):
    self._buffer.append(item)

  def append_no_wait(self, item):
    self._data.append(item)
    return self._trim()

  def extend(self, items):
    self._buffer.extend(items)

  def extend_no_wait(self, items):
    self._data.extend(items)
    return self._trim()

  def __contains__(self, item):
    return item in self._data

  def __len__(self):
    return len(self._data)

  def __getitem__(self, item):
    return self._data[item]

  def __iter__(self):
    for _ in self._data:
      yield _

  def __repr__(self):
    return f'{type(self).__name__}({self.size}, {self._data})'

  def __str__(self):
    return str(self._data)


class Client:
  all_data = set()
  dropped_data = set()
  _cnt = count(0)

  def __init__(self, buffer_size: int, batch_size: int, peer_limit: int, gen_interv: int,
               sendp_interv: int, sends_interv: int, server: Server):
    self.data = EpochBuffer(buffer_size, [DataRecord(True) for _ in range(buffer_size)])
    self.batch_size = batch_size
    self.peer_limit = peer_limit
    self.peers = []
    self.gen_interv = gen_interv
    self.sendp_interv = sendp_interv
    self.sends_interv = sends_interv
    self.server = server
    self.id = next(self._cnt)

  def send2peers(self):
    selected = random.sample(self.data, self.batch_size)
    for s in selected:
      s.hop += 1
      self.data.remove(s)

    print('send', selected, 'from', self, 'to', self.peers)
    for p in self.peers:
      p.recv(selected)

  def recv(self, recs):
    self.data.extend(recs)

  def gen_record(self):
    d = DataRecord(False)
    self.all_data.add(d)
    self.dropped_data.update(self.data.append_no_wait(d))

  def send2server(self, epoch):
    self.server.recv(self.data, epoch)

  def add_peer(self, peer):
    if peer in self.peers or peer == self:
      return None

    self.peers.append(peer)
    if len(self.peers) > self.peer_limit:
      self.peers.pop(0)

    return peer

  def update(self, epoch, gen=True):
    if epoch % self.sends_interv == 0:
      self.send2server(epoch)

    if epoch % self.gen_interv == 0 and gen:
      self.gen_record()

    if epoch % self.sendp_interv == 0:
      self.send2peers()

  def commit(self):
    self.dropped_data.update(self.data.commit())

  def __str__(self):
    return f'C{self.id}'

  def __repr__(self):
    return f'C{self.id}'
