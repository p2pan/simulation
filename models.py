import numpy as np
import random
import uuid
import time
import logging

from dataclasses import dataclass
from itertools import count
from collections.abc import Sequence
from uuid import uuid1
from hashlib import sha256
from copy import deepcopy


class DataRecord:
  _cnt = count(0)
  __slots__ = ['isFake', 'id', 'hop']

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
  recved_cnt = 0

  def recv(self, data, epoch):
    self.recved_cnt += len(data)
    # if epoch in self.data:
    #   self.data[epoch].extend(data)
    # else:
    #   self.data[epoch] = []


class EpochBuffer(Sequence):
  def __init__(self, size: int, data: list = None, limit=True):
    self.size = size
    self._data = [] if not data else data
    self._buffer = []
    self._limit = limit
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
    if len(self._data) > self.size and self._limit:
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

  def clear(self):
    self.flush()
    self._data.clear()

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
  _cnt = count(1)

  __slots__ = ['data', 'cold_data', 'peers', 'grp_limit', 'buffer_size', 'duplicated', 'batch_size', 'peer_limit', 
    'gen_interv', 'sendp_interv', 'sends_interv', 'server', 'id', 'gid', 'old_gid', 'recved_hash', 'recved_str', 'is_collector', 'connection_map']

  def __init__(self, buffer_size: int, batch_size: int, peer_limit: int, grp_limit: int, gen_interv: int,
               sendp_interv: int, sends_interv: int, server: Server, duplicated=2, limit=True):
    self.data = EpochBuffer(buffer_size, [DataRecord(True) for _ in range(buffer_size)], True)
    self.cold_data = EpochBuffer(0, [DataRecord(True) for _ in range(buffer_size)], False)
    self.peers = []

    self.grp_limit = grp_limit
    self.buffer_size = buffer_size
    self.duplicated = duplicated
    self.batch_size = batch_size
    self.peer_limit = peer_limit
    self.gen_interv = gen_interv
    self.sendp_interv = sendp_interv
    self.sends_interv = sends_interv
    self.server = server
    self.id = next(self._cnt)
    self.gid = self.id
    self.old_gid = self.gid
    self.recved_hash = dict()
    self.recved_str = dict()
    self.is_collector = False
    self.connection_map = None

  def send2peers(self):
    bat = min(self.batch_size, len(self.data))
    selected = deepcopy(random.sample(self.data, bat))
    selected = random.sample(self.data, bat)
    for s in selected:
      # s.hop += 1
      self.data.remove(s)

    dup = min(self.duplicated, len(self.peers))
    selected_peer = random.sample(self.peers, dup)
    # print('send', selected, [s.hop for s in selected], 'from', self, 'to', selected_peer)
    for p in selected_peer:
      p.recv(selected)

  def recv(self, recs):
    self.cold_data.extend(recs)

  def gen_record(self):
    d = DataRecord(False)
    self.all_data.add(d)
    # self.cold_data.extend_no_wait(self.data.append_no_wait(d))
    self.data.append_no_wait(d)

  def send2server(self, epoch):
    # self.server.recv(self.data, epoch)
    self.server.recv(self.cold_data, epoch)
    self.cold_data.clear()

  def join_group(self, grp_mem):
    all_members = grp_mem.group_members()
    # print(grp_mem.gid, self, grp_mem, len(all_members))
    joinable = all([self.check_connect(m) for m in all_members]) and len(all_members) < self.grp_limit
    if joinable:
      self.gid = grp_mem.gid
      for m in all_members:
        m.accept_peer_to_group(self)
        self.accept_peer_to_group(m)
      return self.gid
    else:
      return False

  def exit_group(self):
    for m in self.group_members():
      m.remove_peer(self)
    self.gid = self.old_gid

  def accept_peer_to_group(self, peer):
    if len(self.peers) == self.peer_limit:
      try:
        unfortunate_peer = next(filter(lambda x: x.gid != self.gid, self.peers))
        self.peers.remove(unfortunate_peer)
      except StopIteration:
        pass
        # print(self.gid, len(self.peers), [p.gid for p in self.peers])

    self.peers.append(peer)

  def remove_peer(self, peer):
    pass

  def check_connect(self, peer):
    return random.randint(0, 2) > 0

    if not self.connection_map:
      raise ValueError("Connection map hasn't been set yet!")

    return peer in self.connection_map[self] and self in self.connection_map[peer]

  def add_peer(self, peer):
    if peer in self.peers or peer == self:
      return None

    if len(self.peers) >= self.peer_limit:
      return False
    else:
      self.peers.append(peer)

    return peer

  def group_members(self):
    return list(filter(lambda x: x.gid == self.gid, self.peers)) + [self, ]

  def update(self, epoch, gen=True):
    if epoch % self.sends_interv == 0:
      self.send2server(epoch)

    if epoch % self.gen_interv == 0 and gen:
      self.gen_record()

    if epoch % self.sendp_interv == 0:
      self.send2peers()

  def commit(self):
    self.dropped_data.update(self.data.commit())
    self.cold_data.commit()
    self.data.extend_no_wait(self.cold_data[:self.buffer_size - len(self.data)])
    self.cold_data.delete(self.buffer_size - len(self.data))

  def send_vote(self):
    self.random_str = repr(uuid1())
    str_hash = sha256()
    str_hash.update(self.random_str.encode('utf8'))
    self.locked_hash = str_hash.hexdigest()
    for p in self.peers:
      time.sleep(random.randint(10, 200) / 1000)
      # logging.info('stage1: {} send {} to {}'.format(self, self.locked_hash, p))
      p.recv_vote(self, self.locked_hash, True)

    fin = False
    while not fin:
      fin = all([p in self.recved_hash for p in self.peers])
      time.sleep(0.5)

    for p in self.peers:
      time.sleep(random.randint(10, 200) / 1000)
      # logging.info('stage2: {} send {} to {}'.format(self, self.random_str, p))
      p.recv_vote(self, self.random_str, False)

    fin = False
    while not fin:
      fin = all([p in self.recved_str for p in self.peers])
      time.sleep(0.5)

    # order and validate
    self.recved_hash[self] = self.locked_hash
    self.recved_str[self] = self.random_str
    sorted_clients = sorted(self.recved_hash.keys(), key=lambda x: self.recved_hash[x])
    sorted_all = {c: (self.recved_hash[c], self.recved_str[c]) for c in sorted_clients}
    dishonest_clients = []
    for p, pair in sorted_all.items():
      locked, seed = pair
      sha = sha256()
      sha.update(seed.encode('utf8'))
      if locked != sha.hexdigest():
        dishonest_clients.append(p)

    for d in dishonest_clients:
      sorted_all.pop(d)
      sorted_clients.remove(d)

    combined = ''.join([pair[1] for p, pair in sorted_all.items()])
    sha = sha256()
    sha.update(combined.encode('utf8'))
    index = int(sha.hexdigest(), base=16) % len(sorted_all)

    sha2 = sha256()
    sha2.update(sha.hexdigest().encode('utf8'))
    index2 = int(sha2.hexdigest(), base=16) % len(sorted_all)

    if index2 == index:
      index2 = (index + 1) % len(sorted_all)

    # logging.warning('{} and {} Win!'.format(sorted_clients[index], sorted_clients[index2]))
    self.is_collector = sorted_clients[index] == self
    self.is_collector = sorted_clients[index2] == self
    return sorted_clients[index], sorted_clients[index2]

  def recv_vote(self, p, val, is_hash=True):
    if is_hash:
      self.recved_hash[p] = val
    else:
      self.recved_str[p] = val

  def merge(self, new_peer, connection_map):
    if new_peer in connection_map[self]:
      pass

  def __str__(self):
    return f'C{self.id}'

  def __repr__(self):
    return f'C{self.id}'
