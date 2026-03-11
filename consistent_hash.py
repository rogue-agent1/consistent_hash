#!/usr/bin/env python3
"""Consistent hashing with virtual nodes."""
import sys, hashlib, bisect
class ConsistentHash:
    def __init__(self,replicas=100): self.replicas=replicas; self.ring=[]; self.nodes={}
    def _hash(self,key): return int(hashlib.md5(key.encode()).hexdigest(),16)
    def add_node(self,node):
        for i in range(self.replicas):
            h=self._hash(f"{node}:{i}"); bisect.insort(self.ring,h); self.nodes[h]=node
    def remove_node(self,node):
        for i in range(self.replicas):
            h=self._hash(f"{node}:{i}"); self.ring.remove(h); del self.nodes[h]
    def get_node(self,key):
        h=self._hash(key); idx=bisect.bisect(self.ring,h)%len(self.ring)
        return self.nodes[self.ring[idx]]
ch=ConsistentHash(replicas=50)
for s in ['server-1','server-2','server-3']: ch.add_node(s)
keys=['user:1','user:2','user:3','photo:1','photo:2','session:abc','session:def']
print("Before removal:")
dist=collections.Counter()
import collections
for k in keys: node=ch.get_node(k); dist[node]+=1; print(f"  {k:15s} → {node}")
ch.remove_node('server-2')
print("\nAfter removing server-2:")
moved=0
for k in keys:
    node=ch.get_node(k); print(f"  {k:15s} → {node}")
