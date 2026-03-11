#!/usr/bin/env python3
"""Consistent hashing — minimal key remapping when nodes change."""
import sys, hashlib, bisect

class ConsistentHash:
    def __init__(self, replicas=150):
        self.replicas = replicas; self.ring = []; self.nodes = {}
    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2**32)
    def add(self, node):
        for i in range(self.replicas):
            h = self._hash(f"{node}:{i}")
            bisect.insort(self.ring, h)
            self.nodes[h] = node
    def remove(self, node):
        self.ring = [h for h in self.ring if self.nodes.get(h) != node]
        self.nodes = {h: n for h, n in self.nodes.items() if n != node}
    def get(self, key):
        if not self.ring: return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        return self.nodes[self.ring[idx]]
    def distribution(self, keys):
        d = {}
        for k in keys:
            n = self.get(k)
            d[n] = d.get(n, 0) + 1
        return d

if __name__ == "__main__":
    ch = ConsistentHash()
    nodes = ["server-A", "server-B", "server-C"]
    for n in nodes: ch.add(n)
    keys = [f"key-{i}" for i in range(10000)]
    before = {k: ch.get(k) for k in keys}
    dist = ch.distribution(keys)
    print("Distribution (3 nodes, 10000 keys):")
    for n, c in sorted(dist.items()):
        print(f"  {n}: {c} ({c/100:.1f}%)")
    ch.add("server-D")
    moved = sum(1 for k in keys if ch.get(k) != before[k])
    print(f"\nAdded server-D: {moved} keys moved ({moved/100:.1f}%)")
    dist2 = ch.distribution(keys)
    for n, c in sorted(dist2.items()):
        print(f"  {n}: {c} ({c/100:.1f}%)")
