#!/usr/bin/env python
# encoding: utf-8

import json
import sys


from pyspark import SparkContext
sc = SparkContext(appName="Exsto", master="local[*]")

from pyspark.sql import SQLContext, Row
sqlCtx = SQLContext(sc)

msg = sqlCtx.jsonFile("data").cache()
msg.registerTempTable("msg")


# Question: Who are the senders?

who = msg.map(lambda x: x.sender).distinct().zipWithUniqueId()
who.take(10)

whoMap = who.collectAsMap()

print "\nsenders"
print len(whoMap)


# Question: Who are the top K senders?

from operator import add

top_sender = msg.map(lambda x: (x.sender, 1,)).reduceByKey(add) \
 .map(lambda (a, b): (b, a)) \
 .sortByKey(0, 1) \
 .map(lambda (a, b): (b, a))

print "\ntop senders"
print top_sender.take(11)


# Question: Which are the top K conversations?

import itertools

def nitems (replier, senders):
  for sender, g in itertools.groupby(senders):
    yield len(list(g)), (replier, sender,)

senders = msg.map(lambda x: (x.id, x.sender,))
replies = msg.map(lambda x: (x.prev_thread, x.sender,))

convo = replies.join(senders).values() \
 .filter(lambda (a, b): a != b)

top_convo = convo.groupByKey() \
 .flatMap(lambda (a, b): list(nitems(a, b))) \
 .sortByKey(0)

print "\ntop convo"
print top_convo.take(10)


# Prepare for Sender/Reply Graph Analysis

edge = top_convo.map(lambda (a, b): (whoMap.get(b[0]), whoMap.get(b[1]), a,))
edgeSchema = edge.map(lambda p: Row(replier=long(p[0]), sender=long(p[1]), num=int(p[2])))
edgeTable = sqlCtx.inferSchema(edgeSchema)
edgeTable.saveAsParquetFile("reply_edge.parquet")

node = who.map(lambda (a, b): (b, a))
nodeSchema = node.map(lambda p: Row(id=long(p[0]), sender=p[1]))
nodeTable = sqlCtx.inferSchema(nodeSchema)
nodeTable.saveAsParquetFile("reply_node.parquet")


# Prepare for TextRank Analysis per paragraph

def map_graf_edges (x):
  j = json.loads(x)

  for pair in j["tile"]:
    n0 = int(pair[0])
    n1 = int(pair[1])

    if n0 > 0 and n1 > 0:
      yield (j["id"], n0, n1,)
      yield (j["id"], n1, n0,)

graf = sc.textFile("parsed").flatMap(map_graf_edges)
n = graf.count()
print "\ngraf edges", n

edgeSchema = graf.map(lambda p: Row(id=p[0], node0=int(p[1]), node1=int(p[2])))

edgeTable = sqlCtx.inferSchema(edgeSchema)
edgeTable.saveAsParquetFile("graf_edge.parquet")


def map_graf_nodes (x):
  j = json.loads(x)

  for word in j["graf"]:
    yield [j["id"]] + word

graf = sc.textFile("parsed").flatMap(map_graf_nodes)
n = graf.count()
print "\ngraf nodes", n

nodeSchema = graf.map(lambda p: Row(id=p[0], node_id=int(p[1]), raw=p[2], root=p[3], pos=p[4], keep=p[5], num=int(p[6])))

nodeTable = sqlCtx.inferSchema(nodeSchema)
nodeTable.saveAsParquetFile("graf_node.parquet")
