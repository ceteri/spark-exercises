# TextRank, based on:
# http://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf

from itertools import tee, izip
from nltk import stem
from text.blob import TextBlob as tb
from textblob_aptagger import PerceptronTagger
import nltk.data
import numpy as np
import sys


TOKENIZER = nltk.data.load('tokenizers/punkt/english.pickle')
TAGGER = PerceptronTagger()
STEMMER = stem.porter.PorterStemmer()


def pos_tag (s):
  """high-performance part-of-speech tagger"""
  global TAGGER
  return TAGGER.tag(s)


def wrap_words (pair):
  """wrap each (word, tag) pair as an object with fully indexed metadata"""
  global STEMMER
  index = pair[0]
  result = []
  for word, tag in pair[1]:
    word = word.lower()
    stem = STEMMER.stem(word)
    if stem == "":
      stem = word
    keep = tag in ('JJ', 'NN', 'NNS', 'NNP',)
    result.append({ "id": 0, "index": index, "stem": stem, "word": word, "tag": tag, "keep": keep })
    index += 1
  return result


######################################################################
## build a graph from raw text

TEXT = """
Compatibility of systems of linear constraints over the set of natural numbers. 
Criteria of compatibility of a system of linear Diophantine equations, strict
inequations, and nonstrict inequations are considered. Upper bounds for
components of a minimal set of solutions and algorithms of construction of
minimal generating sets of solutions for all types of systems are given. 
These criteria and the corresponding algorithms for constructing a minimal
supporting set of solutions can be used in solving all the considered types
systems and systems of mixed types.
"""

from pyspark import SparkContext
sc = SparkContext(appName="TextRank", master="local[*]")

sent = sc.parallelize(TOKENIZER.tokenize(TEXT)).map(pos_tag).cache()
sent.collect()

base = list(np.cumsum(np.array(sent.map(len).collect())))
base.insert(0, 0)
base.pop()
sent_length = sc.parallelize(base)
 
tagged_doc = sent_length.zip(sent).map(wrap_words)


######################################################################

from pyspark.sql import SQLContext, Row
sqlCtx = SQLContext(sc)

def enum_words (s):
  for word in s:
    yield word

words = tagged_doc.flatMap(enum_words)
pair_words = words.keyBy(lambda w: w["stem"])
uniq_words = words.map(lambda w: w["stem"]).distinct().zipWithUniqueId()

uniq = sc.broadcast(dict(uniq_words.collect()))


def id_words (pair):
  (key, val) = pair
  word = val[0]
  id = val[1]
  word["id"] = id
  return word

id_doc = pair_words.join(uniq_words).map(id_words)
id_words = id_doc.map(lambda w: (w["id"], w["index"], w["word"], w["stem"], w["tag"]))

wordSchema = id_words.map(lambda p: Row(id=long(p[0]), index=int(p[1]), word=p[2], stem=p[3], tag=p[4]))
wordTable = sqlCtx.inferSchema(wordSchema)

wordTable.registerTempTable("word")
wordTable.saveAsParquetFile("word.parquet")


######################################################################

def sliding_window (iterable, size):
  """apply a sliding window to produce 'size' tiles"""
  iters = tee(iterable, size)
  for i in xrange(1, size):
    for each in iters[i:]:
      next(each, None)
  return list(izip(*iters))


def keep_pair (pair):
  """filter the relevant linked word pairs"""
  return pair[0]["keep"] and pair[1]["keep"] and (pair[0]["word"] != pair[1]["word"])


def link_words (seq):
  """attempt to link words in a sentence"""
  return [ (seq[0], word) for word in seq[1:] ]


tiled = tagged_doc.flatMap(lambda s: sliding_window(s, 3)).flatMap(link_words).filter(keep_pair)

t0 = tiled.map(lambda l: (uniq.value[l[0]["stem"]], uniq.value[l[1]["stem"]],))
t1 = tiled.map(lambda l: (uniq.value[l[1]["stem"]], uniq.value[l[0]["stem"]],))

neighbors = t0.union(t1)

edgeSchema = neighbors.map(lambda p: Row(n0=long(p[0]), n1=long(p[1])))
edgeTable = sqlCtx.inferSchema(edgeSchema)

edgeTable.registerTempTable("edge")
edgeTable.saveAsParquetFile("edge.parquet")
