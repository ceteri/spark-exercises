#!/usr/bin/env python
# encoding: utf-8

# TextRank, based on:
# http://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf

from itertools import tee, izip
from textblob import TextBlob
from textblob_aptagger import PerceptronTagger
import hashlib
import json
import re
import sys
import uuid

DEBUG = False # True

PAT_PUNCT = re.compile(r'^\W+$')
POS_KEEPS = ['v', 'n', 'j', 'r']
POS_LEMMA = ['v', 'n']
TAGGER = PerceptronTagger()
UNIQ_WORDS = { ".": 0 }


def get_word_id (root):
  """lookup/assign a unique identify for each word"""
  global UNIQ_WORDS

  # in practice, this should use a microservice via some robust
  # distributed cache, e.g., Cassandra, Redis, etc.

  if root not in UNIQ_WORDS:
    UNIQ_WORDS[root] = len(UNIQ_WORDS)

  return UNIQ_WORDS[root]


def sliding_window (iterable, size):
  """apply a sliding window to produce 'size' tiles"""
  iters = tee(iterable, size)

  for i in xrange(1, size):
    for each in iters[i:]:
      next(each, None)

  return list(izip(*iters))


def get_tiles (graf, size=3):
  """generate word pairs for the TextRank graph"""
  for seq in sliding_window(graf, size):
    for word in seq[1:]:
      w0, w1 = (seq[0], word,)

      if w0[4] == w1[4] == 1:
        yield (w0[0], w1[0],)


def tag_doc (text):
  """parse and markup a document"""
  global DEBUG
  global POS_KEEPS, POS_LEMMA, PAT_PUNCT, TAGGER

  m = hashlib.sha1()
  i = 0
  doc = []
  tiles = []

  for s in TextBlob(text).sentences:
    graf = []
    doc.append(graf)

    pos = TAGGER.tag(str(s))
    p_idx = 0
    w_idx = 0

    while p_idx < len(pos):
      p = pos[p_idx]

      if (p[1] == "SYM") or PAT_PUNCT.match(p[0]):
        if (w_idx == len(s.words) - 1):
          w = p[0]
          t = '.'
        else:
          p_idx += 1
          continue
      else:
        w = s.words[w_idx]
        t = p[1].lower()[0]
        w_idx += 1

      if t in POS_LEMMA:
        l = str(w.singularize().lemmatize(t)).lower()
      elif t != '.':
        l = str(w).lower()
      else:
        l = w

      keep = 1 if t in POS_KEEPS else 0
      m.update(l)

      id = get_word_id(l) if keep == 1 else 0
      graf.append((id, w, l, p[1], keep, i,))

      i += 1
      p_idx += 1

    # tile the pairs for TextRank
    tiles.append(list(get_tiles(graf)))

  meta = {
    "uuid": str(uuid.uuid4()).replace('-', ''),
    "len": i,
    "lang": s.detect_language(),
    "sha1": m.hexdigest(),
    "polarity": s.sentiment.polarity,
    "subjectivity": s.sentiment.subjectivity
    }

  return (meta, doc, tiles)


def pretty_print (obj, indent=False):
  """pretty print a JSON object"""
  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ',))
  else:
    return json.dumps(obj, sort_keys=True)


if __name__ == "__main__":
  def new_graf (graf):
    return [], " ".join(graf)


  def parse_graf (graf_text):
    global DEBUG

    if DEBUG:
      print graf_text

    if len(graf_text) > 0:
      return pretty_print(tag_doc(graf_text), indent=False)


  ## test Spark email list message
  ## https://www.mail-archive.com/user@spark.apache.org/msg17932.html

  with open(sys.argv[1], 'r') as f:
    graf, graf_text = new_graf("")

    # segment raw text into paragraphs
    for line in f.readlines():
      line = line.strip()

      if len(line) < 1:
        graf, graf_text = new_graf(graf)
        print parse_graf(graf_text)
      else:
        graf.append(line)

    graf, graf_text = new_graf(graf)
    print parse_graf(graf_text)
