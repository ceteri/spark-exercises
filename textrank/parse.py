#!/usr/bin/env python
# encoding: utf-8

import hashlib
import itertools
import json
import re
import sys
import textblob
import textblob_aptagger as tag

DEBUG = False # True

PAT_PUNCT = re.compile(r'^\W+$')
POS_KEEPS = ['v', 'n', 'j', 'r']
POS_LEMMA = ['v', 'n']
TAGGER = tag.PerceptronTagger()
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
  iters = itertools.tee(iterable, size)

  for i in xrange(1, size):
    for each in iters[i:]:
      next(each, None)

  return list(itertools.izip(*iters))


def get_tiles (graf, size=3):
  """generate word pairs for the TextRank graph"""
  for seq in sliding_window(graf, size):
    for word in seq[1:]:
      w0, w1 = (seq[0], word,)

      if w0[4] == w1[4] == 1:
        yield (w0[0], w1[0],)


def parse_graf (text):
  """parse and markup each sentence in the given paragraph"""
  global DEBUG
  global PAT_PUNCT, POS_KEEPS, POS_LEMMA, TAGGER

  markup = []

  for s in textblob.TextBlob(text).sentences:
    graf = []
    m = hashlib.sha1()
    i = 0

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
    tile = list(get_tiles(graf))

    markup.append({
        "size": i,
        "lang": s.detect_language(),
        "sha1": m.hexdigest(),
        "polr": s.sentiment.polarity,
        "subj": s.sentiment.subjectivity,
        "graf": graf,
        "tile": tile
        })

  return markup


def pretty_print (obj, indent=False):
  """pretty print a JSON object"""
  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': ',))
  else:
    return json.dumps(obj, sort_keys=True)


def main():
  path = sys.argv[1]

  with open(path, 'r') as f:
    for line in f.readlines():
      for graf_text in json.loads(line):
        for sent in parse_graf(graf_text):
          print pretty_print(sent)


if __name__ == "__main__":
  main()
