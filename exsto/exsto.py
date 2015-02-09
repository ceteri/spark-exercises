#!/usr/bin/env python
# encoding: utf-8

import dateutil.parser as dp
import hashlib
import json
import lxml.html
import os
import re
import string
import textblob
import textblob_aptagger as tag
import urllib 

DEBUG = False # True


######################################################################
## scrape the Apache mailing list archives

PAT_EMAIL_ID = re.compile("^.*\%3c(.*)\@.*$")


def scrape_url (url):
  """get the HTML and parse it as an XML doc"""
  text = urllib.urlopen(url).read()
  text = filter(lambda x: x in string.printable, text)
  root = lxml.html.document_fromstring(text)

  return root


def parse_email (root, base_url):
  """parse email fields from an lxml root"""
  global PAT_EMAIL_ID
  meta = {}

  path = "/html/head/title"
  meta["subject"] = root.xpath(path)[0].text

  path = "/html/body/table/tbody/tr[@class='from']/td[@class='right']"
  meta["sender"] = root.xpath(path)[0].text

  path = "/html/body/table/tbody/tr[@class='date']/td[@class='right']"
  meta["date"] = dp.parse(root.xpath(path)[0].text).isoformat()

  path = "/html/body/table/tbody/tr[@class='raw']/td[@class='right']/a"
  link = root.xpath(path)[0].get("href")
  meta["id"] = PAT_EMAIL_ID.match(link).group(1)

  path = "/html/body/table/tbody/tr[@class='contents']/td/pre"
  meta["text"] = root.xpath(path)[0].text

  # parse the optional elements

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by date']"
  refs = root.xpath(path)

  if len(refs) > 0:
    link = refs[0].get("href")
    meta["next_url"] = base_url + link
  else:
    meta["next_url"] = ""

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Previous by thread']"
  refs = root.xpath(path)
  
  if len(refs) > 0:
    link = refs[0].get("href")
    meta["prev_thread"] = PAT_EMAIL_ID.match(link).group(1)
  else:
    meta["prev_thread"] = ""

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by thread']"
  refs = root.xpath(path)
  
  if len(refs) > 0:
    link = refs[0].get("href")
    meta["next_thread"] = PAT_EMAIL_ID.match(link).group(1)
  else:
    meta["next_thread"] = ""

  return meta


######################################################################
## filter the novel text versus quoted text in an email message

PAT_FORWARD = re.compile("\n\-+ Forwarded message \-+\n")
PAT_REPLIED = re.compile("\nOn.*\d+.*\n?wrote\:\n+\>")
PAT_UNSUBSC = re.compile("\n\-+\nTo unsubscribe,.*\nFor additional commands,.*")


def split_grafs (lines):
  """segment the raw text into paragraphs"""
  graf = []

  for line in lines:
    line = line.strip()

    if len(line) < 1:
      if len(graf) > 0:
        yield "\n".join(graf)
        graf = []
    else:
      graf.append(line)

  if len(graf) > 0:
    yield "\n".join(graf)


def filter_quotes (text):
  """filter the quoted text out of a message"""
  global DEBUG
  global PAT_FORWARD, PAT_REPLIED, PAT_UNSUBSC

  text = filter(lambda x: x in string.printable, text)

  if DEBUG:
    print text

  # strip off quoted text in a forward
  m = PAT_FORWARD.split(text, re.M)

  if m and len(m) > 1:
    text = m[0]

  # strip off quoted text in a reply
  m = PAT_REPLIED.split(text, re.M)

  if m and len(m) > 1:
    text = m[0]

  # strip off any trailing unsubscription notice
  m = PAT_UNSUBSC.split(text, re.M)

  if m:
    text = m[0]

  # replace any remaining quoted text with blank lines
  lines = []

  for line in text.split("\n"):
    if line.startswith(">"):
      lines.append("")
    else:
      lines.append(line)

  return list(split_grafs(lines))


def test_filter (path):
  """run the unit tests for known quoting styles"""
  global DEBUG
  DEBUG = True

  for root, dirs, files in os.walk(path):
    for file in files:
      with open(path + file, 'r') as f:
        line = f.readline()
        meta = json.loads(line)
        grafs = filter_quotes(meta["text"])

        if not grafs or len(grafs) < 1:
          raise Exception("no results")
        else:
          print grafs


######################################################################
## parse and markup text paragraphs for semantic analysis

PAT_PUNCT = re.compile(r'^\W+$')
PAT_SPACE = re.compile(r'\_+$')

POS_KEEPS = ['v', 'n', 'j']
POS_LEMMA = ['v', 'n']
TAGGER = tag.PerceptronTagger()
UNIQ_WORDS = { ".": 0 }


def is_not_word (word):
  return PAT_PUNCT.match(word) or PAT_SPACE.match(word)


def get_word_id (root):
  """lookup/assign a unique identify for each word"""
  global UNIQ_WORDS

  # in practice, this should use a microservice via some robust
  # distributed cache, e.g., Cassandra, Redis, etc.

  if root not in UNIQ_WORDS:
    UNIQ_WORDS[root] = len(UNIQ_WORDS)

  return UNIQ_WORDS[root]


def get_tiles (graf, size=3):
  """generate word pairs for the TextRank graph"""
  graf_len = len(graf)

  for i in xrange(0, graf_len):
    w0 = graf[i]

    for j in xrange(i + 1, min(graf_len, i + 1 + size)):
      w1 = graf[j]

      if w0[4] == w1[4] == 1:
        yield (w0[0], w1[0],)


def parse_graf (msg_id, text, base):
  """parse and markup each sentence in the given paragraph"""
  global DEBUG
  global POS_KEEPS, POS_LEMMA, TAGGER

  markup = []
  i = base

  for s in textblob.TextBlob(text).sentences:
    graf = []
    m = hashlib.sha1()

    pos = TAGGER.tag(str(s))
    p_idx = 0
    w_idx = 0

    while p_idx < len(pos):
      p = pos[p_idx]

      if DEBUG:
        print "IDX", p_idx, p
        print "reg", is_not_word(p[0])
        print "   ", w_idx, len(s.words), s.words
        print graf

      if is_not_word(p[0]) or (p[1] == "SYM"):
        if (w_idx == len(s.words) - 1):
          w = p[0]
          t = '.'
        else:
          p_idx += 1
          continue
      elif w_idx < len(s.words):
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

    #"lang": s.detect_language(),
    markup.append({
        "id": msg_id,
        "size": len(graf),
        "sha1": m.hexdigest(),
        "polr": s.sentiment.polarity,
        "subj": s.sentiment.subjectivity,
        "graf": graf,
        "tile": tile
        })

  return markup, i


######################################################################
## common utilities

def pretty_print (obj, indent=False):
  """pretty print a JSON object"""

  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
  else:
    return json.dumps(obj, sort_keys=True)
