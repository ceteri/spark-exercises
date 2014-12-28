#!/usr/bin/env python
# encoding: utf-8

import dateutil.parser as dp
import json
import lxml.html
import os
import re
import string
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


def filter_quotes (line):
  """filter the quoted text out of a message"""
  global DEBUG
  global PAT_FORWARD, PAT_REPLIED, PAT_UNSUBSC

  meta = json.loads(line)
  text = filter(lambda x: x in string.printable, meta["text"])

  if DEBUG:
    print line
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
        grafs = filter_quotes(line)

        if not grafs or len(grafs) < 1:
          raise Exception("no results")
        else:
          print grafs


######################################################################
## common utilities

def pretty_print (obj, indent=False):
  """pretty print a JSON object"""

  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
  else:
    return json.dumps(obj, sort_keys=True)
