#!/usr/bin/env python
# encoding: utf-8

import json
import os
import re
import string
import sys

DEBUG = False # True

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


def test_cases (path):
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


def main ():
  path = sys.argv[1]

  if os.path.isdir(path):
    test_cases(path)
  else:
    with open(path, 'r') as f:
      for line in f.readlines():
        print json.dumps(filter_quotes(line))


if __name__ == "__main__":
  main()
