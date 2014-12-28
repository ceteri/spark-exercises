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


def new_graf (graf):
  return [], " ".join(graf)


def split_grafs (lines):
    graf, graf_text = new_graf("")

    # segment raw text into paragraphs
    for line in lines:
      line = line.strip()

      if len(line) < 1:
        graf, graf_text = new_graf(graf)

        if len(graf_text) > 0:
          yield graf_text
      else:
        graf.append(line)

    graf, graf_text = new_graf(graf)

    if len(graf_text) > 0:
      yield graf_text


def filter_json (line):
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
  global DEBUG
  DEBUG = True

  for root, dirs, files in os.walk(path):
    for file in files:
      with open(path + file, 'r') as f:
        line = f.readline()
        grafs = filter_json(line)

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
        print filter_json(line)


if __name__ == "__main__":
  main()
