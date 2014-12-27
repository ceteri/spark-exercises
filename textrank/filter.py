#!/usr/bin/env python
# encoding: utf-8

import json
import re
import sys

PAT_FORWARD = re.compile("^\-+ Forwarded message \-+$")
PAT_REPLIED = re.compile("^On.*\d+.*\swrote\:$")


def strip_forward (text):
  global PAT_FORWARD
  lines = []
  new_msg = True

  for line in text.split("\n"):
    if PAT_FORWARD.match(line):
      new_msg = False

    if new_msg:
      lines.append(line)

  return lines


def strip_replied (text):
  global PAT_REPLIED
  lines = []

  for line in text.split("\n"):
    if len(line) < 1 or line[0] != '>':
      if not PAT_REPLIED.match(line):
        lines.append(line)

  return lines


PATS = {
  "forward": (PAT_FORWARD, strip_forward,),
  "replied": (PAT_REPLIED, strip_replied,),
  }


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


if __name__ == "__main__":
  with open(sys.argv[1], 'r') as f:
    for line in f.readlines():
      meta = json.loads(line)
      text = meta["text"]
      print line
      print text
      mode = set([])

      # scan for a replied/quoted text pattern
      for line in text.split("\n"):
        for key, (pat, func) in PATS.items():
          if pat.match(line):
            mode.add(key)

      # filter for novel text in the reply
      if len(mode) > 0:
        idx = list(mode)[0]
        _, func = PATS[idx]
        print list(split_grafs(func(text)))

