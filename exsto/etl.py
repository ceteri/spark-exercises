#!/usr/bin/env python
# encoding: utf-8

import exsto
import json
import sys

DEBUG = False # True


def main ():
  global DEBUG
  path = sys.argv[1]

  with open(path, 'r') as f:
    for line in f.readlines():
      meta = json.loads(line)

      for graf_text in exsto.filter_quotes(meta["text"]):
        try:
          for sent in exsto.parse_graf(meta["id"], graf_text):
            print exsto.pretty_print(sent)
        except (IndexError) as e:
          if DEBUG:
            print "IndexError: " + str(e)
            print graf_text

if __name__ == "__main__":
  main()
