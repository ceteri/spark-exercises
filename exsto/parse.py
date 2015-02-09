#!/usr/bin/env python
# encoding: utf-8

import exsto
import json
import sys


DEBUG = False # True

def main():
  global DEBUG

  path = sys.argv[1]

  with open(path, 'r') as f:
    for line in f.readlines():
      meta = json.loads(line)
      base = 0

      for graf_text in exsto.filter_quotes(meta["text"]):
        if DEBUG:
          print graf_text

        grafs, new_base = exsto.parse_graf(meta["id"], graf_text, base)
        base = new_base

        for graf in grafs:
          print exsto.pretty_print(graf)


if __name__ == "__main__":
  main()
