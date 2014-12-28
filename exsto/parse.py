#!/usr/bin/env python
# encoding: utf-8

import exsto
import json
import sys


def main():
  path = sys.argv[1]

  with open(path, 'r') as f:
    for line in f.readlines():
      for graf_text in json.loads(line):
        for sent in exsto.parse_graf(graf_text):
          print exsto.pretty_print(sent)


if __name__ == "__main__":
  main()
