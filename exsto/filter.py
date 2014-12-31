#!/usr/bin/env python
# encoding: utf-8

import exsto
import json
import os
import sys


def main ():
  path = sys.argv[1]

  if os.path.isdir(path):
    exsto.test_filter(path)
  else:
    with open(path, 'r') as f:
      for line in f.readlines():
        meta = json.loads(line)
        print exsto.pretty_print(exsto.filter_quotes(meta["text"]))


if __name__ == "__main__":
  main()
