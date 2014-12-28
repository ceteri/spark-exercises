#!/usr/bin/env python
# encoding: utf-8

import exsto
import os
import sys


def main ():
  path = sys.argv[1]

  if os.path.isdir(path):
    exsto.test_filter(path)
  else:
    with open(path, 'r') as f:
      for line in f.readlines():
        print exsto.pretty_print(exsto.filter_quotes(line))


if __name__ == "__main__":
  main()
