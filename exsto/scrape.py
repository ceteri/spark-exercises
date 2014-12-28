#!/usr/bin/env python
# encoding: utf-8

import ConfigParser
import exsto
import sys
import time


def main ():
  config = ConfigParser.ConfigParser()
  config.read("defaults.cfg")

  iterations = config.getint("scraper", "iterations")
  nap_time = config.getint("scraper", "nap_time")
  base_url = config.get("scraper", "base_url")
  url = base_url + config.get("scraper", "start_url")

  with open(sys.argv[1], 'w') as f:
    for i in xrange(0, iterations):
      if len(url) < 1:
        break
      else:
        root = exsto.scrape_url(url)
        meta = exsto.parse_email(root, base_url)

        f.write(exsto.pretty_print(meta))
        f.write('\n')

        url = meta["next_url"]
        time.sleep(nap_time)


if __name__ == "__main__":
  main()
