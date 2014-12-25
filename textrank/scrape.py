#!/usr/bin/env python
# encoding: utf-8

from dateutil import parser
from tempfile import mkstemp
import ConfigParser
import json
import lxml.html
import os
import re
import sys
import urllib 


PAT_ID = re.compile("^.*\%3c(.*)\@.*$")


def scrape_url (url):
  """temp file workaround for lxml.html.parse() bug"""

  _, temp_path = mkstemp()
  root = None

  with open(temp_path, 'w') as f:
    f.write(urllib.urlopen(url).read())
    f.close()
    root = lxml.html.parse(temp_path).getroot()

  os.remove(temp_path)
  return root


def parse_email (root, base_url):
  """parse email fields from an lxml root"""
  global PAT_ID
  meta = {}

  path = "/html/head/title"
  meta["subject"] = root.xpath(path)[0].text

  path = "/html/body/table/tbody/tr[@class='from']/td[@class='right']"
  meta["sender"] = root.xpath(path)[0].text

  path = "/html/body/table/tbody/tr[@class='date']/td[@class='right']"
  meta["date"] = parser.parse(root.xpath(path)[0].text).isoformat()

  path = "/html/body/table/tbody/tr[@class='raw']/td[@class='right']/a"
  link = root.xpath(path)[0].get("href")
  meta["id"] = PAT_ID.match(link).group(1)

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by date']"
  meta["next_url"] = base_url + root.xpath(path)[0].get("href")

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Previous by thread']"
  link = root.xpath(path)[0].get("href")
  meta["prev_thread"] = PAT_ID.match(link).group(1)

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by thread']"
  link = root.xpath(path)[0].get("href")
  meta["next_thread"] = PAT_ID.match(link).group(1)

  path = "/html/body/table/tbody/tr[@class='contents']/td/pre"
  meta["text"] = root.xpath(path)[0].text

  return meta


def pretty_print (obj, indent=False):
  """pretty print a JSON object"""

  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
  else:
    return json.dumps(obj, sort_keys=True)


if __name__ == "__main__":
  config = ConfigParser.ConfigParser()
  config.read("defaults.cfg")

  base_url = config.get("scraper", "base_url")
  url = base_url + config.get("scraper", "start_url")

  print pretty_print(parse_email(scrape_url(url), base_url))
