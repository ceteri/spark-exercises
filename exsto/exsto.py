#!/usr/bin/env python
# encoding: utf-8

import dateutil.parser as dp
import json
import lxml.html
import re
import string
import urllib 

DEBUG = False # True


######################################################################
## scrape the Apache mailing list archives

PAT_ID = re.compile("^.*\%3c(.*)\@.*$")


def scrape_url (url):
  """get the HTML and parse it as an XML doc"""
  text = urllib.urlopen(url).read()
  text = filter(lambda x: x in string.printable, text)
  root = lxml.html.document_fromstring(text)

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
  meta["date"] = dp.parse(root.xpath(path)[0].text).isoformat()

  path = "/html/body/table/tbody/tr[@class='raw']/td[@class='right']/a"
  link = root.xpath(path)[0].get("href")
  meta["id"] = PAT_ID.match(link).group(1)

  path = "/html/body/table/tbody/tr[@class='contents']/td/pre"
  meta["text"] = root.xpath(path)[0].text

  # parse the optional elements

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by date']"
  refs = root.xpath(path)

  if len(refs) > 0:
    link = refs[0].get("href")
    meta["next_url"] = base_url + link
  else:
    meta["next_url"] = ""

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Previous by thread']"
  refs = root.xpath(path)
  
  if len(refs) > 0:
    link = refs[0].get("href")
    meta["prev_thread"] = PAT_ID.match(link).group(1)
  else:
    meta["prev_thread"] = ""

  path = "/html/body/table/thead/tr/th[@class='nav']/a[@title='Next by thread']"
  refs = root.xpath(path)
  
  if len(refs) > 0:
    link = refs[0].get("href")
    meta["next_thread"] = PAT_ID.match(link).group(1)
  else:
    meta["next_thread"] = ""

  return meta


######################################################################
## common utilities

def pretty_print (obj, indent=False):
  """pretty print a JSON object"""

  if indent:
    return json.dumps(obj, sort_keys=True, indent=2, separators=(',', ': '))
  else:
    return json.dumps(obj, sort_keys=True)
