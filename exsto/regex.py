import re

PAT_PUNCT = re.compile(r'^\W+$')
PAT_SPACE = re.compile(r'\S+$')

ex =  [
    '________________________________',
    '.'
    ]

for s in ex:
    print s
    print "reg", PAT_PUNCT.match(s)
    print "foo", PAT_SPACE.match(s)
