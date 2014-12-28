# Microservices, Containers, and Machine Learning

A frequently asked question on the Apache Spark user email list
concerns where to find data sets for evaluating the code.

Oddly enough, the collect of archived messages for this email list
provides an excellent data set to evalute machine learning, graph
algorithms, text analytics, time-series analysis, etc.


Not every use case fits Big Data...

This archtecture demonstrates how to combine microservices and
containers along with Big Data frameworks, to leverage the best of
each approach.

That provides a practical way to scale out production systems.


The intended use case helps surface insights about an open source
developer community, based on the activity of its email forum.

In a sense, these insighs assist the community to become more
self-aware.

As an example, we use data from the `<user@spark.apache.org>` 
[email list archives](http://mail-archives.apache.org).

Technologies used in this example include:

  * [Apache Spark](http://spark.apache.org/)
  * [GraphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html)
  * [MLlib](http://spark.apache.org/docs/latest/mllib-guide.html)
  * [Apache Mesos](http://mesos.apache.org/)
  * [Docker](https://www.docker.com/)
  * [Anaconda](http://continuum.io/downloads)
  * [Flask](http://flask.pocoo.org/)
  * [lxml.html](http://lxml.de/lxmlhtml.html)
  * [python-dateutil](https://labix.org/python-dateutil)
  * [NLTK](http://www.nltk.org/)
  * [TextBlob](https://textblob.readthedocs.org/en/dev/)
  * [Perceptron Tagger](http://stevenloria.com/tutorial-state-of-the-art-part-of-speech-tagging-in-textblob/)
  * [TextRank](http://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf)
  * [Word2Vec](https://code.google.com/p/word2vec/)


## Dependencies

  * https://github.com/opentable/docker-anaconda

```bash
conda config --add channels https://conda.binstar.org/sloria
conda install textblob
python -m textblob.download_corpora
python -m nltk.downloader -d ~/nltk_data all
pip install -U textblob textblob-aptagger
pip install lxml
pip install python-dateutil
pip install Flask
```

NLTK and TextBlob require some
[data downloads](https://s3.amazonaws.com/textblob/nltk_data.tar.gz)
which may also require updating the NLTK data path:

```python
import nltk
nltk.data.path.append("~/nltk_data/")
```


## Running

To change the project configuration simply edit the `defaults.cfg`
file.


### scrape the email list

```bash
./scrape.py foo.json
```

### parse the email text

```bash
./filter.py foo.json
./parse.py foo.json
```


# What's In A Name?

The word [exsto](http://en.wiktionary.org/wiki/exsto) is the Latin
verb meaning "to stand out", in its present active form.


# Research Topics

### machine learning

  * [TextRank](http://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf)
  * [Word2Vec use cases](http://www.yseam.com/blog/WV.html)

### microservices and containers

  * [The Strengths and Weaknesses of Microservices](http://www.infoq.com/news/2014/05/microservices)
  * [Microservices architecture](http://martinfowler.com/articles/microservices.html)
  * [Adrian Crockcroft @ DockerCon](https://blog.docker.com/2014/12/dockercon-europe-keynote-state-of-the-art-in-microservices-by-adrian-cockcroft-battery-ventures/)
  * [Weave](https://github.com/zettio/weave)