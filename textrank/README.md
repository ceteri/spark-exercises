# Microservices, Containers, and Machine Learning

This project illustrates how to run advanced text analytics and graph
algorithms at scale.
Its archtecture demonstrates how to combine microservices and
containers along with Big Data frameworks, for a practical approach
to scaling out production systems.
The intended use case helps surface insights about an open source
developer community, based on the activity of its email forum.
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
./scrape.py foo.txt
```

### parse the email text

```bash
./parse.py foo.txt
```


# Research Topics

  * [microservices architecture](http://martinfowler.com/articles/microservices.html)
  * [word2vec use cases](http://www.yseam.com/blog/WV.html)
