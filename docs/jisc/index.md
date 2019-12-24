# BL newspaper article queries

JISC dataset.

This dataset has the following format:

```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE BL_newspaper SYSTEM "BL_newspaper.dtd">
<BL_newspaper xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/elements/1.1/">
<BL_article>
<title_metadata>
<title>The News</title>
...
</title_metadata>
...
<articleImage>
<articleSequence>0014-053</articleSequence>
<articleImageFile>XXX_YYY_ZZZ.tif</articleImageFile>
<articleCoordinates>234,123,5884,6654</articleCoordinates>
<articleText>
<articleWord coord="0,61,212,121">The</articleWord>
...
```

Keyword searches:

* [Segement articles images by keywords](./keyword_matching.md)

