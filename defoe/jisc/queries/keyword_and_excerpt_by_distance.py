'''
Run a Spark text analysis job to analyse articles in 
XML JISC BL_newspaper/BL_article format.

For example:

    $SPARK_HOME/bin/spark-submit --py-files defoe.zip defoe/run_query.py -n 4 -r results_jisc.yml \
        data_jisc.txt jisc defoe.jisc.queries.keyword_and_excert_by_distance queries/accident_targets.yml

'''

import yaml
import os

from defoe import query_utils
from defoe.jisc.queries.utils import find_words

def extract_output(matched):
    path = matched.target.path
    if path.startswith('blob:'):
        path = path[len('blob:'):]
    target_pos = matched.target.position
    key_pos = matched.keyword.position
    start_pos = max(0, min(target_pos-10, key_pos-10))
    end_pos = min(len(matched.words), max(target_pos+10, key_pos+10))
    excerpt = [word.word for word in matched.words[start_pos:end_pos]]
    return (matched.keyword.word, {
            "path": path,
            "excerpt": excerpt,
            "target_word": matched.target.word,
            "keyword": matched.keyword.word,
            "distance": matched.distance
        })

def do_query(articles, config_file=None, logger=None, context=None):

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    preprocess_type = query_utils.extract_preprocess_word_type(config)
    data_file = query_utils.extract_data_file(config,
                                              os.path.dirname(config_file))
    keywords = []
    with open(data_file, 'r') as f:
        input_words = yaml.safe_load(f)

    target_words = set([query_utils.preprocess_word(word, preprocess_type) for word in input_words['targets']])
    keywords = set([query_utils.preprocess_word(word, preprocess_type) for word in input_words['keywords']])

    # find textblocks that contain pairs of (target word, keyword) and record their distance
    filtered_words = articles.flatMap(
        lambda document: find_words(document, target_words, keywords, preprocess_type))

    # mapping from
    #   [MatchedWords(target, keyword, distance, words, preprocessed path)]
    # to
    #   [(word, {"path": path, "distance": distance, ...}), ...]
    matching_docs = filtered_words.map(extract_output)

    # group by the matched keywords and collect all the articles by keyword
    # [(word, {"path": path, ...}), ...]
    # =>
    # [(word, [{"path": path, ...], {...}), ...)]
    # sorted by distance between target and keyword
    result = matching_docs \
        .groupByKey() \
        .map(lambda word_context:
             (word_context[0], sorted(list(word_context[1]), key=lambda d: d['distance']))) \
        .collect()
    return result
    