'''
Run a Spark text analysis job to analyse XML input articles in 
JISC BL_newspaper/BL_article format.

For example:

    $SPARK_HOME/bin/spark-submit --py-files defoe.zip defoe/run_query.py -n 4 -r results_jisc.yml \
        data_jisc.txt jisc defoe.jisc.queries.weighted_target_keyword_query queries/accident_targets.yml

'''

import yaml
import os
from collections import namedtuple, defaultdict

from defoe import query_utils

WordLocation = namedtuple('WordLocation', "word position path ")
MatchedWords = namedtuple('MatchedWords', 'target_word keyword distance words preprocessed path')

def compute_distance(word1_loc, word2_loc):
    return abs(word1_loc.position - word2_loc.position)


def get_min_distance_to_target(keyword_locations, target_locations):
    min_distance = None
    target_loc = None
    keyword_loc = None
    for k_loc in keyword_locations:
        for t_loc in target_locations:
            d = compute_distance(k_loc, t_loc)
            if not min_distance or d < min_distance:
                min_distance = d
                target_loc = t_loc
                keyword_loc = k_loc
    return min_distance, target_loc, keyword_loc


def find_words(
        article, target_words, keywords,
        preprocess_type=query_utils.PreprocessWordType.LEMMATIZE):

    matches = []
    keys = defaultdict(lambda: [])
    targets = []
    preprocessed_words = []
    for pos, word in enumerate(article.article_words):
        if word.word is not None:
            preprocessed_word = query_utils.preprocess_word(word.word, preprocess_type)
            loc = WordLocation(
                word=preprocessed_word,
                position=pos,
                path=article.path)
            preprocessed_words.append(preprocessed_word)
            if preprocessed_word in keywords:
                keys[preprocessed_word].append(loc)
            if preprocessed_word in target_words:
                targets.append(loc)
    for k, l in keys.items():
        min_distance, target_loc, keyword_loc = get_min_distance_to_target(l, targets)
        if min_distance:
            matches.append(
                MatchedWords(
                    target_word=target_loc.word,
                    keyword=keyword_loc.word,
                    path=target_loc.path,
                    distance=min_distance,
                    words=article.article_words,
                    preprocessed=preprocessed_words
                )
            )

    return matches


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
    
    # create the output dictionary
    # mapping from
    #   [MatchedWords(target_word, keyword, distance, words, preprocessed path)]
    # to
    #   [(word, {"path": path, "distance": distance, ...}), ...]
    matching_docs = filtered_words.map(
        lambda matched:
        (matched.keyword, {
            "path": matched.path[len('blob:'):],
            "target_word": matched.target_word,
            "keyword": matched.keyword,
            "distance": matched.distance
        }))

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
    