from collections import namedtuple, defaultdict

from defoe import query_utils

WordLocation = namedtuple('WordLocation', "word position path ")
MatchedWords = namedtuple('MatchedWords', 'target keyword distance words preprocessed')


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
                    target=target_loc,
                    keyword=keyword_loc,
                    distance=min_distance,
                    words=article.article_words,
                    preprocessed=preprocessed_words
                )
            )

    return matches

