"""
Given a filename create a defoe.jisc.archive.Article
"""

from defoe.jisc.archive import Article


def filename_to_object(filename):
    """
    Given a filename create a defoe.jisc.archive.Article.  If an error
    arises during its creation this is caught and returned as a
    string.

    :param filename: filename
    :type filename: str or unicode
    :return: tuple of form (Article, None) or (filename, error message),
    if there was an error creating Archive
    :rtype: tuple(defoe.jisc.archive.Article | str or unicode, str or unicode)
    """
    try:
        result = (Article.parse(filename), None)
    except Exception as e:
        result = (filename, str(e))    
    return result
