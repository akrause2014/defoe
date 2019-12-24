# Match keywords and target words in an article and calculate distance score

This query searches for a number of keywords and target words in each article.
An article matches if it contains both a target word and a keyword, 
and the distance between the target word and the keyword
(i.e. the number of words between target and keyword) is calculated.
The results are grouped by the matched keyword and sorted by distance score.
An article may appear multiple times, once for each matched keyword.

* Query module: `defoe.jisc.queries.weighted_target_keyword_query`
* Configuration file: 
   * a YAML file containing the target words and the keywords.
   * Example:
      `queries/accidents_targets.yml`
* Result format:
  ```
  <KEYWORD>: 
  - target: <TARGET>
  - keyword: <KEYWORD>
  - distance: <DISTANCE SCORE>
  - path: <PATH>
  <KEYWORD>:
  ...
  ```