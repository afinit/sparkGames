# sparkGames
Playing around with Spark.. starting with UCI datasets


This started out with the goal of playing around with Spark, but quickly got sidetracked for a bit. I built a few things to play with streaming ideas here.

- `LinearModel`: A simple linear model object trained with gradient descent so that I can complete updates to the model.
- `RunningMean`: Simple mean aggregator

more explanations to follow
 
## Command Line Help ##  

```
Usage: SparkGames [options]

  --runRedWine         Run Red Wine Linear Model (from file iterator)
  --runWhiteWine       Run White Wine Linear Model (from file iterator)
  --runSparkRed        Run Red Wine Linear Model with Spark
  --runPhRunningMean   Run Ph Running Mean (from file iterator)
  --runSparkWikiEdits  Run Spark Wiki Edits Reader
  --runWikiFirehose    Run Wiki Firehose to watch edits.. only uses akka http
  --groupSize <value>  Batch size of runs
  --help               prints this usage text
```

## Explanation of each workflow ##  
### runRedWine ###
Using a file iterator, runs through Red Wine dataset from the [Wine Quality UCI dataset](http://archive.ics.uci.edu/ml/datasets/wine+quality). This processes the file in batches set by groupSize. By default, this value is 25. With this value, the `LinearModel` trains on a batch of 24 observations and 1 row per batch is added to the test set. We are training regressing on the quality of the wine here, a value between 0-10.

I did not spend a lot of time tuning this, so I'm not sure what the max would be, but current settings train the model to achieve a mean squared error of ~4.5

### runWhiteWine ###
Same as `runRedWine` but with the White Wine dataset instead.

This achieves a mean squared error of ~3.3 with current settings.

### runSparkRed ###
Same as `runRedWine` but using Spark to run through the file.

### runPhRunningMean ###
Calculates the running mean in batches of `groupSize` of the pH value of the Red Wine Dataset. Mean is printed to screen on each batch.

### runSparkWikiEdits ###
Currently just reads wiki edits into Spark stream, converts then to a case class, and writes to screen. Log events are not handled. This will be expanded in the future.

### runWikiFirehose ###
Hooks up to the Wikipedia Edits event stream and prints events to screen. This uses akka http.

https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
