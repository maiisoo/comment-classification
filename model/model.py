import argparse
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.classification import LogisticRegression

#sadness (0), joy (1), love (2), anger (3), fear (4), suprised (5)

def train(df, model_path):
    # Tokenize and remove stopwords
    tokenizer = RegexTokenizer(inputCol="text", outputCol="tokenized", pattern="\\s+", toLowercase=True)
    stopwords_remover = StopWordsRemover(inputCol="tokenized", outputCol="sw_rmv")
    # TF-IDF
    hashing_tf = HashingTF(inputCol="sw_rmv",
                           outputCol="raw_features",
                           numFeatures=10000)
    idf = IDF(inputCol="raw_features", outputCol="features")
    # Log regression
    lr = LogisticRegression(featuresCol='features',
                            labelCol='label',
                            family="multinomial",
                            regParam=0.3,
                            elasticNetParam=0,
                            maxIter=50)

    # ML pipeline
    model = Pipeline(stages=[tokenizer,
                             stopwords_remover,
                             hashing_tf,
                             idf,
                             lr])

    model = model.fit(df)

    model.save(model_path)

    print("Finished. Trained model saved at: ", model_path)


if __name__ == "__main__":
    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("train_data_dir", type=str)
    parser.add_argument("model_dir", type=str)
    args = parser.parse_args()
    train_data_dir = args.train_data_dir
    model_dir = args.model_dir

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("classifier") \
        .getOrCreate()

    df = spark.read.csv(train_data_dir, inferSchema=True, header=True)
    train(df, model_dir)
