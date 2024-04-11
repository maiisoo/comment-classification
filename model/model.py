from pyspark import SparkContext
from tensorflowonspark import TFNode
import numpy as np


# Define your Keras model
def create_model():
    from keras.models import Sequential
    from keras.layers import Embedding, Bidirectional, LSTM, Dense
    from keras.optimizers import Adam

    model = Sequential()
    model.add(Embedding(16000, 100, input_length=66))
    model.add(Bidirectional(LSTM(150)))
    model.add(Dense(16000, activation='softmax'))
    adam = Adam(lr=0.01)
    model.compile(loss='categorical_crossentropy', optimizer=adam, metrics=['accuracy'])
    return model


def train_model(data):
    train_X, train_y_one_hot = zip(*data)
    train_X = np.array(train_X)
    train_y_one_hot = np.array(train_y_one_hot)

    model = create_model()
    history = model.fit(train_X, train_y_one_hot, epochs=25, verbose=1, validation_split=0.2)

    return history


if __name__ == "__main__":
    sc = SparkContext()
    sc.setLogLevel("WARN")

    # Parallelize your training data across the Spark cluster
    train_data = sc.parallelize(
        [(train_X_partition, train_y_one_hot_partition) for train_X_partition, train_y_one_hot_partition in
         zip(train_X_partitions, train_y_one_hot_partitions)])

    # Train the model on each partition of data
    histories = train_data.map(train_model).collect()

    # Aggregate and process training histories as needed
    # (e.g., compute average loss and accuracy across all epochs)
