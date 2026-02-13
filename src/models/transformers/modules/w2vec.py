from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np

class Word2VecProcessor:
    
    def __init__(self, word2vec_model):
        """Initializes the processor with the Word2Vec model."""
        self.word2vec_model = word2vec_model

    def tokenize(self, text):
        """Tokenizes the text, keeping only alphabetic words."""
        return [word for word in text.split() if word.isalpha()]

    def get_word2vec_vector(self, tokens):
        """Generates the average Word2Vec vector for a list of tokens."""
        vec = np.zeros(self.word2vec_model.vector_size)
        count = 0
        for word in tokens:
            if word in self.word2vec_model.wv:
                vec += self.word2vec_model.wv[word]
                count += 1
        if count > 0:
            vec /= count
        return vec

class Word2VecVectorizer(BaseEstimator, TransformerMixin):
    def __init__(self, word2vec_model):
        self.word2vec_model = word2vec_model
        self.word2vec_processor = Word2VecProcessor(word2vec_model)

    def fit(self, X, y=None):
        """Fit method, required for scikit-learn compatibility."""
        return self

    def transform(self, X):
        """Transforms texts into average Word2Vec vectors."""
        return np.array([
            self.word2vec_processor.get_word2vec_vector(
                self.word2vec_processor.tokenize(text)
            ) for text in X
        ])