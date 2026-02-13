import nltk
import re
import unicodedata
from nltk.corpus import stopwords
from sklearn.base import BaseEstimator, TransformerMixin

def ensure_nltk_resources():
    resources = {
        'stopwords': 'corpora/stopwords',
        'punkt': 'tokenizers/punkt'
    }
    for resource, path in resources.items():
        try:
            nltk.data.find(path)
        except LookupError:
            nltk.download(resource)

ensure_nltk_resources()
stop_words = set(stopwords.words('portuguese'))

class TextCleaner(BaseEstimator, TransformerMixin):
    """Custom transformer to clean and preprocess text data."""

    def __init__(self, remove_accents=False):
        self.remove_accents = remove_accents

    def clean_text(self, text: str) -> str:
        """Removes unwanted characters and normalizes the text."""
        text = text.lower()
        text = text.replace('\n', ' ')
        text = re.sub(r'[^a-záàâãéèêíïóôõöúçñü\s]', '', text, flags=re.IGNORECASE)
        if self.remove_accents:
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            text = re.sub(r'[^a-z\s]', '', text)
        return text

    def preprocess_text(self, text: str) -> str:
        """Preprocesses the text by removing stop words and cleaning."""
        clean_text = self.clean_text(text)
        tokens = [word for word in clean_text.split() if word not in stop_words]
        return ' '.join(tokens)

    def fit(self, X, y=None) -> 'TextCleaner':
        return self

    def transform(self, X) -> list:
        return [self.preprocess_text(text) for text in X]