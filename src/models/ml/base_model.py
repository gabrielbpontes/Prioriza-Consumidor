import json
from abc import ABC, abstractmethod
from numpy import argmax

class BaseModel(ABC):
    def __init__(self, model_config):
        self.model_path = model_config['model_path']
        self.metrics_path = model_config['metrics_path']
        self.model = None
        self.load_model()

    @abstractmethod
    def load_model(self):
        pass

    def get_metrics(self):
        with open(self.metrics_path, 'r') as f:
            metrics = json.load(f)
        return metrics

    def predict(self, X):
        return self.model.predict(X)

    def predict_label(self, prediction):
        return argmax(prediction, axis=1)

