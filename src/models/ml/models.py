import joblib
from numpy import argmax
from tensorflow.keras.models import load_model
from src.config import Config
from src.models.ml.base_model import BaseModel

config = Config()

class LogisticRegression(BaseModel):
    def __init__(self, model_config = config.model_configs['LOGIT']):
        super().__init__(model_config)

    def load_model(self):
        self.model = joblib.load(self.model_path)
    
class ANN(BaseModel):
    def __init__(self, model_config = config.model_configs['ANN']):
        super().__init__(model_config)

    def load_model(self):
        self.model = load_model(self.model_path)
