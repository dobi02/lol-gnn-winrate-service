import random

class MockLoLModel:
    def __init__(self):
        print(" [Mock] 가상의 승률 예측 모델을 초기화합니다...")

    def predict(self, input_data):
        """
        input_data: PyG Data 객체 혹은 전처리된 텐서 (현재는 무시)
        return: 0.0 ~ 1.0 사이의 확률 (스칼라)
        """
        # 실제 추론 대신 랜덤값 반환 (테스트용)
        fake_prob = 0.4 + (random.random() * 0.2) # 0.4 ~ 0.6 사이
        return fake_prob

def load_model():
    # 나중에는 여기서 mlflow.pyfunc.load_model()을 호출
    return MockLoLModel()