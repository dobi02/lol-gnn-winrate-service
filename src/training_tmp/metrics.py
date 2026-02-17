# metrics.py
import numpy as np
from sklearn.metrics import brier_score_loss, log_loss, accuracy_score, roc_auc_score
from sklearn.calibration import calibration_curve

def calculate_metrics(targets, preds, prefix="val"):
    """
    Args:
        targets: 정답 레이블 (0 or 1)
        preds: 예측 확률 (0.0 ~ 1.0)
        prefix (str): 로깅 키 접두사 ('val', 'test' 등)
    """
    # 1. Basic Metrics
    pred_labels = (preds > 0.5).astype(int)
    acc = accuracy_score(targets, pred_labels)
    brier = brier_score_loss(targets, preds)
    ll = log_loss(targets, preds)
    
    # 2. AUC Calculation
    # 데이터셋에 정답 클래스가 하나만 있는 경우(예: 모두 1) 에러가 날 수 있으므로 예외처리
    try:
        auc = roc_auc_score(targets, preds)
    except ValueError:
        auc = 0.5 # 혹은 np.nan

    # 3. Adaptive Calibration Error (ACE)
    prob_true, prob_pred = calibration_curve(targets, preds, n_bins=10, strategy='quantile')
    ace = np.mean(np.abs(prob_true - prob_pred))
    
    # [AUC 추가됨]
    return {
        f"{prefix}_accuracy": acc,
        f"{prefix}_brier_score": brier,
        f"{prefix}_log_loss": ll,
        f"{prefix}_auc": auc,
        f"{prefix}_ace": ace
    }