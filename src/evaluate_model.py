
"""Evaluate Isolation Forest using labelled synthetic weather readings."""

import logging

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from src.config import MODEL_PATH
from src.train_model import FEATURE_NAMES, generate_training_data


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def generate_labelled_test_data(random_state: int = 123) -> tuple:
    """Create labelled normal and anomalous synthetic weather readings.

    Label 0 = normal reading.
    Label 1 = anomalous reading.
    """
    rng = np.random.default_rng(random_state)

    # Normal readings follow the training-data ranges.
    normal = np.column_stack(
        (
            rng.uniform(22, 34, 1000),
            rng.uniform(35, 85, 1000),
            rng.uniform(2, 25, 1000),
        )
    )

    # Heatwave: unusually high temperature.
    heatwave = np.column_stack(
        (
            rng.uniform(40, 47, 150),
            rng.uniform(35, 85, 150),
            rng.uniform(2, 25, 150),
        )
    )

    # Storm gust: unusually high wind speed.
    storm = np.column_stack(
        (
            rng.uniform(22, 34, 150),
            rng.uniform(35, 85, 150),
            rng.uniform(40, 80, 150),
        )
    )

    # Humidity sensor fault: implausibly low or high humidity.
    humidity = np.column_stack(
        (
            rng.uniform(22, 34, 150),
            np.concatenate(
                (
                    rng.uniform(0, 15, 75),
                    rng.uniform(95, 100, 75),
                )
            ),
            rng.uniform(2, 25, 150),
        )
    )

    X = np.vstack((normal, heatwave, storm, humidity))
    y = np.concatenate(
        (
            np.zeros(len(normal), dtype=int),
            np.ones(len(heatwave), dtype=int),
            np.ones(len(storm), dtype=int),
            np.ones(len(humidity), dtype=int),
        )
    )

    # Shuffle rows while keeping each label paired with its reading.
    order = rng.permutation(len(y))
    return X[order], y[order]


def report_metrics(name: str, model, X_test, y_test) -> dict:
    """Print evaluation metrics using 1 to represent an anomaly."""
    predictions = (model.predict(X_test) == -1).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_test, predictions, labels=[0, 1]
    ).ravel()

    precision = precision_score(y_test, predictions, zero_division=0)
    recall = recall_score(y_test, predictions, zero_division=0)
    f1 = f1_score(y_test, predictions, zero_division=0)
    accuracy = accuracy_score(y_test, predictions)

    print(f"\n{'=' * 60}")
    print(name)
    print(f"{'=' * 60}")
    print(f"Accuracy : {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall   : {recall:.4f}")
    print(f"F1-score : {f1:.4f}")
    print("\nConfusion matrix (rows=actual, columns=predicted):")
    print("                 Predicted normal  Predicted anomaly")
    print(f"Actual normal          {tn:4d}              {fp:4d}")
    print(f"Actual anomaly         {fn:4d}              {tp:4d}")

    print("\nClassification report:")
    print(
        classification_report(
            y_test,
            predictions,
            labels=[0, 1],
            target_names=["Normal", "Anomaly"],
            zero_division=0,
        )
    )

    return {
        "contamination": name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "true_negatives": tn,
        "false_positives": fp,
        "false_negatives": fn,
        "true_positives": tp,
    }


def evaluate_model() -> None:
    """Evaluate the saved model and compare contamination settings."""
    X, y = generate_labelled_test_data()

    # Use a separate normal-only training dataset.
    normal_training_data = generate_training_data(
        n_samples=5000,
        random_state=42,
    )

    X_train, _ = train_test_split(
        normal_training_data,
        test_size=0.2,
        random_state=42,
    )

    print(f"Features: {FEATURE_NAMES}")
    print(f"Evaluation samples: {len(y)}")
    print(f"Normal samples: {int(np.sum(y == 0))}")
    print(f"Anomaly samples: {int(np.sum(y == 1))}")

    results = []

    # Evaluate the model already saved by train_model.py.
    saved = joblib.load(MODEL_PATH)
    saved_model = saved.get("model", saved) if isinstance(saved, dict) else saved

    results.append(
        report_metrics(
            "Saved model",
            saved_model,
            X,
            y,
        )
    )

    # Compare new models trained on the same normal-only training data.
    for contamination in (0.03, 0.05, 0.10):
        comparison_model = make_pipeline(
            StandardScaler(),
            IsolationForest(
                n_estimators=200,
                contamination=contamination,
                random_state=42,
            ),
        )
        comparison_model.fit(X_train)

        results.append(
            report_metrics(
                f"Contamination = {contamination:.2f}",
                comparison_model,
                X,
                y,
            )
        )

    print("\nCONTAMINATION COMPARISON")
    print("-" * 78)
    print(
        f"{'Model':<25} {'Precision':>10} {'Recall':>10} "
        f"{'F1-score':>10} {'FP':>8} {'FN':>8}"
    )

    for result in results:
        print(
            f"{result['contamination']:<25} "
            f"{result['precision']:>10.4f} "
            f"{result['recall']:>10.4f} "
            f"{result['f1_score']:>10.4f} "
            f"{result['false_positives']:>8d} "
            f"{result['false_negatives']:>8d}"
        )

    print(
        "\nNote: These are results on a synthetic evaluation dataset, "
        "not real-world weather observations."
    )


if __name__ == "__main__":
    evaluate_model()
