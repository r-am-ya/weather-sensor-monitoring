
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split


RANDOM_STATE = 42
FEATURE_NAMES = ["temperature_c", "humidity_pct", "wind_speed_kmh"]


def generate_training_data(
    n_samples: int = 20000, random_state: int = RANDOM_STATE
) -> np.ndarray:
    """Generate reproducible synthetic normal inputs used by this demo model."""
    rng = np.random.default_rng(random_state)
    return np.column_stack(
        (
            rng.uniform(22, 36, n_samples),
            rng.uniform(30, 90, n_samples),
            rng.uniform(0, 30, n_samples),
        )
    )


def main():
    # Generate independent combinations of normal sensor readings.
    # These ranges match the normal ranges in the sensor simulator.
    n_samples = 20000

    X = generate_training_data(n_samples, RANDOM_STATE)
    feature_names = FEATURE_NAMES

    X_train, X_test = train_test_split(
        X,
        test_size=0.20,
        random_state=RANDOM_STATE,
    )

    model = IsolationForest(
        n_estimators=200,
        contamination=0.03,
        random_state=RANDOM_STATE,
    )

    model.fit(X_train)

    # Check the model against held-out normal readings.
    predictions = model.predict(X_test)
    flagged = int(np.sum(predictions == -1))

    print(f"Normal test readings: {len(X_test)}")
    print(f"Flagged by Isolation Forest: {flagged}")
    print(f"Flagged percentage: {flagged / len(X_test) * 100:.2f}%")

    # Check representative sensor combinations.
    samples = np.array([
        [22.6, 32.2, 22.7],
        [26.2, 46.7, 27.8],
        [30.8, 56.2, 12.5],
        [31.6, 63.5, 19.2],
    ])

    print("\nRepresentative normal readings:")
    for reading, prediction, score in zip(
        samples,
        model.predict(samples),
        model.decision_function(samples),
    ):
        print(
            dict(zip(feature_names, reading)),
            "=>",
            "ANOMALY" if prediction == -1 else "NORMAL",
            f"(score={score:.4f})",
        )

    model_path = Path(__file__).resolve().parent.parent / "models"
    model_path.mkdir(parents=True, exist_ok=True)

    output_file = model_path / "isolation_forest.joblib"
    joblib.dump(model, output_file)

    print(f"\nModel saved to: {output_file}")


if __name__ == "__main__":
    main()
