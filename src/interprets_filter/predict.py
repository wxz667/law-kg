from __future__ import annotations

from pathlib import Path

from .api import InterpretFilterInput, InterpretPrediction, predict_interprets


def predict(
    inputs: list[InterpretFilterInput | dict[str, object] | str],
    model_dir: Path | None = None,
    config_path: Path | None = None,
) -> list[InterpretPrediction]:
    return predict_interprets(inputs, model_dir=model_dir, config_path=config_path)
