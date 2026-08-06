"""Evaluation metrics for trainable compliance detectors."""
from __future__ import annotations

from typing import Any

from .feature_extractor import map_label


def confusion_matrix(rows: list[dict[str, Any]], predictions: list[str], labels: list[str]) -> dict[str, dict[str, int]]:
    matrix: dict[str, dict[str, int]] = {gold: {pred: 0 for pred in labels} for gold in labels}
    for row, pred in zip(rows, predictions):
        gold = map_label(row)
        matrix.setdefault(gold, {label: 0 for label in labels})
        matrix[gold][pred] = matrix[gold].get(pred, 0) + 1
    return matrix


def classification_report(rows: list[dict[str, Any]], predictions: list[str], labels: list[str]) -> dict[str, Any]:
    total = len(rows)
    correct = sum(1 for row, pred in zip(rows, predictions) if map_label(row) == pred)
    per_class: dict[str, dict[str, float | int]] = {}
    for label in labels:
        tp = sum(1 for row, pred in zip(rows, predictions) if map_label(row) == label and pred == label)
        fp = sum(1 for row, pred in zip(rows, predictions) if map_label(row) != label and pred == label)
        fn = sum(1 for row, pred in zip(rows, predictions) if map_label(row) == label and pred != label)
        tn = total - tp - fp - fn
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
        per_class[label] = {
            "support": tp + fn,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }
    macro_f1 = sum(float(per_class[label]["f1"]) for label in labels) / len(labels)
    return {
        "samples": total,
        "correct": correct,
        "accuracy": round(correct / total if total else 0.0, 4),
        "macro_f1": round(macro_f1, 4),
        "per_class": per_class,
        "confusion": confusion_matrix(rows, predictions, labels),
    }

