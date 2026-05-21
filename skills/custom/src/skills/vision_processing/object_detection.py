from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np

from src.skills.base import BaseSkill, SkillContext


class ObjectDetectionSkill(BaseSkill):
    name = "object-detection"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        if "detections" in input_data:
            return self.success({"detections": input_data["detections"]})

        frames = input_data.get("frames", [])
        labels = input_data.get("labels") or ["car"]
        model_config = context.config.get("models", {}).get("object_detection", {})
        provider = model_config.get("provider", "mock")
        thresholds = model_config.get("thresholds", {})
        if provider == "ultralytics":
            return self._run_ultralytics(frames, labels, thresholds, model_config)
        if provider == "opencv_dnn":
            return self._run_opencv_dnn(frames, labels, thresholds, model_config)

        mock_objects = context.config.get("mock_detections", {})
        detections = []
        for index, frame in enumerate(frames):
            objects = mock_objects.get(frame["frame_id"])
            if objects is None:
                label = labels[0]
                objects = [
                    {
                        "object_id": f"det_{index + 1:04d}",
                        "label": label,
                        "confidence": max(thresholds.get(label, 0.5), 0.8),
                        "bbox": [120 + index, 300, 260 + index, 460],
                    }
                ]
            detections.append(
                {
                    "frame_id": frame["frame_id"],
                    "timestamp": frame.get("timestamp"),
                    "objects": [obj for obj in objects if obj.get("confidence", 0) >= thresholds.get(obj.get("label"), 0.0)],
                }
            )
        return self.success({"detections": detections})

    def _run_ultralytics(self, frames: list[dict], labels: list[str], thresholds: dict, model_config: dict) -> dict:
        try:
            from ultralytics import YOLO
        except ModuleNotFoundError:
            return self.failed(
                "YOLO_DEPENDENCY_MISSING",
                "ultralytics is not installed. Install it with: python -m pip install ultralytics",
                False,
            )

        model_name = model_config.get("name", "yolov8n.pt")
        model_path = model_config.get("model_path") or model_name
        try:
            model = YOLO(model_path)
        except Exception as exc:
            return self.failed("YOLO_MODEL_LOAD_FAILED", f"Failed to load YOLO model: {exc}", False)

        label_set = set(labels)
        detections = []
        for frame_index, frame in enumerate(frames):
            image_path = Path(frame["image_uri"].removeprefix("file://"))
            if not image_path.exists():
                return self.failed("FRAME_NOT_FOUND", f"Frame image not found: {image_path}")
            try:
                results = model.predict(
                    source=str(image_path),
                    conf=float(model_config.get("default_threshold", 0.25)),
                    device=model_config.get("device"),
                    verbose=False,
                )
            except Exception as exc:
                return self.failed("YOLO_INFERENCE_FAILED", f"YOLO inference failed: {exc}", True)

            objects = []
            for result in results:
                names = result.names
                for box_index, box in enumerate(result.boxes):
                    cls_id = int(box.cls[0].item())
                    label = names.get(cls_id, str(cls_id))
                    mapped = model_config.get("label_map", {}).get(label, label)
                    confidence = float(box.conf[0].item())
                    if label_set and mapped not in label_set:
                        continue
                    if confidence < float(thresholds.get(mapped, 0.0)):
                        continue
                    xyxy = [round(float(value), 2) for value in box.xyxy[0].tolist()]
                    objects.append(
                        {
                            "object_id": f"det_{frame_index + 1:04d}_{box_index + 1:03d}",
                            "label": mapped,
                            "confidence": round(confidence, 4),
                            "bbox": xyxy,
                            "model_label": label,
                        }
                    )
            detections.append({"frame_id": frame["frame_id"], "timestamp": frame.get("timestamp"), "objects": objects})
        confidence = sum(obj["confidence"] for det in detections for obj in det["objects"])
        count = sum(len(det["objects"]) for det in detections)
        return self.success({"detections": detections, "model": str(model_path)}, confidence / count if count else 0)

    def _run_opencv_dnn(self, frames: list[dict], labels: list[str], thresholds: dict, model_config: dict) -> dict:
        model_path = Path(str(model_config.get("model_path", "")))
        if not model_path.exists():
            return self.failed("YOLO_MODEL_NOT_FOUND", f"OpenCV DNN model not found: {model_path}")
        class_names = model_config.get("class_names") or _coco_class_names()
        label_map = model_config.get("label_map", {})
        label_set = set(labels)
        input_size = int(model_config.get("input_size", 640))
        default_threshold = float(model_config.get("default_threshold", 0.25))
        nms_threshold = float(model_config.get("nms_threshold", 0.45))

        try:
            net = cv2.dnn.readNetFromONNX(str(model_path))
        except Exception as exc:
            return self.failed("YOLO_MODEL_LOAD_FAILED", f"Failed to load ONNX model with OpenCV DNN: {exc}")

        detections = []
        for frame_index, frame in enumerate(frames):
            image_path = Path(frame["image_uri"].removeprefix("file://"))
            image = cv2.imread(str(image_path))
            if image is None:
                return self.failed("FRAME_NOT_FOUND", f"Frame image not found or unreadable: {image_path}")
            h, w = image.shape[:2]
            blob = cv2.dnn.blobFromImage(image, 1 / 255.0, (input_size, input_size), swapRB=True, crop=False)
            net.setInput(blob)
            try:
                output = net.forward()
            except Exception as exc:
                return self.failed("YOLO_INFERENCE_FAILED", f"OpenCV DNN inference failed: {exc}", True)

            boxes, scores, class_ids = _parse_yolov8_output(output, w, h, input_size, default_threshold)
            keep = cv2.dnn.NMSBoxes(boxes, scores, default_threshold, nms_threshold)
            keep_ids = set(np.array(keep).flatten().tolist()) if len(keep) else set()
            objects = []
            for box_index, (box, score, class_id) in enumerate(zip(boxes, scores, class_ids)):
                if box_index not in keep_ids:
                    continue
                label = class_names[class_id] if class_id < len(class_names) else str(class_id)
                mapped = label_map.get(label, label)
                if label_set and mapped not in label_set:
                    continue
                if score < float(thresholds.get(mapped, 0.0)):
                    continue
                x, y, bw, bh = box
                objects.append(
                    {
                        "object_id": f"det_{frame_index + 1:04d}_{box_index + 1:03d}",
                        "label": mapped,
                        "confidence": round(float(score), 4),
                        "bbox": [round(x, 2), round(y, 2), round(x + bw, 2), round(y + bh, 2)],
                        "model_label": label,
                    }
                )
            detections.append({"frame_id": frame["frame_id"], "timestamp": frame.get("timestamp"), "objects": objects})
        confidence = sum(obj["confidence"] for det in detections for obj in det["objects"])
        count = sum(len(det["objects"]) for det in detections)
        return self.success({"detections": detections, "model": str(model_path)}, confidence / count if count else 0)


def _parse_yolov8_output(output: np.ndarray, image_w: int, image_h: int, input_size: int, threshold: float) -> tuple[list, list, list]:
    prediction = np.squeeze(output)
    if prediction.ndim == 2 and prediction.shape[0] < prediction.shape[1]:
        prediction = prediction.T
    boxes = []
    scores = []
    class_ids = []
    x_scale = image_w / input_size
    y_scale = image_h / input_size
    for row in prediction:
        if len(row) < 6:
            continue
        classes = row[4:]
        class_id = int(np.argmax(classes))
        score = float(classes[class_id])
        if score < threshold:
            continue
        cx, cy, bw, bh = row[:4]
        x = float((cx - bw / 2) * x_scale)
        y = float((cy - bh / 2) * y_scale)
        boxes.append([max(0.0, x), max(0.0, y), float(bw * x_scale), float(bh * y_scale)])
        scores.append(score)
        class_ids.append(class_id)
    return boxes, scores, class_ids


def _coco_class_names() -> list[str]:
    return [
        "person",
        "bicycle",
        "car",
        "motorcycle",
        "airplane",
        "bus",
        "train",
        "truck",
        "boat",
        "traffic light",
        "fire hydrant",
        "stop sign",
        "parking meter",
        "bench",
        "bird",
        "cat",
        "dog",
        "horse",
        "sheep",
        "cow",
        "elephant",
        "bear",
        "zebra",
        "giraffe",
        "backpack",
        "umbrella",
        "handbag",
        "tie",
        "suitcase",
        "frisbee",
        "skis",
        "snowboard",
        "sports ball",
        "kite",
        "baseball bat",
        "baseball glove",
        "skateboard",
        "surfboard",
        "tennis racket",
        "bottle",
        "wine glass",
        "cup",
        "fork",
        "knife",
        "spoon",
        "bowl",
        "banana",
        "apple",
        "sandwich",
        "orange",
        "broccoli",
        "carrot",
        "hot dog",
        "pizza",
        "donut",
        "cake",
        "chair",
        "couch",
        "potted plant",
        "bed",
        "dining table",
        "toilet",
        "tv",
        "laptop",
        "mouse",
        "remote",
        "keyboard",
        "cell phone",
        "microwave",
        "oven",
        "toaster",
        "sink",
        "refrigerator",
        "book",
        "clock",
        "vase",
        "scissors",
        "teddy bear",
        "hair drier",
        "toothbrush",
    ]
