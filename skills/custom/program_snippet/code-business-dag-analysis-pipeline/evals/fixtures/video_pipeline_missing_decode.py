import cv2


class Detector:
    def predict(self, frame):
        return [{"box": [10, 20, 80, 120], "score": 0.91}]


class Tracker:
    def update(self, detections):
        return [{"track_id": 1, "box": item["box"]} for item in detections]


def save_tracks(tracks, output_path):
    with open(output_path, "w", encoding="utf-8") as file:
        for track in tracks:
            file.write(f"{track['track_id']},{track['box']}\n")


def run_pipeline(video_path, output_path):
    capture = cv2.VideoCapture(video_path)
    detector = Detector()
    tracker = Tracker()

    ok, frame = capture.read()
    if not ok:
        return []

    detections = detector.predict(frame)
    tracks = tracker.update(detections)
    save_tracks(tracks, output_path)
    return tracks
