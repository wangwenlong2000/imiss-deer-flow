import java.util.List;

public class VideoPipelineMissingDecode {
    private final VideoCapture camera;
    private final ObjectDetector detector;
    private final ObjectTracker tracker;
    private final TrackRepository repository;

    public VideoPipelineMissingDecode(
            VideoCapture camera,
            ObjectDetector detector,
            ObjectTracker tracker,
            TrackRepository repository) {
        this.camera = camera;
        this.detector = detector;
        this.tracker = tracker;
        this.repository = repository;
    }

    public void runPipeline() {
        Frame frame = camera.read();
        List<Detection> detections = detector.detect(frame);
        List<Track> tracks = tracker.update(detections);
        repository.save(tracks);
    }
}
