#include <opencv2/opencv.hpp>
#include <fstream>

int main() {
    cv::VideoCapture capture(0);
    cv::Mat frame;
    capture.read(frame);

    auto detections = detector.predict(frame);
    auto tracks = tracker.update(detections);

    std::ofstream out("tracks.txt");
    out.write((char*)&tracks, sizeof(tracks));
    return 0;
}
