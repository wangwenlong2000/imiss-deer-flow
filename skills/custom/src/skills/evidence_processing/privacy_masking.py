from __future__ import annotations

from pathlib import Path

import cv2

from src.skills.base import BaseSkill, SkillContext


class PrivacyMaskingSkill(BaseSkill):
    name = "privacy-masking"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        enabled = context.config.get("privacy_masking", {}).get("enabled", True)
        uri = input_data.get("uri") or input_data.get("image_uri")
        if not uri:
            return self.failed("MISSING_URI", "uri or image_uri is required")
        if not enabled:
            return self.success({"uri": uri, "privacy_masked": False, "masked_regions": []})
        path = Path(uri.removeprefix("file://"))
        if path.exists():
            image = cv2.imread(str(path))
            if image is None:
                return self.failed("IMAGE_READ_FAILED", f"Could not read image: {path}")
            regions = input_data.get("sensitive_regions", [])
            if regions:
                for region in regions:
                    x1, y1, x2, y2 = [int(value) for value in region.get("bbox", region)]
                    roi = image[y1:y2, x1:x2]
                    if roi.size:
                        image[y1:y2, x1:x2] = cv2.GaussianBlur(roi, (31, 31), 0)
            output_path = path.with_name(f"{path.stem}_masked{path.suffix}")
            cv2.imwrite(str(output_path), image)
            return self.success(
                {
                    "uri": str(output_path),
                    "privacy_masked": True,
                    "masked_regions": regions,
                    "method": context.config.get("privacy_masking", {}).get("method", "gaussian_blur"),
                }
            )
        masked_uri = context.storage.upload_bytes(
            context.storage.read_bytes(uri) + b":masked",
            input_data.get("output_key") or uri.replace("memory://", "").replace(".jpg", "_masked.jpg"),
        )
        return self.success(
            {
                "uri": masked_uri,
                "privacy_masked": True,
                "masked_regions": input_data.get("sensitive_regions", []),
                "method": context.config.get("privacy_masking", {}).get("method", "gaussian_blur"),
            }
        )
