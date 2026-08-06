# Space Data Processing

## Patterns


---
  #### **Name**
optical_pipeline
  #### **Steps**
    - Quality assessment (cloud cover, gaps)
    - Radiometric calibration to TOA reflectance
    - Atmospheric correction to surface reflectance
    - Geometric correction / orthorectification
    - Cloud and shadow masking
    - Calculate spectral indices
    - Analysis / classification

---
  #### **Name**
sar_pipeline
  #### **Steps**
    - Apply orbit file
    - Radiometric calibration
    - Speckle filtering
    - Terrain correction
    - Convert to dB
    - Analysis

---
  #### **Name**
change_detection_pipeline
  #### **Steps**
    - Select bi-temporal images
    - Co-register to common geometry
    - Radiometric normalization
    - Apply change detection method
    - Threshold to binary change
    - Post-processing (morphology, filtering)
    - Accuracy assessment

## Anti-Patterns


---
  #### **Name**
ignoring_atmosphere
  #### **Problem**
Surface reflectance analysis inaccurate
  #### **Solution**
Apply atmospheric correction before surface analysis

---
  #### **Name**
training_single_scene
  #### **Problem**
Classifier overfits to one acquisition
  #### **Solution**
Use diverse training data across scenes/dates

---
  #### **Name**
no_cloud_mask
  #### **Problem**
False detections in clouds/shadows
  #### **Solution**
Always mask clouds and shadows

---
  #### **Name**
sar_without_speckle
  #### **Problem**
Noise overwhelms signal
  #### **Solution**
Apply appropriate speckle filter

---
  #### **Name**
mixed_sensors_no_harmonization
  #### **Problem**
Inconsistent results between sensors
  #### **Solution**
Harmonize radiometry between different sensors