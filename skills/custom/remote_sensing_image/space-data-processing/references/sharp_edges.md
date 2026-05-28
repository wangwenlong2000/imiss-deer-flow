# Space Data Processing - Sharp Edges

## Analyzing TOA Reflectance as Surface

### **Id**
atmospheric-effects-ignored
### **Severity**
critical
### **Summary**
Atmospheric scattering makes vegetation look less green
### **Symptoms**
  - NDVI values lower than expected
  - Haze visible in imagery
  - Results don't match field data
### **Why**
  Top-of-atmosphere (TOA) reflectance includes atmospheric effects.
  Rayleigh scattering adds blue haze. Water vapor absorbs in specific
  bands. If you calculate NDVI from TOA, you'll get lower values
  than actual surface NDVI.
  
### **Gotcha**
  "Field measurements show NDVI = 0.7"
  "Satellite NDVI = 0.4"
  "That's a huge difference"
  "Did you apply atmospheric correction?"
  "No, I used Level 1 data directly"
  
  # TOA NDVI can be 20-30% lower than surface NDVI
  
### **Solution**
  1. Always use atmospherically corrected data for surface analysis:
     - Level 2 products
     - Apply 6S, FLAASH, or DOS
     - Use Analysis Ready Data (ARD)
  
  2. Know what you're analyzing:
     - TOA: okay for cloud detection, QA
     - Surface: required for vegetation, water, soil
  
  3. Verify correction:
     - Check for over/under correction
     - Compare to known targets
     - Dark objects should be near zero
  

## Classifier Fails on New Scene

### **Id**
training-data-bias
### **Severity**
high
### **Summary**
Model trained on one scene doesn't generalize
### **Symptoms**
  - High accuracy on training scene
  - Poor accuracy on different scene
  - Systematic misclassification
### **Why**
  Machine learning models learn the specific characteristics of
  training data. If trained on one scene, they learn that scene's
  illumination, atmospheric conditions, and phenology. A different
  scene has different conditions and the model fails.
  
### **Gotcha**
  "Classifier was 95% accurate on training"
  "Applied to new scene - 60% accuracy"
  "The training data was all from July"
  "The new scene is from October"
  
  # Seasonal variation, sun angle, atmospheric haze all differ
  
### **Solution**
  1. Diverse training data:
     - Multiple dates/seasons
     - Multiple sensors
     - Different atmospheric conditions
  
  2. Feature engineering:
     - Use indices rather than raw bands
     - Normalize for illumination
     - Include texture, context
  
  3. Validation strategy:
     - Test on completely separate scenes
     - Cross-validation across dates
     - Report per-scene accuracy
  

## Clouds Misclassified as Features

### **Id**
cloud-contamination
### **Severity**
high
### **Summary**
Bright clouds detected as snow, urban, or other bright targets
### **Symptoms**
  - Classification shows features that move between dates
  - Unusual patterns in analysis
  - Spikes in time series
### **Why**
  Clouds are spectrally similar to other bright targets. Without
  proper masking, they're classified as snow, bright soil, or
  buildings. Cloud shadows appear as water or dark vegetation.
  
### **Gotcha**
  "The classification shows a new lake"
  "That's weird, let me check the imagery"
  "It's a cloud shadow"
  "Where did our QA mask go?"
  
  # Clouds: highly reflective, shadows: low reflectance
  
### **Solution**
  1. Always mask clouds and shadows:
     - Use provided QA bands
     - Apply cloud detection algorithms
     - Buffer cloud edges
  
  2. Multi-temporal compositing:
     - Use best available pixel
     - Median composites
     - Harmonic fitting
  
  3. Quality metrics:
     - Track cloud percentage
     - Flag uncertain pixels
     - Document exclusion criteria
  

## SAR Analysis Overwhelmed by Speckle

### **Id**
sar-speckle-noise
### **Severity**
high
### **Summary**
Coherent noise makes classification impossible
### **Symptoms**
  - Salt and pepper appearance
  - Classification results noisy
  - Small features undetectable
### **Why**
  SAR is a coherent imaging system. Random phase interference creates
  speckle - multiplicative noise that varies pixel to pixel. Without
  filtering, signal-to-noise ratio is terrible for analysis.
  
### **Gotcha**
  "Trying to map wetlands from Sentinel-1"
  "Everything looks like static"
  "Standard deviation is huge"
  "You need to filter that first"
  
  # Speckle reduces effective SNR by factor of sqrt(looks)
  
### **Solution**
  1. Apply speckle filtering:
     - Lee, Frost, or Gamma-MAP
     - Match filter to application
     - Accept resolution loss
  
  2. Multi-looking:
     - Average multiple looks
     - Reduces noise, reduces resolution
     - Common for classification
  
  3. Ensemble approach:
     - Use multiple images
     - Temporal averaging
     - Reduces speckle statistically
  

## Misregistered Bands Cause False Features

### **Id**
band-registration-error
### **Severity**
medium
### **Summary**
Band misalignment creates artifacts in indices
### **Symptoms**
  - Edge effects in spectral indices
  - Halos around features
  - Systematic patterns following edges
### **Why**
  If bands aren't perfectly co-registered, edges create artifacts.
  When calculating ratios (NDVI = (NIR-Red)/(NIR+Red)), a 1-pixel
  misalignment at a field edge can create large spurious values.
  
### **Gotcha**
  "There's a bright line around every field in my NDVI"
  "That's not real vegetation"
  "It's a registration artifact"
  "NIR and Red are offset by 0.5 pixels"
  
  # Sub-pixel misregistration common in some sensors
  
### **Solution**
  1. Check band registration:
     - Visual inspection at edges
     - Cross-correlation check
     - Use pan-sharpened if available
  
  2. Resampling:
     - Use appropriate interpolation
     - Consistent resampling for all bands
     - Document resolution
  
  3. Accept limitations:
     - Avoid analysis at sharp edges
     - Use larger analysis units
     - Field boundaries particularly sensitive
  