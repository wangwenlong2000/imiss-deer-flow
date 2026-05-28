# Space Data Processing - Validations

## Surface Analysis Without Atmospheric Correction

### **Id**
no-atmospheric-correction
### **Severity**
error
### **Type**
regex
### **Pattern**
  - ndvi.*level_1(?!.*correct)
  - surface.*reflectance(?!.*atmospheri)
  - toa.*analysis(?!.*correct)
### **Message**
Surface analysis may be using TOA data without atmospheric correction
### **Fix Action**
Apply atmospheric correction before surface reflectance analysis
### **Applies To**
  - **/*.py

## Analysis Without Cloud Masking

### **Id**
no-cloud-mask
### **Severity**
error
### **Type**
regex
### **Pattern**
  - classify(?!.*cloud|mask|qa)
  - spectral.*index(?!.*cloud)
  - ndvi.*=(?!.*mask)
### **Message**
Classification/analysis may not apply cloud masking
### **Fix Action**
Apply cloud and shadow mask before analysis
### **Applies To**
  - **/*.py

## SAR Analysis Without Speckle Filtering

### **Id**
sar-no-speckle-filter
### **Severity**
warning
### **Type**
regex
### **Pattern**
  - sar.*classify(?!.*filter|speckle)
  - sigma0.*analysis(?!.*lee|frost|multilook)
  - backscatter(?!.*denoise)
### **Message**
SAR analysis may not include speckle filtering
### **Fix Action**
Apply Lee, Frost, or multilooking before SAR classification
### **Applies To**
  - **/*.py

## Spectral Index Division by Zero

### **Id**
division-zero-index
### **Severity**
error
### **Type**
regex
### **Pattern**
  - \(nir\s*-\s*red\)\s*/\s*\(nir\s*\+\s*red\)(?!.*where|np\.where)
  - ndvi\s*=(?!.*eps|1e-10|\+\s*1)
### **Message**
Spectral index calculation may divide by zero for dark pixels
### **Fix Action**
Add small epsilon or use np.where to handle zero denominators
### **Applies To**
  - **/*.py

## Classification Without Accuracy Assessment

### **Id**
no-validation-accuracy
### **Severity**
warning
### **Type**
regex
### **Pattern**
  - predict.*land_cover(?!.*accuracy|valid)
  - classifier\.fit(?!.*test|valid)
  - classification.*output(?!.*assess)
### **Message**
Classification may lack accuracy assessment
### **Fix Action**
Calculate confusion matrix and accuracy metrics on validation set
### **Applies To**
  - **/*.py

## Training on Single Scene Only

### **Id**
single-scene-training
### **Severity**
warning
### **Type**
regex
### **Pattern**
  - train.*single.*scene
  - training_data.*=.*load\((?!.*multi|list)
### **Message**
Training on single scene may not generalize
### **Fix Action**
Include training samples from multiple scenes/dates
### **Applies To**
  - **/*.py

## Missing Radiometric Calibration

### **Id**
no-radiometric-cal
### **Severity**
warning
### **Type**
regex
### **Pattern**
  - raw.*dn.*analysis
  - digital.*number(?!.*calibrat)
  - band.*data(?!.*radiance|reflect)
### **Message**
Analysis may be using uncalibrated DN values
### **Fix Action**
Convert DN to radiance or reflectance before analysis
### **Applies To**
  - **/*.py

## Processing Output Without Units

### **Id**
no-units-documented
### **Severity**
info
### **Type**
regex
### **Pattern**
  - save.*result(?!.*units|metadata)
  - write.*tif(?!.*scale|description)
  - output(?!.*document)
### **Message**
Output products may lack unit documentation
### **Fix Action**
Document units and scale factors in output metadata
### **Applies To**
  - **/*.py