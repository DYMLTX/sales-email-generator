# Artist-Brand Matching Analysis Plan

## Data Processing Tasks

- [ ] **Parse Artist Audience Data from Excel**
  - Extract gender split percentages
  - Parse ethnicity indices (+ positive, - negative)
  - Extract age ranges with indices
  - Parse household income brackets with indices
  - Extract consumer attributes with relevance scores

- [ ] **Create Standardized Demographic Mapping**
  - Map artist age ranges (16-20, 21-29, etc.) to brand age ranges (28-43, 44-59, etc.)
  - Convert artist income brackets to match brand formats
  - Align ethnicity categories between datasets
  - Normalize index values to comparable scores

- [ ] **Build Similarity Scoring Algorithm**
  - Weight demographic factors (age, gender, income, ethnicity)
  - Create scoring for consumer attribute matches
  - Handle index values (over/under indexing)
  - Normalize scores to 0-100 scale
  - Implement distance metrics (Euclidean, Cosine similarity)
  - Calculate confidence intervals for match scores

## Matching Process

- [ ] **Query High-Spend Brand Data**
  - Filter brands with >$5M media spend
  - Extract parsed Audience_Attributes__c JSON
  - Get brand industries for affinity matching
  - Pull audience descriptions for context

- [ ] **Create Industry-to-Consumer-Attribute Mapping**
  - Build lookup table: Consumer attributes → Brand industries
  - Map "Hard Seltzer" → "beer, wine, liquor" industries
  - Map "Coffee Houses" → Coffee/QSR brands
  - Map "Dog Owners" → Pet supply brands
  - Calculate attribute-industry correlation strengths
  - Use TF-IDF on Audience_Description__c for keyword extraction

- [ ] **Calculate Match Scores with Statistical Rigor**
  - Demographic similarity score (Cosine similarity)
  - Consumer attribute affinity score (Weighted Jaccard index)
  - Geographic alignment bonus (Binary or distance-based)
  - Industry-affinity bonus based on consumer attributes
  - Calculate standard error for each component score
  - Generate composite score with confidence intervals

- [ ] **Score All Brand-Artist Combinations**
  - Calculate scores for all 3 artists × 1,246 brands (3,738 total matches)
  - No filtering - return complete match matrix
  - Include all scoring components in output dataset
  - Sort by composite match score descending per artist

## Analysis & Output

- [ ] **Consider Secondary Factors**
  - Brand's typical partnership history
  - Industry alignment with artist genre
  - Regional market overlap

- [ ] **Generate Match Report**
  - Why each match works
  - Key demographic alignments
  - Potential campaign angles
  - Match confidence levels

- [ ] **Export Comprehensive Results Dataset**
  - CSV/Excel with all 3,738 artist-brand combinations
  - Include columns:
    - Artist_Name, Brand_Name, Brand_Industry
    - Composite_Match_Score (0-100)
    - Demographic_Score, Attribute_Score, Geographic_Score
    - Age_Similarity, Gender_Similarity, Income_Similarity, Ethnicity_Similarity
    - Consumer_Attribute_Matches (count)
    - Confidence_Interval_Lower, Confidence_Interval_Upper
    - Match_P_Value, Match_Tier
    - Rank_Within_Artist
  - Sort by Artist then Composite_Match_Score DESC
  - Include data dictionary/metadata sheet

## Statistical Validation & Reporting

- [ ] **Model Validation Metrics**
  - Calculate R² for demographic matching components
  - Report RMSE for continuous variables (age, income)
  - Chi-square test for categorical alignment (gender, ethnicity)
  - F-statistic for overall model significance
  - Cross-validation with held-out brand samples

- [ ] **Match Quality Metrics**
  - Confidence intervals (95%) for each match score
  - P-values for statistical significance of matches
  - Effect sizes (Cohen's d) for demographic differences
  - Sensitivity analysis on weight parameters
  - Bootstrap sampling for score stability

- [ ] **Diagnostic Reporting**
  - Residual plots for score distributions
  - QQ plots for normality checks
  - Correlation matrix between scoring components
  - VIF for multicollinearity detection
  - Match score histograms by artist

## Key Considerations

### Demographic Weighting (with optimization)
- **Primary factors** (60% weight): Age, Gender, Income
- **Secondary factors** (30% weight): Ethnicity, Consumer Attributes
- **Bonus factors** (10% weight): Geographic alignment, Industry affinity
- Weights to be optimized using gradient descent or grid search

### Match Quality Tiers (with statistical thresholds)
- **Exceptional** (85-100): >2σ above mean match score
- **Strong** (70-84): 1-2σ above mean
- **Good** (55-69): Within 1σ of mean
- **Fair** (40-54): Below mean but within acceptable range

### Success Metrics
- All 3,738 artist-brand combinations scored and ranked
- Model R² > 0.7 for primary demographic factors
- Clear statistical support for match quality differentiation
- Interpretable scoring components for business decisions
- P-values to distinguish significant vs random matches

## Industry-Attribute Mapping Examples

### Direct Mappings
- "Hard Seltzer" → Beverage alcohol brands (correlation: 0.85)
- "Coffee Houses" → QSR/Coffee brands (correlation: 0.78)
- "Dog Owners" → Pet care brands (correlation: 0.92)
- "Movie Goers" → Entertainment/Streaming (correlation: 0.71)

### Indirect Mappings
- "Moms" → Family-oriented brands, CPG, Retail
- "Premium/Quality Conscious" → Luxury brands, High-end retail
- "Budget Conscious" → Value retailers, Discount brands
- "Travelers" → Airlines, Hotels, Travel services

### Validation Approach
- Test mappings against known successful partnerships
- Calculate lift metrics for predicted vs actual partnerships
- A/B test recommendations against random pairings