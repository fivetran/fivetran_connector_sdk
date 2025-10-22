# Motion Blend Quality Metrics

This connector supports comprehensive quality metrics for evaluating blended motion sequences.

## Metrics Overview

### 1. L2 Velocity
**Formula:** `Δv(t,j) = |v(t,j) - v(t-1,j)` where `v(t,j) = ||v(t,j)||₂`

Measures joint speed discontinuities between consecutive frames.

### 2. L2 Acceleration  
**Formula:** `ΔΔv(t,j) = |Δv(t,j) - Δv(t-1,j)|`

Measures higher-order smoothness (acceleration changes).

### 3. Fréchet Inception Distance (FID)
Single-sample variant measuring distribution similarity. Lower is better.

### 4. Coverage (Cov)
Fraction of real motions covered by generated samples. Higher is better (0-1).

### 5. Diversity Metrics
- **Global Diversity (GDiv)**: Variance across entire sequence
- **Local Diversity (LDiv)**: Average variance in sliding windows
- **Inter Diversity**: Variance between different joints (spatial)
- **Intra Diversity**: Variance within each joint trajectory (temporal)

### 6. Transition Smoothness
Composite metric evaluating blend region quality (0-1, higher is better).

## Key Joints Tracked
- Pelvis (root/hips)
- Left Wrist
- Right Wrist
- Left Foot
- Right Foot

## Quality Scoring

Overall quality score: `0.4*smoothness + 0.3*diversity + 0.3*fid_coverage`

**Categories:**
- >= 0.80: Excellent
- 0.65-0.79: Good
- 0.50-0.64: Acceptable
- < 0.50: Poor

## BigQuery Schema

Metrics are stored in the destination with the following additional columns:

```json
{
  "fid": "FLOAT",
  "coverage": "FLOAT",
  "global_diversity": "FLOAT",
  "local_diversity": "FLOAT",
  "inter_diversity": "FLOAT",
  "intra_diversity": "FLOAT",
  "l2_velocity_mean": "FLOAT",
  "l2_velocity_max": "FLOAT",
  "l2_acceleration_mean": "FLOAT",
  "transition_smoothness": "FLOAT",
  "quality_score": "FLOAT",
  "quality_category": "STRING"
}
```

## References

- Tselepi et al. (2025) "Controllable Single-Shot Animation Blending with Temporal Conditioning"
- Guo et al. (2020) "Action2Motion: Generating Diverse and Natural Actions"
- Petrovich et al. (2021) "Action-Conditioned 3D Human Motion Synthesis with Transformers"
- Heusel et al. (2017) "GANs Trained by a Two Time-Scale Update Rule"

## Usage

The connector automatically extracts basic motion metadata. For full metrics computation:

1. **Sync motion files** using this connector
2. **Compute metrics** using analysis pipeline:
   ```bash
   python compute_blend_metrics.py \
       --blend-file gs://bucket/blends/my_blend.bvh \
       --transition-start 120 \
       --transition-end 180
   ```
3. **Load metrics** back to BigQuery via separate ingestion
4. **Join with motion data** in dbt transformations

See the full MotionBlendAI repository for complete metrics implementation.
