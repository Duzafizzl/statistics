---
title: ARCHITECTURE.md
description: How paper appendices map to taxonomy, scripts, JSON, the companion page, and the 3D surface.
created: 2026-09-07
updated: 2026-09-07
---

# Architecture

```text
Paper (Appendices A to D)
        |
        v
config/appendix_c_taxonomy.yaml
  taxonomy, thresholds, monthly / tag baselines
        |
        |---> session_classifier / appendix_c_session_dissection
        |         (JSONL session logs -> metrics / flags)
        |
        +---> build_surface_dataset
                  |
                  v
            data/appendix_c_surface_data.json
                  |
                  v
            demos/appendixC_surface_demo.html  (Three.js, local)
            docs/                              companion page
            docs/demo/                         surface on GitHub Pages
```

## Data flow

1. **Taxonomy YAML** defines labels, baselines, and statistical reference blocks used by classifiers and the surface builder.
2. **Optional session JSONL** (Appendix C.7 format) can override baselines with live aggregates. Private transcripts are not shipped.
3. **Surface JSON** feeds the interactive demo (filters, hover insights, animation modes). Paths written into JSON are repo-relative.
4. **Mock data** (`mock_data/`) is a synthetic 12-month topology for the demo: Nov 2025 to Jan 2026 use Appendix C.9 totals; later months are fill for ridges only.

## Notes

- `miu_session_logger.py` is an upstream helper for Appendix C.7-style exports. The surface demo does not require it.
- Prefer `?source=pipeline` on the demo only when you have built richer surface JSON locally.

## Design intent

The paper treats emergence conditions as measurable system behavior. This repo is the executable layer: same claims, inspectable artifacts. Peaks, collapses, and filtered windows should be visible without reading raw logs.
