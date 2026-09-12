---
title: CONTRIBUTING.md
description: How to run the toolkit and add a metric without breaking the surface pipeline.
created: 2026-09-07
updated: 2026-09-12
---

# Contributing

## Run locally

```bash
pip install -r requirements.txt
python3 build_surface_dataset.py
python3 generate_mock_data.py
python3 -m http.server 8000
```

Open `http://localhost:8000/demos/appendixC_surface_demo.html`.

## Add a metric

1. Document the metric in the essay / Appendix B language (what it measures, falsifiability).
2. Add baseline / threshold fields to `config/appendix_c_taxonomy.yaml` under `statistical_reference` when applicable.
3. Wire the field in `src/appendix_c/session_classifier.py` or `appendix_c_session_dissection.py`.
4. If the surface should show it, extend `build_surface_dataset.py` output schema and the demo hover/filter UI.
5. Regenerate mock data so Pages/demo stay coherent: `python3 generate_mock_data.py`, then refresh `docs/demo/mock_data/` if the Pages copy changed.

## Do not

- Commit private session logs or secrets.
- Weaken safety / ethics notes from Appendix D for convenience.
- Invent baselines. Calibrate from real data, or mark the file as mock.
- Present the essay as a peer-reviewed journal paper.