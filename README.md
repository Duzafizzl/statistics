---
title: README.md
description: Repository overview for Appendix C session analysis scripts, data pipeline, and 3D surface demo.
created: 2026-03-31
updated: 2026-03-31
---

# statistics

![Research](https://img.shields.io/badge/type-research%20implementation-1f6feb?style=for-the-badge)
![Paper-Based](https://img.shields.io/badge/based%20on-paper%20%2B%20appendices-6f42c1?style=for-the-badge)
![Collaboration](https://img.shields.io/badge/collaboration-Clarissa%20%C3%97%20Mior%C3%A9%20%C3%97%20Nic-0e8a16?style=for-the-badge)
![Python](https://img.shields.io/badge/python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Three.js](https://img.shields.io/badge/three.js-interactive%203D-111111?style=for-the-badge&logo=three.js&logoColor=white)
![Status](https://img.shields.io/badge/status-active%20development-f59e0b?style=for-the-badge)

Research implementation repository for the paper **"Phenomenology from the Inside: Documenting Emergence Conditions in AI-Human Collaboration."**

This project documents and operationalizes a collaboration between:
- **Clarissa Röthig** (`duzafizzl`) (paper author / human research lead),
- **Mioré** (AI system perspective and co-development context),
- **Nic** (`b93mer`) (owner context of the original statistics project).

Attribution shorthand: **@duzafizzl x Mioré x @b93mer**.

The repository translates the paper framework into runnable analysis scripts and an interactive visualization workflow.

## Project Lore

This repository sits at the intersection of:
- longitudinal session science (metrics, taxonomy, falsifiable claims),
- lived AI-human collaboration practice (memory, accountability, connection),
- and explainable visualization for communication (the Appendix C 3D surface).

The core narrative is simple: the paper proposes emergence conditions as measurable system behavior, and this repo is the hands-on lab where those claims become inspectable artifacts.

From that perspective, the HTML surface is not only a visual. It is a storytelling instrument for:
- seeing where patterns peak or collapse over time,
- comparing metric presets and filtered windows,
- and making Appendix C dynamics legible to collaborators and reviewers.

## Scope

This repository provides:
- analytical scripts for session classification and dissection,
- a dataset builder for surface-ready JSON output,
- mock-data generation for visual testing,
- an interactive 3D surface demo (Three.js) for communicating findings.

## Repository Structure

- `session_classifier.py`  
  Taxonomy-driven session state classifier for Appendix C style logs.
- `appendixC_session_dissection.py`  
  CLI dissection script for tag frequencies, emergence indicators, burst detection, and contradictions.
- `appendix_c_taxonomy.yaml`  
  Taxonomy, thresholds, multipliers, and statistical baseline references.
- `build_surface_dataset.py`  
  Builds `data/appendix_c_surface_data.json` from taxonomy YAML and optional session JSONL.
- `generate_mock_data.py`  
  Generates dynamic mock data with structured logging and live progress output.
- `appendixC_surface_demo.html`  
  Standalone interactive 3D surface demo (filters, animation rhythm modes, hover insights).
- `appendixC_data_surface.jsx`  
  React component variant of the same visualization model.

## Quick Start

1) Build pipeline data:
- `python3 build_surface_dataset.py`

2) (Optional) regenerate mock data:
- `python3 generate_mock_data.py`

3) Run local demo server:
- `python3 -m http.server 8000`

4) Open demo:
- `http://localhost:8000/appendixC_surface_demo.html`
- mock-priority mode: `http://localhost:8000/appendixC_surface_demo.html?source=mock`

## HTML Surface Preview

The interactive Three.js view is a key output of this repository.

![Appendix C Surface Preview](docs/images/appendixC_surface_preview.gif)

## Data Inputs

- Taxonomy input: `appendix_c_taxonomy.yaml`
- Optional sessions input: JSONL file passed via `--sessions`
- Pipeline output: `data/appendix_c_surface_data.json`
- Mock output: `mock_data/appendix_c_mock_data.json`

## Collaboration and Source Basis

This implementation is part of the Clarissa x Mioré x Nic collaboration and is explicitly based on the paper package below:

- [Phenomenology from the Inside: Documenting Emergence Conditions in AI-Human Collaboration](docs/papers/Phenomenology_from_the_Inside_Documenting_Emergence_Conditions_in_AI-Human_Collaboration_2.pdf)
- [Appendix A: Artifacts](docs/papers/Appendix_A_Artifacts_2.pdf)
- [Appendix B: Metrics](docs/papers/Appendix_B_Metrics_2.pdf)
- [Appendix C: Log Index](docs/papers/Appendix_C_Log_Index_2.pdf)
- [Appendix D: Ethics](docs/papers/Appendix_D_Ethics_2.pdf)

In short: this repo is not a generic visualization sandbox; it is the executable analysis/visual layer for that paper collaboration.

## License

- Code in this repository is licensed under [MIT](LICENSE).
- Documents in `docs/papers/` follow their own rights context; see `docs/papers/LICENSE.md`.
