# GeoReSeT Atlas

GeoReSeT Atlas is a versioned, reproducible assembly of the existing labeled OpenStreetMap text mirrors for land-use and land-cover relevance. This repository will contain the schema, bounded builder, validation, aggregation, and methods documentation; bulk Parquet releases belong in the companion [Hugging Face dataset](https://huggingface.co/datasets/NoeFlandre/georeset-atlas).

**Status:** implementation is in progress. The final Atlas v0 dataset has not been assembled or published. See the [open work issues](https://github.com/NoeFlandre/georeset-atlas/issues) and draft pull request for current scope and verification.

Source mirrors are read-only inputs pinned by immutable revisions. The release will preserve source provenance, original text occurrences and label limitations, and will not add model inference or semantic habitat classes.
