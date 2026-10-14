# Results

Two documents here, both written for an honest account of what was built and what the live numbers actually show:

- **[`BUILD_LOG.md`](BUILD_LOG.md)** — the full, task-by-task build narrative: what each of the 17 implementation tasks delivered, what was independently verified, and — where it matters — what went wrong against the real cluster and how it was found and fixed. Covers the infrastructure build, the five-stage pipeline, getting all three chaos scenarios to actually fire end-to-end, the evaluation harness, and the final whole-branch review that caught three methodological issues in the evaluation numbers themselves before merge.
- **[`EVALUATION.md`](EVALUATION.md)** — the real evaluation results from the live cluster (5 repeats per scenario), with the same honest caveats the raw data carries: which numbers are current, which predate a correctness fix, and why.

Raw, timestamped harness output lives in [`harness/results/`](../harness/results/) — `EVALUATION.md` is the readable summary of those files, not a replacement for them.

See the repository root [`README.md`](../README.md) for setup and run instructions.
