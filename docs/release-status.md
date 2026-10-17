# Release status

**Status: not ready for public release.** The full three-source assembly is incomplete. Public release of Website text remains blocked while its source-site redistribution basis is unresolved.

## Build progress

The local durable checkpoint has 784 of 1,533 partitions on plan `cb43a674c81643dadc20b727844c014d06e9e1335820fc8cef243f80d8bb5480`. It contains 386 Description partitions, 386 Website partitions, and 12 Wiki partitions. Description and Website are complete in this plan.

Grid5000 job 3149489 on `chartreuse7-1` owns 749 Wiki partitions. An exact comparison of its worklist with the 12 local Wiki manifest IDs found no overlap. The job has committed Armenia and Asturias and was joining Australia at the last observed heartbeat on 2026-10-17 15:07 UTC. Its outputs remain on node-local temporary storage and have not been copied back to the Seagate checkpoint.

The local heartbeat is stale, and no local assembler process is running. The `status` field in `assembly-progress.json` is not evidence of a live process.

## Remaining release gates

- Return the Grid5000 outputs to Seagate with checksums, then verify and reconcile all 1,533 manifests and source totals.
- Build H3 aggregates, maps, and downloadable summaries from the complete output.
- Verify source attribution, Wiki document license fields, language inventories, historical lineage, and assembly resource records.
- Resolve Website text redistribution rights before publishing the complete dataset or any Website text.
- Complete the remaining quality gates tracked in issue #2, including strict CRAP, global mutation, architecture, and final acceptance checks.

The local assembly keeps Description, Website, and Wiki in scope. No Website text is approved for public upload while the rights gate remains unresolved. See [Licensing and attribution](licensing-and-attribution.md).
