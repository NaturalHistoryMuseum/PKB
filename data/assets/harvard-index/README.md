# Harvard Index Archived Assets

This directory contains archived Harvard Index of Botanists source material used
by PKB because the original Harvard Index website workflow is no longer
available for repeatable scraping.

## Files

`huh-agents.tab.txt` is the Harvard agent list originally downloaded from:

```text
https://huh-it.s3.amazonaws.com/huh-agents.tab.txt
```

`huh-collectors.csv` is the historical scrape output produced by
`src/pkb/tasks/harvard.py`. That task used `huh-agents.tab.txt` as its input,
visited the Harvard Index botanist detail endpoint for each agent, parsed the
detail table, joined in derived geography and institution fields, and wrote the
collector table.

## Current Use

The active PKB pipeline treats `huh-collectors.csv` as an archived local source.
`HarvardIndexCollectorsSourceTask` stores the exact CSV bytes in versioned S3
and writes a `DatasetVersion` manifest. `HarvardIndexPeopleTask` then builds the
people-domain parquet from that stored source object.
