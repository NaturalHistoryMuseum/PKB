# Planetary Knowledge Base

The Planetary Knowledge Base (PKB) builds a connected biodiversity knowledge graph from structured source datasets, curated resolution outputs, and later intelligence-derived evidence.

Phase 1 focuses on the information backbone: People, Taxonomy and Nomenclature, Specimens, Places and Localities, Institutions and Collections, Publications and Literature, and Conservation Assessments.

## Refactor Principles

The codebase separates source materialisation, dataset building, entity resolution, and graph loading. Each task should have one clear responsibility and one durable output contract.

### Source Tasks Produce Versioned Source Manifests

Tasks under `src/pkb/tasks/sources/` acquire raw upstream material and store it in versioned object storage. Their normal Luigi output is a `DatasetVersion` JSON manifest under `data/intermediate/sources/`.

Source tasks may call external APIs, download files, or page through remote services, but they should preserve the upstream response as raw bytes. They should not parse source data into PKB-shaped tables, assign PKB entity identifiers, or write to Neo4j.

Examples:

```text
pkb.tasks.sources.people.wikidata.WikiDataCollectorsSourceTask
  -> data/intermediate/sources/wikidata/people/collectors.dataset-version.json
  -> s3://pkb-source-data/wikidata/people/collectors/source.json

pkb.tasks.sources.institutions.grscicol.GRSciCollInstitutionsTSVSourceTask
  -> data/intermediate/sources/grscicol/institutions.dataset-version.json
  -> s3://pkb-source-data/grscicol/institutions/source.tsv
```

### Build Tasks Produce Files

Tasks under `src/pkb/tasks/build/` materialise datasets. Their normal output is a parquet file under `data/intermediate/`.

Build tasks read source manifests, retrieve the exact raw source objects from S3, parse source files or API responses, normalise source fields, assign PKB build keys, or aggregate source records into a domain table. They should not call remote APIs directly or write to Neo4j.

Examples:

```text
pkb.tasks.build.institutions.grscicol.GRSciCollInstitutionsTSVTask
  -> data/intermediate/institutions/grscicol_institutions.parquet

pkb.tasks.build.institutions.grscicol.GRSciCollCollectionsTSVTask
  -> data/intermediate/institutions/grscicol_collections.parquet

pkb.tasks.build.institutions.institutions.InstitutionsTask
  -> data/intermediate/institutions/institutions.parquet
```

### Load Tasks Write To Neo4j

Tasks under `src/pkb/tasks/load/` import built parquet datasets into the graph. They should not fetch remote source data or perform heavy source normalisation.

Node loaders read parquet rows and create or update graph nodes. Edge loaders read parquet rows and create graph relationships. Domain loaders combine the node and edge loaders needed for a knowledge domain.

Examples:

```text
pkb.tasks.load.nodes.institutions.LoadInstitutionNodes
pkb.tasks.load.edges.collection_institution_edges.LinkCollectionsToInstitutions
pkb.tasks.load.domains.institutions.LoadInstitutionsDomainTask
```

### Source Records Stay Distinct From PKB Entities

Source identifiers and source-specific fields should be preserved. Reconciled PKB entities should be built from source records rather than replacing them.

Use explicit fields for source identifiers, candidate matches, accepted matches, confidence, provenance, and processing history where those are available.

### Knowledge Domains Own Their Outputs

Organise build tasks by knowledge domain, not primarily by source. Source-specific tasks live inside the domain they contribute to.

For example, Wikidata contributes to both People and Institutions, so those tasks live in separate domain packages:

```text
src/pkb/tasks/build/people/wikidata.py
src/pkb/tasks/build/institutions/wikidata.py
```

### Neo4j First

The active graph target is Neo4j. JanusGraph code may remain in the repository, but it is not part of the active Phase 1 import path.

Graph labels and relationship names should be shared through `src/pkb/graph/schema.py` rather than repeated as raw strings in task files.

### Raw Source Data Lives In S3

Source tasks store raw upstream bytes in S3 and write a local manifest that points at the exact object version. Local development uses LocalStack S3; production uses AWS S3 through the same Boto3 client code.

The source storage helper returns a `DatasetVersion` record containing the dataset name, source URL, retrieval time, SHA-256, size, S3 bucket, S3 key, S3 version ID, and optional upstream version.

## Current Layout

```text
src/pkb/
  config.py
  graph/
    factory.py
    neo4j_writer.py
    schema.py
    writer.py
  storage/
    s3.py
  tasks/
    sources/
      people/
    build/
      people/
      institutions/
      taxonomy/
      specimens/
      places/
      publications/
      conservation/
    load/
      nodes/
      edges/
      domains/
    phase1.py
  wikidata/
  utils/
```

Compatibility modules such as `pkb.tasks.build.wikidata` and `pkb.tasks.build.grscicol` re-export classes from their new domain packages. New code should import from the domain package directly.

## Running Tasks

Build currently implemented Phase 1 parquet outputs:

```bash
luigi --module pkb.tasks.phase1 BuildPhase1BackboneTask --local-scheduler
```

Load currently implemented Phase 1 domains into Neo4j:

```bash
PKB_GRAPH_BACKEND=neo4j luigi --module pkb.tasks.phase1 LoadPhase1BackboneToNeo4jTask --local-scheduler
```

Run non-integration tests:

```bash
pytest -m "not integration"
```

## Local Source Storage

Start LocalStack S3:

```bash
make localstack-up
```

Configure local AWS-compatible credentials:

```bash
cp .env.example .env
```

Initialise the source-data bucket and enable versioning:

```bash
python -c "from pkb.storage import ensure_source_bucket, get_s3_client; ensure_source_bucket(s3=get_s3_client())"
```

In AWS production, do not set `PKB_S3_ENDPOINT_URL`; Boto3 will use the normal AWS endpoint and credential chain.

## Local Graph

Neo4j is the preferred local graph target for Phase 1 work. JanusGraph is intentionally ignored for now.
