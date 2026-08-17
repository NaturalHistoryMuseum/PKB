graph-up:
	cd docker/janusgraph && docker compose up -d

graph-down:
	cd docker/janusgraph && docker compose down

graph-logs:
	cd docker/janusgraph && docker compose logs -f

neo4j-up:
	cd docker/neo4j && docker compose up -d

neo4j-down:
	cd docker/neo4j && docker compose down

localstack-up:
	cd docker/localstack && docker compose up -d

localstack-down:
	cd docker/localstack && docker compose down

localstack-logs:
	cd docker/localstack && docker compose logs -f
