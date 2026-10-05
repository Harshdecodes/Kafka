# Kafka air-quality event pipeline

This project demonstrates message passing with Apache Kafka. A Python producer generates mock PM2.5 and CO2 readings for several monitoring stations. A separate consumer validates each event, marks it ALERT or NORMAL, and saves it in SQLite. Kafka, the producer, and the consumer run in separate containers.

## Architecture

    Producer -- keyed events --> Kafka (3 partitions) --> Consumer --> SQLite --> Report
                                 ^                       |
                                 +-- offset commit -------+

Every event has a UUID event_id, a sensor_id, an ISO 8601 UTC observed_at, pm25_ug_m3, and co2_ppm. The demo marks a reading ALERT when PM2.5 is at least **35 ug/m3** or CO2 is at least **1000 ppm**. These are illustrative thresholds for the assignment, not a health assessment. The station ID is the Kafka key, which keeps readings for the same station ordered within one partition.

## Run locally

Install Docker Engine and Docker Compose, then run these commands inside this folder:

    docker compose up -d --build kafka init-topic consumer
    docker compose run --rm producer --count 20 --interval 0.2 --sensors 3
    docker compose exec consumer python -m air_quality_pipeline.report --limit 20

The report displays recent readings and counts by status. Run the producer again for more data; use the following command to see records as they are processed:

    docker compose logs -f consumer

The command docker compose down stops containers and preserves data. The command docker compose down -v also removes the Kafka and SQLite volumes and resets the demo.

Kafka is reachable from the host at localhost:39092; containers use kafka:19092. The host listener binds only to loopback. This single-broker setup is for local demonstration.

Example report lines (actual readings vary):

    2026-10-05T10:30:00+00:00 station-01 PM2.5= 41.2 ug/m3 CO2=  720 ppm ALERT  p=1 o=4
    2026-10-05T10:30:01+00:00 station-02 PM2.5= 12.7 ug/m3 CO2=  650 ppm NORMAL p=2 o=2
    total=20 alert=8 normal=12

## Delivery behavior

The producer enables idempotence and waits for broker acknowledgements. The consumer stores each reading in a SQLite transaction and then commits its Kafka offset synchronously. A crash after the insert but before the offset commit can replay the event; unique constraints on event_id and topic/partition/offset make the storage step idempotent. This is **at-least-once delivery with replay-safe storage**. Malformed records stop the consumer without committing their offset so the error can be investigated.

## Tests and CI

Run the tests locally:

    python3 -m venv .venv
    . .venv/bin/activate
    pip install -r requirements-dev.txt
    python -m pytest -q
    docker compose config -q

The tests cover serialization, alert thresholds, input validation, duplicate events, and key mismatches. The included [GitHub Actions workflow](.github/workflows/ci.yml) runs tests and a Docker Compose producer-to-consumer smoke test on pushes and pull requests if this folder is later published to GitHub. The optional local pre-push hook blocks a push when tests fail; install it in a Git repository with:

    git config core.hooksPath .githooks

To enforce checks on GitHub's main branch, require the workflow's test check in branch protection settings.

## 2-3 minute class explanation

1. **Overview:** Monitoring stations publish PM2.5 and CO2 readings. The producer sends them to Kafka, and the consumer saves classified results in SQLite.
2. **Distributed design:** The producer and consumer run separately and communicate through a three-partition Kafka topic. The station ID is the key, so events from one station stay ordered.
3. **Reliability:** The consumer saves first, then commits the offset. If Kafka delivers a record again, the database's unique keys prevent a duplicate row.
4. **Live demo:** Run the producer for five events and show the report. Point out both ALERT and NORMAL readings and the updated total.

## References

- [Apache Kafka Docker image and Compose examples](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md)
- [Confluent Python client: producer flushing and manual consumer commits](https://docs.confluent.io/kafka-clients/python/current/overview.html)
