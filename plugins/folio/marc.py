import logging

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


def post_marc_to_srs(*args, **kwargs):
    dag = kwargs["dag_run"]

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=[{
            "file_name": f"folio_srs_instances_{dag.run_id}_bibs-transformer.json"  # noqa
        }],
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    srs_batch_poster = BatchPoster(
        task_config,
        library_config,
        use_logging=False
    )

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logging.info("Finished posting MARC json to SRS")
