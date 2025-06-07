#!/usr/bin/env python3

import time
import asyncio
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List

from fundermapssdk.command import FunderMapsCommand


class ProcessWorkerJobsCommand(FunderMapsCommand):
    """Command to poll and process jobs from the worker_jobs table."""

    def __init__(self):
        super().__init__(description="Process jobs from the worker_jobs table")

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument(
            "--poll-interval",
            type=int,
            default=30,
            help="Polling interval in seconds (default: 30)",
        )
        parser.add_argument(
            "--job-types",
            nargs="+",
            default=[],
            help="Specific job types to process (default: all)",
        )
        parser.add_argument(
            "--max-concurrent",
            type=int,
            default=3,
            help="Maximum number of concurrent jobs to process (default: 3)",
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Run one polling cycle and exit (default: continuous polling)",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=3600,
            help="Maximum job execution time in seconds (default: 3600)",
        )

    async def _get_pending_jobs(
        self, job_types: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch pending jobs from the worker_jobs table.

        Args:
            job_types: Optional list of job types to filter by

        Returns:
            List of job dictionaries
        """
        self.logger.debug("Fetching pending jobs")
        now = datetime.now(timezone.utc)

        with self.fundermaps.db as db:
            with db.db.cursor() as cur:
                query = """
                    SELECT 
                        id, job_type, payload, priority, retry_count, max_retries
                    FROM 
                        application.worker_jobs
                    WHERE 
                        status = 'pending'
                        AND (process_after IS NULL OR process_after <= %s)
                """
                params = [now]

                if job_types and len(job_types) > 0:
                    placeholders = ", ".join(["%s"] * len(job_types))
                    query += f" AND job_type IN ({placeholders})"
                    params.extend(job_types)

                query += " ORDER BY priority DESC, created_at ASC"
                cur.execute(query, params)

                # Convert rows to dictionaries
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    async def _mark_job_in_progress(self, job_id: int) -> bool:
        """
        Mark a job as in progress.

        Args:
            job_id: The ID of the job to update

        Returns:
            True if successful, False otherwise
        """
        self.logger.debug(f"Marking job {job_id} as in-progress")
        try:
            with self.fundermaps.db as db:
                with db.db.cursor() as cur:
                    query = """
                        UPDATE application.worker_jobs
                        SET status = 'processing', updated_at = NOW()
                        WHERE id = %s AND status = 'pending'
                        RETURNING id
                    """
                    cur.execute(query, (job_id,))
                    result = cur.fetchone()
                    db.db.commit()
                    return result is not None
        except Exception as e:
            self.logger.error(f"Failed to mark job {job_id} as in-progress: {e}")
            return False

    async def _mark_job_complete(self, job_id: int) -> None:
        """
        Mark a job as completed.

        Args:
            job_id: The ID of the job to update
        """
        self.logger.info(f"Marking job {job_id} as completed")
        try:
            with self.fundermaps.db as db:
                with db.db.cursor() as cur:
                    query = """
                        UPDATE application.worker_jobs
                        SET status = 'completed', updated_at = NOW()
                        WHERE id = %s
                    """
                    cur.execute(query, (job_id,))
                    db.db.commit()
        except Exception as e:
            self.logger.error(f"Failed to mark job {job_id} as completed: {e}")

    async def _mark_job_failed(
        self, job_id: int, error: str, retry: bool = True
    ) -> None:
        """
        Mark a job as failed, potentially scheduling a retry.

        Args:
            job_id: The ID of the job to update
            error: The error message
            retry: Whether to retry the job if retries are available
        """
        self.logger.warning(f"Marking job {job_id} as failed: {error}")
        try:
            with self.fundermaps.db as db:
                with db.db.cursor() as cur:
                    # First, get current retry information
                    cur.execute(
                        "SELECT retry_count, max_retries FROM application.worker_jobs WHERE id = %s",
                        (job_id,),
                    )
                    result = cur.fetchone()
                    if not result:
                        self.logger.error(
                            f"Job {job_id} not found when updating failure status"
                        )
                        return

                    retry_count, max_retries = result
                    new_retry_count = retry_count + 1

                    # Determine if we can retry
                    can_retry = retry and new_retry_count <= max_retries
                    new_status = "pending" if can_retry else "failed"

                    # Calculate next process time if retrying (exponential backoff)
                    process_after = None
                    if can_retry:
                        # Simple exponential backoff: 30s, 2m, 8m, etc.
                        backoff_seconds = 30 * (2 ** (new_retry_count - 1))
                        process_after = f"NOW() + INTERVAL '{backoff_seconds} seconds'"
                        self.logger.info(
                            f"Scheduling job {job_id} for retry in {backoff_seconds}s (attempt {new_retry_count}/{max_retries})"
                        )

                    # Update the job
                    query = f"""
                        UPDATE application.worker_jobs
                        SET 
                            status = %s, 
                            retry_count = %s,
                            last_error = %s,
                            process_after = {process_after if process_after else 'NULL'},
                            updated_at = NOW()
                        WHERE id = %s
                    """
                    cur.execute(query, (new_status, new_retry_count, error, job_id))
                    db.db.commit()
        except Exception as e:
            self.logger.error(f"Failed to mark job {job_id} as failed: {e}")

    async def _process_job(self, job: Dict[str, Any]) -> bool:
        """
        Process a single job based on its type.

        Args:
            job: The job dictionary

        Returns:
            True if successful, False otherwise
        """
        job_id = job["id"]
        job_type = job["job_type"]
        payload = job["payload"] if job["payload"] else {}

        self.logger.info(f"Processing job {job_id} of type {job_type}")

        try:
            # Different job types can be processed differently
            if job_type == "refresh_models":
                return await self._process_refresh_models_job(job_id, payload)
            elif job_type == "load_dataset":
                return await self._process_load_dataset_job(job_id, payload)
            elif job_type == "process_mapset":
                return await self._process_mapset_job(job_id, payload)
            elif job_type == "cleanup_storage":
                return await self._process_cleanup_storage_job(job_id, payload)
            elif job_type == "export_product":
                return await self._process_export_product_job(job_id, payload)
            elif job_type == "send_mail":
                return await self._process_send_mail_job(job_id, payload)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
        except Exception as e:
            self.logger.error(f"Error processing job {job_id}: {e}", exc_info=True)
            return False

    async def _process_refresh_models_job(
        self, job_id: int, payload: Dict[str, Any]
    ) -> bool:
        """Process a refresh_models job."""
        from refresh_models import ModelRefreshCommand

        self.logger.info(f"Running refresh_models job {job_id}")

        # Create command arguments
        args = argparse.Namespace()
        args.skip_risk = payload.get("skip_risk", False)
        args.skip_statistics = payload.get("skip_statistics", False)
        args.view = payload.get("view")

        # Run the command
        command = ModelRefreshCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_load_dataset_job(
        self, job_id: int, payload: Dict[str, Any]
    ) -> bool:
        """Process a load_dataset job."""
        from load_dataset import LoadDatasetCommand

        self.logger.info(f"Running load_dataset job {job_id}")

        # Create command arguments
        args = argparse.Namespace()
        args.dataset_input = payload.get("dataset_input")
        if not args.dataset_input:
            raise ValueError("Missing required field 'dataset_input' in job payload")

        args.layer = payload.get("layer", [])
        args.delete_after = payload.get("delete_after", False)
        args.tmp_dir = payload.get("tmp_dir")

        # Run the command
        command = LoadDatasetCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_mapset_job(self, job_id: int, payload: Dict[str, Any]) -> bool:
        """Process a process_mapset job."""
        from process_mapset import ProcessMapsetCommand

        self.logger.info(f"Running process_mapset job {job_id}")

        # Create command arguments
        args = argparse.Namespace()
        tileset_value = payload.get("tileset")
        args.tileset = tileset_value if isinstance(tileset_value, list) else [tileset_value] if tileset_value else []
        args.max_workers = payload.get("max_workers", 3)

        # Run the command
        command = ProcessMapsetCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_cleanup_storage_job(
        self, job_id: int, payload: Dict[str, Any]
    ) -> bool:
        """Process a cleanup_storage job."""
        from cleanup_storage import CleanupStorageCommand

        self.logger.info(f"Running cleanup_storage job {job_id}")

        # Create command arguments (no specific args for this command)
        args = argparse.Namespace()

        # Run the command
        command = CleanupStorageCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_export_product_job(
        self, job_id: int, payload: Dict[str, Any]
    ) -> bool:
        """Process an export_product job."""
        from export_product import ProductExportCommand

        self.logger.info(f"Running export_product job {job_id}")

        # Create command arguments
        args = argparse.Namespace()
        args.date = payload.get("date")

        # Run the command
        command = ProductExportCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_send_mail_job(
        self, job_id: int, payload: Dict[str, Any]
    ) -> bool:
        """Process a send_mail job."""
        from send_mail import SendMailCommand

        self.logger.info(f"Running send_mail job {job_id}")

        # Create command arguments
        args = argparse.Namespace()
        args.to = payload.get("to")
        if not args.to:
            raise ValueError("Missing required field 'to' in job payload")

        args.subject = payload.get("subject")
        if not args.subject:
            raise ValueError("Missing required field 'subject' in job payload")

        args.text = payload.get("text")
        if not args.text:
            raise ValueError("Missing required field 'text' in job payload")

        # Run the command
        command = SendMailCommand()
        command.args = args
        command.fundermaps = self.fundermaps
        command.logger = self.logger

        return await command.execute() == 0

    async def _process_jobs(
        self,
        semaphore: asyncio.Semaphore,
        job_types: List[str] = None,
        timeout: int = 3600,
    ) -> None:
        """
        Process pending jobs with concurrency control.

        Args:
            semaphore: Semaphore for controlling concurrency
            job_types: Optional list of job types to filter by
            timeout: Maximum job execution time in seconds
        """
        jobs = await self._get_pending_jobs(job_types)
        if not jobs:
            self.logger.debug("No pending jobs found")
            return

        self.logger.info(f"Found {len(jobs)} pending job(s)")

        async def process_job_with_semaphore(job):
            async with semaphore:
                job_id = job["id"]

                # Try to mark the job as in-progress
                if not await self._mark_job_in_progress(job_id):
                    self.logger.warning(
                        f"Job {job_id} could not be marked as in-progress, skipping"
                    )
                    return

                # Process the job with timeout
                try:
                    # Create a task with timeout
                    process_task = asyncio.create_task(self._process_job(job))
                    success = await asyncio.wait_for(process_task, timeout=timeout)

                    if success:
                        await self._mark_job_complete(job_id)
                    else:
                        await self._mark_job_failed(
                            job_id, "Job processing returned failure"
                        )
                except asyncio.TimeoutError:
                    self.logger.error(f"Job {job_id} timed out after {timeout} seconds")
                    await self._mark_job_failed(
                        job_id, f"Job execution timed out after {timeout} seconds"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error processing job {job_id}: {e}", exc_info=True
                    )
                    await self._mark_job_failed(job_id, str(e))

        # Create tasks for all jobs
        tasks = [process_job_with_semaphore(job) for job in jobs]
        await asyncio.gather(*tasks)

    async def execute(self) -> int:
        """Execute the process worker jobs command."""
        try:
            poll_interval = self.args.poll_interval
            job_types = self.args.job_types if self.args.job_types else None
            max_concurrent = self.args.max_concurrent
            run_once = self.args.run_once
            timeout = self.args.timeout

            semaphore = asyncio.Semaphore(max_concurrent)

            self.logger.info(
                f"Starting worker job processor with poll interval of {poll_interval}s"
                f" and max concurrency of {max_concurrent}"
            )
            if job_types:
                self.logger.info(f"Processing only job types: {', '.join(job_types)}")

            # Main processing loop
            while True:
                try:
                    start_time = time.time()
                    await self._process_jobs(semaphore, job_types, timeout)
                    elapsed = time.time() - start_time

                    if run_once:
                        self.logger.info("Run-once mode enabled, exiting")
                        return 0

                    # Adjust sleep time to maintain consistent polling interval
                    sleep_time = max(0.1, poll_interval - elapsed)
                    if sleep_time < poll_interval:
                        self.logger.debug(
                            f"Processing took {elapsed:.2f}s, sleeping for {sleep_time:.2f}s"
                        )

                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    self.logger.error(f"Error in processing cycle: {e}", exc_info=True)
                    await asyncio.sleep(poll_interval)

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down")
            return 0
        except Exception as e:
            self.logger.error(f"An error occurred: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(ProcessWorkerJobsCommand().run())
    exit(exit_code)
