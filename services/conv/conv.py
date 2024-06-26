import time

from nxtools import s2words, xml

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import JobState
from nebula.jobs import Action, get_job
from services.conv.ffmpeg import NebulaFFMPEG
from services.conv.melt import NebulaMelt
from services.conv.playoutCopy import NebulaPlayoutCopy

FORCE_INFO_EVERY = 20

available_encoders = {
    "ffmpeg": NebulaFFMPEG,
    "melt": NebulaMelt,
    "playoutCopy": NebulaPlayoutCopy,
}


class Service(BaseService):
    def on_init(self):
        self.service_type = "conv"
        self.actions = []
        db = DB()
        db.query(
            """
            SELECT id, title, service_type, settings
            FROM actions ORDER BY id
            """
        )
        for id_action, title, service_type, settings in db.fetchall():
            if service_type == self.service_type:
                nebula.log.debug(f"Registering action {title}")
                self.actions.append(Action(id_action, title, xml(settings)))
        self.reset_jobs()

    def reset_jobs(self):
        db = DB()
        db.query(
            """
            UPDATE jobs SET
                id_service=NULL,
                progress=0,
                retries=0,
                status=5,
                message='Restarting after service restart',
                start_time=0,
                end_time=0
            WHERE
                id_service=%s AND STATUS IN (0,1,5)
            RETURNING id
            """,
            [self.id_service],
        )
        for (id_job,) in db.fetchall():
            nebula.log.info(f"Restarting job ID {id_job} (converter restarted)")
        db.commit()

    def progress_handler(self, progress: float | None = None):
        stat = self.job.get_status()
        if stat == JobState.RESTART:
            self.encoder.stop()
            self.job.restart()
            return
        elif stat == JobState.ABORTED:
            self.encoder.stop()
            self.job.abort()
            return
        if progress is None:
            message = "Encoding: Unknown progress"
            progress = 0
        else:
            message = f"Encoding: {progress:.02f}%"
        self.job.set_progress(progress, message)

    def on_main(self):
        db = DB()
        self.job = get_job(
            self.id_service, [action.id for action in self.actions], db=db
        )
        if not self.job:
            return
        nebula.log.info(f"Got {self.job}")

        asset = self.job.asset
        action = self.job.action

        try:
            job_params = self.job.settings
        except Exception:
            nebula.log.traceback()
            job_params = {}

        tasks = action.settings.findall("task")
        job_start_time = time.time()

        for id_task, task in enumerate(tasks):
            try:
                using = task.attrib["mode"]
                available_encoders[using]
            except KeyError:
                self.job.fail(
                    f"Wrong encoder type specified for task {id_task}", critical=True
                )
                return

            self.encoder = available_encoders[using](asset, task, job_params)

            nebula.log.debug(f"Configuring task {id_task+1} of {len(tasks)}")

            try:
                self.encoder.configure()
            except Exception as e:
                self.job.fail(f"Failed to configure task {id_task+1}: {e}")
                nebula.log.traceback()
                return

            nebula.log.info(f"Starting task {id_task+1} of {len(tasks)}")
            try:
                self.encoder.start()
                self.encoder.wait(self.progress_handler)
            except Exception as e:
                self.job.fail(f"Failed to encode task {id_task+1}: {e}")
                nebula.log.traceback()
                return

            if self.encoder.aborted:
                return

            nebula.log.debug(f"Finalizing task {id_task+1} of {len(tasks)}")
            try:
                self.encoder.finalize()
            except Exception as e:
                self.job.fail(f"Failed to finalize task {id_task+1}: {e}")
                nebula.log.traceback()
                return

            job_params = self.encoder.params

        job = self.job  # noqa
        assert job

        for success_script in action.settings.findall("success"):
            nebula.log.info("Executing success script")
            success_script = success_script.text
            try:
                exec(success_script)
            except Exception:
                nebula.log.traceback()
                self.job.fail("Failed to execute success script")
                return

        elapsed_time = time.time() - job_start_time
        duration = asset["duration"] or 1
        speed = duration / elapsed_time

        self.job.done(f"Finished in {s2words(elapsed_time)} ({speed:.02f}x realtime)")
