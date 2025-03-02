import os
from pathlib import Path

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from pulse.models import Base, Job
from pulse.sdk import JobModel


def init_db(db_path: Path, drop: bool = False) -> sessionmaker:
    engine = create_engine(f"sqlite:///{db_path}")
    if db_path.exists() and drop:
        meta = MetaData()
        meta.reflect(bind=engine)
        meta.drop_all(bind=engine)

    create_session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)
    return create_session


def load_jobs(directory: Path, create_session: sessionmaker) -> list[Job]:
    jobs = []
    with create_session() as session:
        for root, dirs, files in os.walk(directory):
            for file in files:
                model = JobModel.from_yaml(Path(root) / file)
                job = Job(
                    file_loc=str(model.file_path),
                    schedule=model.schedule,
                    start_date=model.start_date,
                    end_date=model.end_date,
                )
                jobs.append(job)
        session.add_all(jobs)
        session.commit()
    return jobs
