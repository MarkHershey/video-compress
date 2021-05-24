import multiprocessing
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import List, Tuple, Union

from markkk.logger import logger


def compress_single(job: Tuple[str, str]) -> Union[bool, str]:
    in_file, out_file = job
    assert Path(in_file).is_file()
    tmp_outfile = out_file + ".part"

    out_ext: str = Path(out_file).suffix
    out_format = out_ext[1:]

    # ref: https://unix.stackexchange.com/a/38380
    args = shlex.split(
        f'ffmpeg -loglevel warning -y -i "{in_file}" -vcodec libx265 -f {out_format} -crf 28 "{tmp_outfile}"'
    )
    completed = subprocess.run(args, stdout=subprocess.DEVNULL)

    failed = True if completed.returncode != 0 else False
    if failed:
        logger.error(f"Failed compress job: ({in_file}) -> ({out_file})")
        return f"Failed: {in_file}"
    else:
        logger.debug(f"Succeeded compress job: ({in_file}) -> ({out_file})")
        shutil.move(src=tmp_outfile, dst=out_file)
        return True


def batch_compress(
    in_dir: str,
    out_dir: str,
    allowable_formats: List[str] = [".mp4"],
    overwrite: bool = False,
    num_workers: int = None,
) -> None:
    if not isinstance(num_workers, int):
        num_workers = os.cpu_count()
    else:
        assert num_workers > 0

    in_dir: Path = Path(in_dir).resolve()
    assert in_dir.is_dir()

    out_dir: Path = Path(out_dir).resolve()
    assert out_dir.is_dir()

    validated_job_list: List[tuple] = []

    for filename in os.listdir(in_dir):
        filepath: Path = in_dir / filename
        _ext: str = filepath.suffix
        if _ext not in allowable_formats:
            logger.warning(f"Skipping file '{filename}'")
            continue

        out_filepath: Path = out_dir / filename
        if out_filepath.is_file():
            if overwrite:
                logger.info(f"'{out_filepath}' will be overwritten.")
            else:
                logger.warning(
                    f"Skipping file '{filename}' due to overwrite set to False"
                )
                continue

        job: Tuple[str, str] = (str(filepath), str(out_filepath))
        validated_job_list.append(job)

    logger.debug(f"Number of compression jobs pending: {len(validated_job_list)}")
    logger.debug(f"Number of parallel workers to be used: {num_workers}")

    start = str(input("Start compression? (y/n) ")).strip().lower()
    if start != "y":
        logger.error("Operation aborted by instruction.")
        return

    pool = multiprocessing.Pool(num_workers)
    result = pool.map(compress_single, validated_job_list)

    while True in result:
        result.remove(True)

    if result:
        logger.error("----- Failed job list as follows -----")
        print(result)
    else:
        logger.debug("----- All job completed successfully -----")


if __name__ == "__main__":
    in_dir = "REPLACE_ME"
    out_dir = "REPLACE_ME"

    if not Path(out_dir).is_dir():
        os.makedirs(out_dir)

    batch_compress(in_dir, out_dir)
