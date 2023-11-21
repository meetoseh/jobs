try:
    import helper  # type: ignore
except:
    import tests.helper  # type: ignore

import logging
import logging.config
import unittest
import yaml
import os
from shareables.generate_class.p05_stitch_audio import get_audio_info
from shareables.generate_class.p06_generate_background_music import (
    combine_using_crossfade,
    Music,
)
from temp_files import temp_dir

ALL_INPATHS = [
    os.path.join("tmp", "unlock-me-149058.mp3"),
    os.path.join("tmp", "leva-eternity-149473.mp3"),
    os.path.join("tmp", "a-call-to-the-soul-149262.mp3"),
    os.path.join("tmp", "reflected-light-147979.mp3"),
]


class Test(unittest.TestCase):
    def test_combine_crossfades_3(self):
        inpaths = ALL_INPATHS[:3]
        outpath = os.path.join("tmp", "test_combine_audio.wav")
        if os.path.exists(outpath):
            os.remove(outpath)

        with temp_dir() as workingdir:
            combine_using_crossfade(
                segments=[
                    Music(
                        path=path,
                        attribution=[],
                        duration=get_audio_info(path).duration,
                        fade_after=5,
                        delay_after=None,
                    )
                    for path in inpaths
                ],
                folder=workingdir,
                out=outpath,
            )

        self.assertTrue(os.path.exists(outpath))
        get_audio_info(outpath)

    def test_combine_crossfades_4(self):
        inpaths = ALL_INPATHS[:4]
        outpath = os.path.join("tmp", "test_combine_audio_4.wav")
        if os.path.exists(outpath):
            os.remove(outpath)

        with temp_dir() as workingdir:
            combine_using_crossfade(
                segments=[
                    Music(
                        path=path,
                        attribution=[],
                        duration=get_audio_info(path).duration,
                        fade_after=5,
                        delay_after=None,
                    )
                    for path in inpaths
                ],
                folder=workingdir,
                out=outpath,
            )

        self.assertTrue(os.path.exists(outpath))
        get_audio_info(outpath)


if __name__ == "__main__":
    with open("logging.yaml") as f:
        logging_config = yaml.safe_load(f)
    logging.config.dictConfig(logging_config)
    unittest.main()
