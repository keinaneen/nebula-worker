import os
from shutil import copy2
import stat
import threading

import nebula
from nebula.storages import storages
from .common import BaseEncoder, ConversionError


class NebulaPlayoutCopy(BaseEncoder):
    def configure(self) -> None:
        self.files = {}
        self.copyparams = []
        self.copyparams.extend([self.asset.file_path])
        asset = self.asset
        params = self.params
        assert asset
        assert params is not None
        
        for p in self.task:
            if p.tag == "param":
                raise ConversionError("param not supported")

            elif p.tag == "script":
                raise ConversionError("script not supported")

            elif p.tag == "paramset" and eval(p.attrib["condition"]):
                raise ConversionError("paramset not supported")

            elif p.tag == "output":
                id_storage = int(eval(p.attrib["storage"]))
                storage = storages[id_storage]
                if not storage.is_writable:
                    raise ConversionError("Target storage is not writable")

                target_rel_path = eval(p.text)
                target_path = os.path.join(
                    storages[id_storage].local_path, target_rel_path
                )
                target_dir = os.path.split(target_path)[0]

                if not os.path.isdir(target_dir):
                    try:
                        os.makedirs(target_dir)
                    except Exception:
                        nebula.log.traceback()
                        raise ConversionError(
                            f"Unable to create output directory {target_dir}"
                        )
                self.copyparams.append(target_path)
                self.files["temp_path"] = target_path
    @property
    def is_running(self) -> bool:
        return self.proc and self.proc.is_alive() 

    def start(self) -> None:
        
        self.proc = threading.Thread(target=copy2, args=(self.copyparams))
        self.proc.start()
        
    def stop(self) -> None:
        return

    def wait(self, progress_handler) -> None:
        
        ofile_size = self.asset["file/size"]
        progress = 0

        while progress < 100:

            try:
                fs = os.stat(self.files["temp_path"])
                file_exists = stat.S_ISREG(fs[stat.ST_MODE])
            except FileNotFoundError:
                file_exists = False
                continue
                
            pfile_size = fs[stat.ST_SIZE]
            progress = (pfile_size / ofile_size) * 100
            progress_handler(progress)
            
        self.proc.join()

    def finalize(self) -> None:
        return