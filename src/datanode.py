import os
import shutil

out = "./var/storage"

if not os.path.exists(out):
    os.makedirs(out)
else:
    shutil.rmtree(out)
    os.makedirs(out)
    
