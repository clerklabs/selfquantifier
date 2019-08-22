import hashlib
import os
from os.path import getsize, join


def ensure_clerkai_folder_versioning(clerkai_folder_path):
    from git import Repo
    if os.path.isdir(os.path.join(clerkai_folder_path, ".git")):
        # TODO make sure that this is a clerk-managed git repository
        repo = Repo(clerkai_folder_path)
    else:
        repo = Repo.init(clerkai_folder_path)
        # make the commits in this repo be from clerk automation by default
        config = repo.config_writer()
        config.set_value('user', 'name', 'Clerk.ai')
        config.set_value('user', 'email', 'automation@clerk.ai')
        config.release()
        # initial (empty) commit
        repo.index.commit(message="Initial commit")
    assert not repo.bare
    return repo


# add all untracked and changed files
def add_all_untracked_and_changed_files(repo):
    repo.git.add('-A')
    uncommited_changes = repo.git.status('--porcelain')
    if uncommited_changes != '':
        repo.git.commit('-m', 'Current files', '-a')


# check git sha1
def current_gitsha1(repo):
    return repo.rev_parse('refs/heads/master').hexsha


def is_ignored_file(filename):
    if ".DS_Store" in filename:
        return True
    if ".~lock" in filename:
        return True


def sha256sum(filename):
    h = hashlib.sha256()
    b = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def sha1sum(filename):
    h = hashlib.sha1()
    b = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


def list_files_in_folder(folder_path):
    def is_not_ignored_file(filename):
        return not is_ignored_file(filename)
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        # print(root, "consumes", end=" ")
        # print(sum(getsize(join(root, name)) for name in files), end=" ")
        # print("bytes in", len(files), "non-directory files")
        if '.git' in dirs:
            dirs.remove('.git')  # don't visit .git directories
        files = filter(is_not_ignored_file, files)
        # print(image_files)
        for file in list(files):
            file_sha256sum = sha256sum(join(root, file))
            file_sha1sum = sha1sum(join(root, file))
            all_files.append({
                "file": file,
                "root": root,
                "size": getsize(join(root, file)),
                "file_sha1sum": file_sha1sum,
                "file_sha256sum": file_sha256sum,
            })
    return all_files


def list_files_in_clerk_subfolder(folder_path, clerkai_folder_path, repo):
    import pandas as pd
    _ = pd.DataFrame(list_files_in_folder(folder_path))
    _["root"] = _["root"].apply(lambda root: root.replace(clerkai_folder_path, "@/"))
    _["Historic reference"] = current_gitsha1(repo)
    return _
