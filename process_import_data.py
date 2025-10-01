import os
import sys
import glob

import pandas as pd


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(root, "data", "raw", "import_data")
    out_path = os.path.join(root, "data", "processed", "import_data_processed.csv")

    if not os.path.isdir(src_dir):
        sys.stderr.write("Missing directory: {0}\n".format(src_dir))
        return 1

    files = sorted(glob.glob(os.path.join(src_dir, "*.csv")))
    frames = []
    for path in files:
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as err:
            sys.stderr.write("Skip {0}: {1}\n".format(os.path.basename(path), err))
            continue
        name = os.path.basename(path)
        title, _ = os.path.splitext(name)
        df["__source_file"] = name
        df["__source_title"] = title
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, sort=True) if frames else pd.DataFrame()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    combined.to_csv(out_path, index=False)
    sys.stdout.write(out_path + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


