# ufc
unique file copy 

```
ufc (unique file copy) - Copies unique files from src_dir to dst_dir. Destination has k/v store containing pairs md5<->path

Usage: ufc [OPTIONS]

Options:
  -s, --src-dir <SRC_DIR>
          source directory with potential duplicates
  -d, --dst-dir <DST_DIR>
          destination directory where unique files will be stored
  -r, --dry-run <DRY_RUN>
          simulate work w/o moving data + show stats [possible values: true, false]
  -v, --verbose <VERBOSE>
          dump additional info on the screen [possible values: true, false]
  -k, --keep-copy <KEEP_COPY>
          **TBD** keep previous versions of the file (in case of overwrites) [possible values: true, false]
  -o, --overwrite <OVERWRITE>
          allow overwriting files in destination dir. By default no overwrite will happen [possible values: true, false]
      --name-prefix-checksum <NAME_PREFIX_CHECKSUM>
          add prefix checksum  into destination file name (i.e. creates unique file name) [possible values: true, false]
  -n, --name <NAME>
          copies only files with names matching regular exp. E.g --name ".jpg|.png|.gif"
  -i, --images-only <IMAGES_ONLY>
          copies only images: jpg/gif/png/... [possible values: true, false]
  -p, --progress <PROGRESS>
          show progress ... [possible values: true, false]
  -m, --mt <MT>
          multithreaded [possible values: true, false]
  -t, --tn <TN>
          threads number
  -h, --help
          Print help
  -V, --version
          Print version

```
