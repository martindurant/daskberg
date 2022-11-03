import enum
import json

import dask.dataframe as dd
import fastavro
import fsspec.core

from daskberg.conversions import typemap, transform, convert


class ManifestStatus(enum.IntEnum):
    EXISTING = 0
    ADDED = 1
    DELETED = 2


class IcebergDataset:

    def __init__(self, url, original_url=None, storage_options=None,
                 engine="fastparquet"):
        """
        Parameters
        ----------
        url: str
            Base directory, iceberg "location"
        original_url: str | None
            If the data originated elsewhere, the embedded absolute URLs will be
            wrong. Set this to that original location to replace with new locations
            on load.
        storage_options: dict | None
            passed to fsspec to open all remote files
        """
        self.original_url = original_url or url
        self.url = url.rstrip("/")
        self.engine = engine
        self.storage_options = storage_options or {}
        self.fs, _ = fsspec.core.url_to_fs(url, **self.storage_options)
        self._version_hint = None
        self._versions = None
        self._version = None
        self._snapshots = None
        self._version = None
        self._metadata = None
        self._allfiles = None
        self.summary = None

    @property
    def version_hint(self):
        """Latest version, according to hints file"""
        if self._version_hint is None:
            try:
                with self.fs.open(self.url + "/metadata/" + "version-hint.text", "rb") as f:
                    self._version_hint = int(f.read())
            except (FileNotFoundError, ValueError):
                self._version_hint = 0
        return self._version_hint

    @property
    def version_numbers(self):
        """Available version number, as a list[int]"""
        if self._versions is None:
            if self.version_hint:
                self._versions = [i + 1 for i  in range(self.version_hint)]
            else:
                files = self.fs.glob(self.url + "/metadata/" + "v*.metadata.json")
                stems = [f.rsplit("/")[-1].split(".", 1)[0][1] for f in files]
                self._versions = [int(stem) for stem in stems]
            self._version = self._versions[-1]
        return self._versions

    @property
    def version(self):
        """Selected version (=latest if not specified)"""
        if self._version is None:
            self._version = self.version_numbers[-1]
        return self._version

    @version.setter
    def version(self, version: int):
        self.set_version(version)

    def _load_version(self, version):
        with self.fs.open(f"{self.url}/metadata/v{version}.metadata.json", "rt") as f:
            return json.load(f)

    def set_version(self, version=None):
        """Travel to the given version"""
        if version is None or version < 1:
            version = self.version_numbers[-1]
        if version in self.version_numbers:
            self._version = version
        else:
            raise ValueError(f"Version {version} not in allowed list {self.version_numbers}")
        self._metadata = self._load_version(version)

    @property
    def metadata(self):
        """Full dict of metadata for the current version"""
        if self._metadata is None:
            self.set_version()
        return self._metadata

    @property
    def snapshots(self):
        """Snapshots available"""
        return {s["snapshot-id"]: s for s in self.metadata["snapshots"]}

    @property
    def latest_snapshot(self):
        """Most recent or "current" snapshot for the loaded version"""
        if self.metadata["current-snapshot-id"] < 0:
            raise ValueError("No snapshots")
        return self.snapshots[self.metadata["current-snapshot-id"]]

    def open_snapshot(self, snapshot_id=None, rel=None):
        """Travel to snapshot

        Use either snapshot_id or rel, not both. If both are None, selects
        self.latest_snapshot

        Parameters
        ----------
        snapshot_id: int | None
            Absolute reference, one of self.snapshots
        rel: int | None
            Snapshot relative to the current latest: 0 is the latest, and negative
            number walk backwards from there.
        """
        if rel is not None and snapshot_id is not None:
            raise ValueError("Cannot set both absolute and relative snapshots")
        if snapshot_id is None and rel is None:
            snap = self.latest_snapshot
        elif rel is not None:
            if rel > 0:
                raise ValueError("Relative snapshot ID must be negative or zero")
            if -rel > len(self.snapshots) - 1:
                raise ValueError("Relative snapshot out of range")
            snap = self.latest_snapshot
            if rel < 0:
                for _ in range(-rel):
                    snap = self.snapshots[snap["parent-snapshot-id"]]
        else:
            snap = self.snapshots[snapshot_id]
        self.summary = snap["summary"]
        self.schema = [_["fields"] for _ in self.metadata["schemas"]
                       if _["schema-id"] == snap["schema-id"]][0]
        with self.fs.open(f"{self.url}/metadata/{snap['manifest-list'].rsplit('/', 1)[-1]}") as f:
            manifest_list = list(fastavro.reader(f))
        allfiles = {}
        deletefiles = set()
        for mani in manifest_list:
            path = mani["manifest_path"].replace(self.original_url, self.url)
            # we already know here if the manifest contains only deletions
            with fsspec.open(path, "rb", **self.storage_options) as f:
                for afile in list(fastavro.reader(f)):
                    path = afile["data_file"]["file_path"].replace(self.original_url, self.url)
                    assert afile["data_file"]['file_format'] == "PARQUET"
                    if afile["status"] in [ManifestStatus.ADDED, ManifestStatus.EXISTING]:
                        allfiles[path] = afile["data_file"]
                    elif afile["status"] == ManifestStatus.DELETED:
                        deletefiles.add(path)
                    else:
                        raise ValueError
        for afile in deletefiles:
            allfiles.pop(afile, None)
        self._allfiles = allfiles

    def read(self, filters=None, columns=None, **kwargs):
        """The current snapshot as a dask array

        Parameters
        ----------
        filters: list[tuple| list[tuple]] | None
            parquet-style filter expression
        columns: list[str] | None
            Sub-select columns to load
        kwargs: dict
            Passed to current backend engine
        """
        if self._allfiles is None:
            self.open_snapshot()
        if filters:
            fields = {}
            for field in self.schema:
                fields[field["name"]] = {'id': field["id"], "type": field["type"]}
                part_trans = [_ for _ in self.metadata["partition-spec"]
                              if _["source-id"] == field["id"] and _["transform"] != "identity"]
                if part_trans:
                    fields[field["name"]]["transform"] = part_trans[0]
            parts = apply_filters(self._allfiles.values(), filters, fields)
            parts = [_["file_path"].replace(self.original_url, self.url) for _ in parts]
            if len(parts) == 0:
                raise ValueError("No partitions pass filter(s)")
        else:
            parts = list(self._allfiles)
        if self.engine == "fastparquet":
            dt = {_["name"]: typemap(_["type"])
                  for _ in self.schema}
            kwargs.setdefault("dataset", {})["dtypes"] = dt
        return dd.read_parquet(list(parts), engine=self.engine,
                               calculate_divisions=False, root=self.url + "/data",
                               columns=columns, filters=filters, index=False, **kwargs)

    def unique_partitions(self, field=None):
        """The set of partition values available

        Parameters
        ----------
        field: str | None
            If given, gets values for given fields only, else a dict of {field: values}
        """
        if field is None:
            fields = [_["name"] for _ in self.metadata["partition-spec"]]
            return {f: self.unique_partitions(f) for f in fields}
        out = set()
        for afile in self._allfiles.values():
            out.add(afile["partition"][field])
        return sorted(out)


# adapted from dask.dataframe.io.parquet.core
def apply_filters(file_details, filters, fields):
    """Selects files passing given filters"""

    def apply_conjunction(file_details, conjunction):
        for column, operator, value in conjunction:
            if operator == "in" and not isinstance(value, (list, set, tuple)):
                raise TypeError("Value of 'in' filter must be a list, set, or tuple.")
            out_parts = []
            if "transform" in fields[column]:
                tr_tr = fields[column]["transform"]["transform"]
                value = transform(value, tr_tr)
                column = fields[column]["transform"]["name"]

            for file_detail in file_details:
                if column in file_detail["partition"]:
                    min = max = file_detail["partition"][column]
                else:
                    min = [_["value"] for _ in file_detail['lower_bounds']
                           if _["key"] == fields[column]["id"]][0]
                    min = convert(min, fields[column]["type"])
                    max = [_["value"] for _ in file_detail['upper_bounds']
                           if _["key"] == fields[column]["id"]][0]
                    max = convert(max, fields[column]["type"])
                    # TODO: if min/max is time, need to convert value too, if str
                if (
                    operator in ("==", "=")
                    and min <= value <= max
                    or operator == "!="
                    and (min != value or max != value)
                    or operator == "<"
                    and min < value
                    or operator == "<="
                    and min <= value
                    or operator == ">"
                    and max > value
                    or operator == ">="
                    and max >= value
                    or operator == "in"
                    and any(min <= item <= max for item in value)
                ):
                    out_parts.append(file_detail)

            file_details = out_parts

        return file_details

    conjunction, *disjunction = filters if isinstance(filters[0], list) else [filters]

    out_parts = apply_conjunction(file_details, conjunction)
    for conjunction in disjunction:
        for part in zip(*apply_conjunction(file_details, conjunction)):
            if part not in out_parts:
                out_parts.append(part)

    return out_parts
