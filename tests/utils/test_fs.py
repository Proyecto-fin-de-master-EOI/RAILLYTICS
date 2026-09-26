from raillytics.utils.fs import atomic_write_bytes


def test_atomic_write_bytes_writes_content_and_leaves_no_temp_file(tmp_path):
    dest = tmp_path / "sample.csv"

    result = atomic_write_bytes(dest, b"a,b\n1,2\n")

    assert result == dest
    assert dest.read_bytes() == b"a,b\n1,2\n"
    assert list(tmp_path.iterdir()) == [dest]
