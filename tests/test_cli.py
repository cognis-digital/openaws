"""Tests for the CLI surface (parser + in-process command handlers)."""

from openaws.cli import build_parser, main


def test_version_command(capsys):
    rc = main(["version"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out and out[0].isdigit()


def test_s3_cli_round_trip(tmp_path, capsys):
    data = str(tmp_path / "d")
    f = tmp_path / "hello.txt"
    f.write_bytes(b"hi there")
    assert main(["--data-dir", data, "s3", "mb", "my-bucket"]) == 0
    assert main(["--data-dir", data, "s3", "put", "my-bucket", "h.txt", str(f)]) == 0
    capsys.readouterr()
    assert main(["--data-dir", data, "s3", "ls", "my-bucket"]) == 0
    assert "h.txt" in capsys.readouterr().out


def test_sqs_cli_round_trip(tmp_path, capsys):
    data = str(tmp_path / "d")
    assert main(["--data-dir", data, "sqs", "create", "q"]) == 0
    capsys.readouterr()
    assert main(["--data-dir", data, "sqs", "send", "q", "payload"]) == 0
    capsys.readouterr()
    assert main(["--data-dir", data, "sqs", "receive", "q"]) == 0
    assert "payload" in capsys.readouterr().out


def test_parser_requires_command():
    parser = build_parser()
    import pytest

    with pytest.raises(SystemExit):
        parser.parse_args([])
