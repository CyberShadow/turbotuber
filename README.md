turbotuber - relentless external youtube-dl downloader for YouTube
==================================================================

This is a simple implementation of an external youtube-dl downloader.

This program is a "gloves off" workaround for [youtube-dl issue #29326](https://github.com/ytdl-org/youtube-dl/issues/29326),
i.e. severe throttling that YouTube sometimes imposes on youtube-dl downloads.

The goal is achieved simply by continuously opening connections for increasingly parallel downloads, relentlessly retrying, and aggressively discarding them should they time out.

The author strongly recommends using this downloader only with web services owned by companies whose motto *used to be* "don't be evil". :stuck_out_tongue:

Building
--------

- Install [a D compiler](https://dlang.org/download.html)
- Install [Dub](https://github.com/dlang/dub), if it wasn't included with your D compiler
- Run `dub build -b release`


Usage
-----

turbotuber implements the aria2c CLI interface. To use, just specify the aria2c symlink as the youtube-dl `--external-downloader`:

    $ youtube-dl --external-downloader /path/to/turbotuber/aria2c "https://www.youtube.com/watch?v=..."

License
-------

Mozilla Public License, 2.0
