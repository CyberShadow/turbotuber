import core.sys.posix.unistd;
import core.time;

import std.algorithm.comparison;
import std.algorithm.iteration;
import std.algorithm.mutation;
import std.algorithm.searching;
import std.algorithm.sorting;
import std.array;
import std.conv;
import std.datetime.systime;
import std.digest.crc;
import std.exception;
import std.format;
import std.process : environment;
import std.random;
import std.range;

import ae.net.asockets;
import ae.net.http.client;
import ae.net.http.common;
import ae.net.ietf.headers;
import ae.net.ietf.url : applyRelativeURL;
import ae.net.ssl.openssl;
import ae.sys.data;
import ae.sys.datamm;
import ae.sys.log;
import ae.sys.timing;
import ae.utils.aa;
import ae.utils.array;
import ae.utils.funopt;
import ae.utils.main;
import ae.utils.meta.rcclass;

/// Start new loaders if a request wants more than this many bytes ahead
size_t maxDistance = 256*1024;

/// Loader start frequency
Duration startFrequency = 500.msecs;

/// Drop a loader if it receives no data within this timeframe
Duration loaderTimeout = 10.seconds;

private:

void program(
	Option!string out_,
	string[] url,
	Option!(string[]) header = null,
	Switch!(null, 'c') continue_ = false, // ignore
	Option!string minSplitSize = null, // ignore
	Option!string maxConnectionPerServer = null, // ignore
	Option!string dir = null, // ignore
	Option!string interface_ = null, // ignore
	Option!string allProxy = null, // ignore
	Option!string checkCertificate = null, // ignore
	Option!string remoteTime = null, // ignore
)
{
	Headers headers;
	foreach (h; header)
	{
		string name, value;
		list(name, null, value) = h.findSplit("=");
		headers.add(name, value);
	}
	log = lineLogger("turbotuber");
	files ~= new File(url, headers, out_);
	socketManager.loop();
}

mixin main!(funopt!program);

mixin SSLUseLib;

LineLogger log;

alias Offset = ulong;
enum unknown = Offset.max;

class CLineLogger : CLogger
{
	enum maxLines = 1000;
	string[] lines;

	this(string name) { super(name); }

	override void log(in char[] str)
	{
		import std.stdio : stdout;
		stdout.writeln("\x1B[2K", str);
		stdout.flush();
		redraw(true);
	}
}
alias RCClass!CLineLogger LineLogger;
alias rcClass!CLineLogger lineLogger;

final class Loader
{
	Offset start, end;
	TimerTask timeout;

	this(string url, const Headers headers, Offset start, Offset end)
	{
		log("Creating loader: %s, %s-%s".format(
			url,
			start == unknown ? "*" : start.text,
			end   == unknown ? "*" : end  .text,
		));

		this.start = start;
		this.end = end;
		this.requestHeaders = headers;

		if (start == unknown && end != unknown)
			assert(false, "Created a suffix-length loader");

		timeout = setInterval(&onTimeout, loaderTimeout);

		startRequest(url);
	}

	bool willFetch(Offset offset, Offset maxDistance)
	{
		if (start == unknown && end == unknown)
			return offset < maxDistance; // Will get whole file
		if (start <= offset || offset < end)
			return offset - start < maxDistance; // Will get this range
		return false;
	}

	/// Called when this Loader receives the headers from the server.
	void delegate(HttpResponse response, Offset totalSize) handleHeaders;

	/// Called when a data chunk was received.
	/// Return true to keep going, or false to disconnect early.
	bool delegate(Offset offset, Data[] data) handleData;

	/// Called when the request is done for whatever reason.
	void delegate(Loader, bool success) handleDisconnect;

private:
	const Headers requestHeaders;
	HttpClient client;
	HttpRequest request;

	void startRequest(string url)
	{
		request = new HttpRequest(url);
		request.method = "GET";
		foreach (name, value; requestHeaders)
			request.headers.add(name, value);
		if (start != unknown || end != unknown)
			request.headers["Range"] =
				"bytes=" ~
				(start == unknown ? "" : text(start)) ~
				"-" ~
				(end   == unknown ? "" : text(end - 1));

		if (request.url.startsWith("http://"))
			client = new Http!HttpClient();
		else
		if (request.url.startsWith("https://"))
			client = new Http!HttpsClient();
		else
			throw new Exception("Unknown protocol");
	}

	final class Http(Base) : Base
	{
		this()
		{
			request(this.outer.request);
		}

		bool headersReceived;

		override void onHeadersReceived()
		{
			timeout.restart();

			auto oldStart = start;
			auto oldEnd = end;

			Offset totalSize = unknown;

			if (auto location = currentResponse.headers.get("Location", null))
			{
				auto newUrl = applyRelativeURL(this.outer.request.url, location);
				log("Redirecting to " ~ newUrl);
				return startRequest(newUrl);
			}

			auto contentRange = currentResponse.headers.get("Content-Range", null);
			if (contentRange && contentRange.skipOver("bytes "))
			{
				auto parts = contentRange.findSplit("/");
				enforce(parts, "Bad Content-Range header (no /)");
				if (parts[2] != "*")
					totalSize = parts[2].to!Offset;
				if (parts[0] == "*")
					throw new Exception("Unsatisfied range"); // Should not be possible
				else
				{
					parts = parts[0].findSplit("-");
					enforce(parts, "Bad Content-Range header (no -)");
					start = parts[0].to!Offset;
					end   = parts[2].to!Offset + 1;
				}
			}
			else
			if (auto contentLength = currentResponse.headers.get("Content-Length", null))
			{
				totalSize = contentLength.to!Offset;
				start = 0;
				end   = totalSize;
			}
			else
			{
				start = 0;
				end   = unknown;
			}

			log("Got headers. Requested bytes: %s-%s; Got bytes: %s-%s/%s".format(
				oldStart  == unknown ? "*" : oldStart .text,
				oldEnd    == unknown ? "*" : oldEnd   .text,
				start     == unknown ? "*" : start    .text,
				end       == unknown ? "*" : end      .text,
				totalSize == unknown ? "*" : totalSize.text,
			));

			handleHeaders(currentResponse, totalSize);

			headersReceived = true;
			super.onHeadersReceived();
		}

		override void onData(scope Data[] data)
		{
			// log("Got data: %d-%d (in %d chunks)".format(start, start + data.bytes.length, data.length));
			super.onData(data);
			timeout.restart();

			auto offset = start;
			start += data.bytes.length;

			bool keepGoing = handleData(offset, data);

			if (!keepGoing)
				disconnect("No more data is needed");
		}

		override void onDone()
		{
			if (handleDisconnect)
			{
				handleDisconnect(this.outer, true);
				handleDisconnect = null;
			}
			super.onDone();
		}

		override void onDisconnect(string reason, DisconnectType type)
		{
			log("Loader disconnected: %s".format(reason));
			if (this == this.outer.client)
			{
				timeout.cancel();
				super.onDisconnect(reason, type);
				if (handleDisconnect)
				{
					handleDisconnect(this.outer, false);
					handleDisconnect = null;
				}
			}
		}

		// override void request(HttpRequest request) { return Base.request(request); }
	}

	void log(string s)
	{
		.log(format!"[Loader %s] %s"(/*request.url.crc32Of()*/ cast(void*)this, s));
	}

	void onTimeout()
	{
		log("Time-out, disconnecting.");
		client.disconnect("Time-out");
	}
}

final class File
{
	this(string[] urls, Headers requestHeaders, string outputFileName)
	{
		this.urls = urls;
		this.requestHeaders = requestHeaders;
		this.outputFileName = outputFileName;

		this.timeLastActivity = Clock.currTime();
		log("Created File.");

		loaderStarter = setInterval(&startALoader, startFrequency);
		startALoader();
	}

	void update()
	{
		redraw();
	}

	bool done;

	@property Offset sizeSoFar() { return chain(Fragment.init.only, fragments).back.end; }

private:
	/// URL to the server file.
	string[] urls;

	string outputFileName;

	/// Request headers, for sending to the server (excluding range)
	Headers requestHeaders;

	struct Fragment
	{
		Offset offset;
		size_t length;

		@property Offset end() { return offset + length; }
	}
	Fragment[] fragments;

	/// unknown if unknown
	Data fileData;

	/// For cleanup
	SysTime timeLastActivity;

	HashSet!Loader loaders;

	TimerTask loaderStarter;

	Fragment* dataAt(Offset offset)
	{
		foreach (ref fragment; fragments)
		{
			assert(fragment.length);
			if (fragment.offset <= offset && offset < fragment.end)
				return &fragment;
			else
			if (fragment.offset > offset)
				break;
		}
		return null;
	}

	void startLoader(Offset start, Offset end)
	{
		if (start == unknown)
		{
			assert(end == unknown);
			start = 0;
		}

		if (fileData)
			assert(start < fileData.length, "Trying to start a loader starting after known size");
		assert(!dataAt(start));

		// foreach (loader; loaders)
		// 	if (start <= loader.start && loader.start < end)
		// 		end = loader.start;

		while (end > start)
			if (auto f = dataAt(end - 1))
				end = max(start, f.offset);
			else
				break;

		if (start == end)
			assert(false, "Requested loader for zero-length range");

		auto loader = new Loader(urls[uniform(0, $)], requestHeaders, start, end);
		loader.handleHeaders = &onLoaderHeaders;
		loader.handleData = &onLoaderData;
		loader.handleDisconnect = &onLoaderDisconnect;
		loaders.add(loader);
	}

	void startALoader()
	{
		if (!fileData)
		{
			if (loaders.empty)
			{
				log(format("Starting initial loader"));
				startLoader(0, unknown);
			}
			else
				log(format("Size not yet known, not starting loader"));
			return;
		}

		Offset start = 0;
		Offset[2][] gaps;

		auto fragments = this.fragments.dup;
		HashSet!Offset loaderOffsets;

		foreach (loader; loaders)
			if (loader.start != unknown)
			{
				fragments ~= Fragment(loader.start, 0);
				loaderOffsets.add(loader.start);
			}
		fragments.sort!((a, b) => a.offset == b.offset ? a.end < b.end : a.offset < b.offset);

		foreach (ref fragment; fragments)
		{
			gaps ~= [start, fragment.offset];
			start = fragment.end;
		}
		gaps ~= [start, fileData.length];
		gaps.sort!((a, b) => a[1] - a[0] > b[1] - b[0]);
		if (gaps[0][1] - gaps[0][0] > maxDistance)
		{
			start = gaps[0][0];
			auto end = gaps[0][1];
			if (start in loaderOffsets)
				start = (start + end) / 2;
			log(format("Starting a loader at %d for gap %d-%d (size %d)",
					start, gaps[0][0], gaps[0][1], gaps[0][1] - gaps[0][0]));
			startLoader(start, gaps[0][1] ? gaps[0][1] : unknown);
		}
		else
		{
			log(format("Not starting a loader, biggest gap at %d-%d is too small (%d)",
					gaps[0][0], gaps[0][1], gaps[0][1] - gaps[0][0]));
		}
	}

	void onLoaderHeaders(HttpResponse response, Offset totalSize)
	{
		log(format("Got %d (%s)", response.status, response.statusMessage));

		enforce(totalSize != 0, "File is zero-sized");
		if (!fileData && totalSize != unknown)
		{
			import std.stdio : File;
			auto f = File(outputFileName, "wb");
			ftruncate(f.fileno, totalSize);
			f.close();
			fileData = mapFile(outputFileName, MmMode.readWrite, 0, totalSize);
		}
		if (fileData && totalSize != unknown)
			enforce(fileData.length == totalSize, "Total size mismatch");

		update();
	}

	bool haveAllData()
	{
		if (!fileData)
			return true;
		if (fragments.length == 0)
			return false;
		if (fragments[0].offset > 0 || fragments[$-1].end < fileData.length)
			return false;
		foreach (i; 0 .. fragments.length - 1)
			if (fragments[i].end != fragments[i + 1].offset)
				return false;
		return true;
	}

	bool haveAllDataAfter(Offset offset)
	{
		if (offset == fileData.length)
			return true;
		foreach (sizediff_t i; -1 .. fragments.length)
		{
			auto end = i < 0 ? 0 : fragments[i].end;
			auto end2 = i + 1 == fragments.length ? fileData.length : fragments[i + 1].offset;
			if (end2 < offset)
				continue;
			if (end != end2)
				return false;
		}
		return true;
	}

	bool onLoaderData(Offset offset, Data[] data)
	{
		size_t fragmentIndex; // Insert here

		foreach (datum; data)
		{
			if (!datum.length)
				continue;

			auto end = offset + datum.length;

			// Fast-forward to our fragment's insertion point -
			// skip past all fragments that are completely before this one
			while (fragmentIndex < fragments.length && fragments[fragmentIndex].end <= offset)
				fragmentIndex++;

			// Truncate any existing fragment that overlaps our fragment's start point
			if (fragmentIndex < fragments.length &&
				fragments[fragmentIndex].offset <= offset)
			{
				assert(fragments[fragmentIndex].end > offset); // Should be guaranteed by the loop above
				// TODO: check if previously-had data matches
				fragments[fragmentIndex].length = offset - fragments[fragmentIndex].offset;
				if (fragments[fragmentIndex].length == 0)
					fragments = fragments.remove(fragmentIndex);
				else
					fragmentIndex++;
			}

			// Remove any fragments that are completely contained within our range
			while (fragmentIndex < fragments.length &&
				fragments[fragmentIndex].offset >= offset &&
				fragments[fragmentIndex].end <= end)
			{
				// TODO: check if previously-had data matches
				fragments = fragments.remove(fragmentIndex);
			}
				
			// Truncate any existing fragment that overlaps our fragment's end point
			if (fragmentIndex < fragments.length &&
				fragments[fragmentIndex].offset < end)
			{
				assert(fragments[fragmentIndex].end > end); // Should be guaranteed by the loop above
				// TODO: check if previously-had data matches
				auto overlap = end - fragments[fragmentIndex].offset;
				fragments[fragmentIndex].offset += overlap;
				fragments[fragmentIndex].length -= overlap;
				assert(fragments[fragmentIndex].length > 0);
			}

			// Insert
			fragments.insertInPlace(fragmentIndex, Fragment(offset, datum.length));
			fileData[offset .. offset+datum.length].mcontents[] = datum.contents[];

			fragmentIndex++;
			offset = end;
		}

		update();

		if (fileData)
			if (haveAllDataAfter(offset))
				return false; // We have everything from this point
		return true; // Keep going
	}

	void onLoaderDisconnect(Loader loader, bool success)
	{
		loaders.remove(loader);

		// if (success && !fileData && loader.end == unknown)
		// 	totalSize = sizeSoFar;

		if (fileData && fragments.map!((ref f) => f.length).sum == fileData.length && !done)
		{
			done = true;
			loaderStarter.cancel();
		}

		update();
	}

	void log(string s)
	{
		.log(format!"[File %(%02x%)] %s"(outputFileName.crc32Of(), s));
	}
}

File[] files;
MonoTime lastRedraw;

void redraw(bool force = false)
{
	auto now = MonoTime.currTime;
	if (!force && now - lastRedraw < 100.msecs)
		return;
	lastRedraw = now;

	int barWidth;
	if ("COLUMNS" in environment)
		barWidth = environment["COLUMNS"].to!int - 1;
	else
		barWidth = 100;

	auto cellBytes = new Offset[barWidth];
	auto line = new dchar[barWidth];

	foreach (file; files)
	{
		if (!file.fileData)
			continue;

		foreach (ref fragment; file.fragments)
		{
			assert(fragment.length);
			assert(fragment.end <= file.fileData.length);

			auto startCell = barWidth *  fragment.offset   / file.fileData.length;
			auto   endCell = barWidth * (fragment.end - 1) / file.fileData.length;
			assert(endCell < barWidth);
			foreach (i; startCell .. endCell + 1)
			{
				auto cellStart = max(fragment.offset, file.fileData.length *  i      / barWidth);
				auto cellEnd   = min(fragment.end   , file.fileData.length * (i + 1) / barWidth);
				cellBytes[i] += cellEnd - cellStart;
			}
		}

		foreach (i; 0 .. barWidth)
		{
			if (file.fragments.length == 0)
				line[i] = '?';
			else
			{
				auto cellStart = file.fileData.length *  i      / barWidth;
				auto cellEnd   = file.fileData.length * (i + 1) / barWidth;
				if (cellStart == cellEnd)
					line[i] = '-';
				else
				{
					auto have  = cellBytes[i];
					auto total = cellEnd - cellStart;
					line[i] = " ▁▂▃▄▅▆▇█"d[($-1) * have / total];
				}
			}
		}

		import std.stdio : stdout;
		stdout.write("[", line, "]\r"); stdout.flush();
	}
}
