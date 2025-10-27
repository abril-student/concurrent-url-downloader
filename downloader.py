import argparse
import hashlib
import http.client
import json
import os
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from urllib.parse import urlsplit, urlunsplit

def build_connection(scheme, host, timeout=30):
    if scheme == "https":
        ctx = ssl.create_default_context()
        return http.client.HTTPSConnection(host, timeout=timeout, context=ctx)
    return http.client.HTTPConnection(host, timeout=timeout)

def path_with_query(path, query):
    return f"{path}?{query}" if query else path

def request_with_redirects(method, url, headers=None, max_redirects=3, timeout=30):
    headers = headers or {}
    current = url
    for _ in range(max_redirects + 1):
        parts = urlsplit(current)
        conn = build_connection(parts.scheme, parts.netloc, timeout=timeout)
        try:
            conn.request(method, path_with_query(parts.path or "/", parts.query), headers=headers)
            resp = conn.getresponse()
            if 200 <= resp.status < 300:
                return current, resp, conn
            if resp.status in (301, 302, 303, 307, 308):
                loc = resp.getheader("Location")
                if not loc:
                    return current, resp, conn
                if "://" not in loc:
                    if loc.startswith("/"):
                        current = urlunsplit((parts.scheme, parts.netloc, loc, "", ""))
                    else:
                        base_dir = parts.path.rsplit("/", 1)[0] if "/" in parts.path else ""
                        new_path = (base_dir + "/" if base_dir else "/") + loc
                        current = urlunsplit((parts.scheme, parts.netloc, new_path, "", ""))
                else:
                    current = loc
                resp.read()
                conn.close()
                continue
            return current, resp, conn
        except Exception:
            conn.close()
            raise
    return current, resp, conn

def fetch_headers(url, timeout=30):
    ua = {"User-Agent": "ConcurrentDownloader/1.1 (+http.client)", "Accept": "*/*"}
    final_url, resp, conn = request_with_redirects("HEAD", url, headers=ua, timeout=timeout)
    headers = {k.lower(): v for k, v in resp.getheaders()}
    status = resp.status
    resp.read()
    conn.close()
    if 200 <= status < 300 and ("content-length" in headers or "accept-ranges" in headers):
        return final_url, headers
    parts = urlsplit(final_url)
    conn = build_connection(parts.scheme, parts.netloc, timeout=timeout)
    try:
        h = dict(ua)
        h["Range"] = "bytes=0-0"
        conn.request("GET", path_with_query(parts.path or "/", parts.query), headers=h)
        r = conn.getresponse()
        hdrs = {k.lower(): v for k, v in r.getheaders()}
        r.read()
        for k, v in headers.items():
            hdrs.setdefault(k, v)
        return final_url, hdrs
    finally:
        conn.close()

def calc_ranges(total_size, workers, chunk_size_mb):
    if chunk_size_mb and chunk_size_mb > 0:
        chunk = chunk_size_mb * 1024 * 1024
        num_parts = ceil(total_size / chunk)
        chunk_size = chunk
        workers_eff = min(workers, num_parts)
    else:
        num_parts = max(1, workers)
        chunk_size = ceil(total_size / num_parts)
        workers_eff = num_parts
    ranges = []
    start = 0
    for i in range(1, num_parts + 1):
        end = min(start + chunk_size - 1, total_size - 1)
        ranges.append((i, start, end))
        start = end + 1
    return ranges, workers_eff, num_parts, chunk_size

def download_range_part(url, idx, start, end, part_path, max_retries=3, timeout=60, append=False):
    ua = {"User-Agent": "ConcurrentDownloader/1.1 (+http.client)", "Accept": "*/*"}
    effective_start = start
    mode = "wb"
    if append and os.path.exists(part_path):
        existing = os.path.getsize(part_path)
        if existing > 0:
            effective_start = min(start + existing, end + 1)
            if effective_start > end:
                return
            mode = "ab"
    if effective_start > end:
        return
    for attempt in range(1, max_retries + 1):
        parts = urlsplit(url)
        conn = build_connection(parts.scheme, parts.netloc, timeout=timeout)
        try:
            headers = dict(ua)
            headers["Range"] = f"bytes={effective_start}-{end}"
            conn.request("GET", path_with_query(parts.path or "/", parts.query), headers=headers)
            resp = conn.getresponse()
            if resp.status not in (200, 206):
                resp.read()
                raise RuntimeError(f"HTTP {resp.status} {resp.reason}")
            with open(part_path, mode) as f:
                while True:
                    chunk = resp.read(1024 * 64)
                    if not chunk:
                        break
                    f.write(chunk)
            return
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            if attempt == max_retries:
                raise
            time.sleep(1.0 * attempt)
        finally:
            try:
                conn.close()
            except Exception:
                pass

def download_single(url, out_path, max_retries=3, timeout=60):
    ua = {"User-Agent": "ConcurrentDownloader/1.1 (+http.client)", "Accept": "*/*"}
    for attempt in range(1, max_retries + 1):
        parts = urlsplit(url)
        conn = build_connection(parts.scheme, parts.netloc, timeout=timeout)
        try:
            conn.request("GET", path_with_query(parts.path or "/", parts.query), headers=ua)
            resp = conn.getresponse()
            if resp.status not in (200, 206):
                resp.read()
                raise RuntimeError(f"HTTP {resp.status} {resp.reason}")
            with open(out_path, "wb") as f:
                while True:
                    chunk = resp.read(1024 * 64)
                    if not chunk:
                        break
                    f.write(chunk)
            return
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            if attempt == max_retries:
                raise
            time.sleep(1.5 * attempt)
        finally:
            try:
                conn.close()
            except Exception:
                pass

def concat_parts(base_path, num_parts, final_path):
    with open(final_path, "wb") as out_f:
        for i in range(1, num_parts + 1):
            p = f"{base_path}.p{i}"
            if not os.path.exists(p):
                raise FileNotFoundError(f"Missing part {p}")
            with open(p, "rb") as pf:
                while True:
                    b = pf.read(1024 * 128)
                    if not b:
                        break
                    out_f.write(b)

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def write_manifest(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def read_manifest(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def clear_manifest(path):
    try:
        os.remove(path)
    except OSError:
        pass

def parse_cli_args():
    parser = argparse.ArgumentParser(prog="Concurrent HTTP/HTTPS Range Downloader")
    parser.add_argument("url")
    parser.add_argument("-o", "--output")
    parser.add_argument("-w", "--workers", type=int, default=8)
    parser.add_argument("--chunk-size-mb", type=int, default=0)
    parser.add_argument("--keep-parts", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--sha256", help="Expected SHA-256 hex digest for integrity verification")
    parser.add_argument("--no-resume", action="store_true")
    return parser.parse_args()

def discover_resource(url, timeout):
    final_url, hdrs = fetch_headers(url, timeout=timeout)
    accept_ranges = (hdrs.get("accept-ranges") or "").lower()
    content_length = hdrs.get("content-length")
    etag = hdrs.get("etag")
    last_modified = hdrs.get("last-modified")
    total_size = int(content_length) if content_length and content_length.isdigit() else None
    return final_url, accept_ranges, total_size, etag, last_modified

def derive_output_paths(final_url, output_opt):
    base_name = os.path.basename(urlsplit(final_url).path) or "download.bin"
    out_path = output_opt or base_name
    manifest_path = out_path + ".resume.json"
    return base_name, out_path, manifest_path

def fallback_download_if_needed(accept_ranges, total_size, final_url, out_path, args):
    if accept_ranges != "bytes" or total_size is None:
        print("Server does not advertise ranges or content length, performing single-request download.")
        download_single(final_url, out_path, max_retries=args.max_retries, timeout=args.timeout)
        if args.sha256:
            digest = sha256_file(out_path)
            if digest.lower() != args.sha256.lower():
                print(f"Integrity check failed: got {digest}, expected {args.sha256}")
                sys.exit(2)
        print(f"Done: {out_path}")
        return True
    return False

def plan_parts(total_size, args):
    ranges, workers_eff, num_parts, chunk_size = calc_ranges(total_size, args.workers, args.chunk_size_mb)
    print(f"Content-Length: {total_size} bytes | Accept-Ranges: bytes")
    print(f"Parts: {num_parts} | Chunk: ~{chunk_size} bytes | Workers: {workers_eff}")
    return ranges, workers_eff, num_parts, chunk_size

def load_or_bootstrap_manifest(manifest_path, allow_resume, final_url, out_path, total_size, accept_ranges, etag, last_modified, num_parts, chunk_size):
    manifest = None if not allow_resume else read_manifest(manifest_path)
    if manifest and (manifest.get("url") != final_url or manifest.get("size") != total_size):
        manifest = None
    if manifest and ((manifest.get("etag") and etag and manifest["etag"] != etag) or (manifest.get("last_modified") and last_modified and manifest["last_modified"] != last_modified)):
        manifest = None
    if not manifest:
        manifest = {
            "url": final_url,
            "output": out_path,
            "size": total_size,
            "accept_ranges": accept_ranges,
            "etag": etag,
            "last_modified": last_modified,
            "num_parts": num_parts,
            "chunk_size": chunk_size,
        }
        write_manifest(manifest_path, manifest)
    return manifest

def download_parts(final_url, ranges, out_path, args, max_workers):
    futures = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for idx, start, end in ranges:
                part_path = f"{out_path}.p{idx}"
                append = not args.no_resume and os.path.exists(part_path)
                futures.append(ex.submit(
                    download_range_part,
                    final_url,
                    idx,
                    start,
                    end,
                    part_path,
                    args.max_retries,
                    args.timeout,
                    append
                ))
            for fut in as_completed(futures):
                fut.result()
    except KeyboardInterrupt:
        print("Interrupted, keeping partial parts for resume")
        sys.exit(130)
    except Exception as e:
        print(f"Error while downloading parts: {e}")
        sys.exit(1)

def verify_parts_complete(ranges, out_path):
    for i, start, end in ranges:
        p = f"{out_path}.p{i}"
        if not os.path.exists(p):
            print(f"Missing part {p}")
            sys.exit(1)
        size_needed = end - start + 1
        if os.path.getsize(p) != size_needed:
            print(f"Incomplete part {p} ({os.path.getsize(p)}/{size_needed} bytes)")
            sys.exit(1)

def assemble_file(out_path, num_parts):
    concat_parts(out_path, num_parts, out_path)

def verify_final_size(out_path, expected_size):
    final_size = os.path.getsize(out_path)
    if final_size != expected_size:
        print(f"Size mismatch: assembled {final_size} vs expected {expected_size}")
        sys.exit(2)
    return final_size

def verify_sha256_if_requested(out_path, sha256_hex):
    if not sha256_hex:
        return
    digest = sha256_file(out_path)
    if digest.lower() != sha256_hex.lower():
        print(f"Integrity check failed: got {digest}, expected {sha256_hex}")
        sys.exit(2)

def cleanup_after_success(manifest_path, out_path, num_parts, keep_parts, preserve_noop=True):
    clear_manifest(manifest_path)
    if preserve_noop:
        if not keep_parts if False else False:
            pass
    if not keep_parts:
        for i in range(1, num_parts + 1):
            try:
                os.remove(f"{out_path}.p{i}")
            except OSError:
                pass

def main():
    args = parse_cli_args()
    final_url, accept_ranges, total_size, etag, last_modified = discover_resource(args.url, args.timeout)
    base_name, out_path, manifest_path = derive_output_paths(final_url, args.output)

    print(f"URL: {final_url}")

    if fallback_download_if_needed(accept_ranges, total_size, final_url, out_path, args):
        return

    ranges, workers_eff, num_parts, chunk_size = plan_parts(total_size, args)
    _ = load_or_bootstrap_manifest(
        manifest_path=manifest_path,
        allow_resume=not args.no_resume,
        final_url=final_url,
        out_path=out_path,
        total_size=total_size,
        accept_ranges=accept_ranges,
        etag=etag,
        last_modified=last_modified,
        num_parts=num_parts,
        chunk_size=chunk_size,
    )

    download_parts(final_url, ranges, out_path, args, max_workers=workers_eff)
    verify_parts_complete(ranges, out_path)
    assemble_file(out_path, num_parts)
    final_size = verify_final_size(out_path, total_size)
    verify_sha256_if_requested(out_path, args.sha256)
    cleanup_after_success(manifest_path, out_path, len(ranges), args.keep_parts, preserve_noop=True)
    print(f"Done: {out_path} ({final_size} bytes)")


if __name__ == "__main__":
    main()
