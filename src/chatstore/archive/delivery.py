"""Durability acknowledgement requires published metadata and uncached remote bytes."""

from __future__ import annotations

import hashlib
import json
import ssl
import urllib.request
from typing import Any
from urllib.parse import urlsplit

from ..cache.store import Cache, _j
from ..canonical.records import now_ms
from .container import DEFAULT_LIMITS


def pending(cache: Cache, consumer: str, destination: str) -> list[dict[str, Any]]:
    cursor = cache.conn.execute('SELECT feed_id FROM export_consumers WHERE consumer=?', (consumer,)).fetchone()
    if cursor is None:
        raise ValueError('unknown export consumer')
    stream = cursor['feed_id']
    checkpoint = cache.conn.execute('SELECT sequence FROM delivery_checkpoints WHERE destination=? AND stream_id=?',
                                    (destination, stream)).fetchone()
    through = checkpoint[0] if checkpoint else 0
    return [{'path': row['path'], 'receipt': json.loads(row['receipt'])} for row in cache.conn.execute(
        """SELECT e.path, v.receipt FROM verified_exports v JOIN exports e USING (export_id)
        WHERE json_extract(v.receipt,'$.feed.stream')=? AND json_extract(v.receipt,'$.feed.through')>?
        ORDER BY json_extract(v.receipt,'$.feed.through')""", (stream, through))]



def _open_remote(url: str) -> Any:
    if urlsplit(url).scheme != 'https':
        raise ValueError('durability proof requires HTTPS remote retrieval')
    request = urllib.request.Request(url, headers={'Cache-Control': 'no-cache, no-store', 'Pragma': 'no-cache'})
    response = urllib.request.urlopen(request, timeout=60, context=ssl.create_default_context())
    if urlsplit(response.url).scheme != 'https':
        response.close()
        raise ValueError('durability proof redirected away from HTTPS')
    return response


def confirm(cache: Cache, *, destination: str, export_id: str, receipt_url: str, archive_url: str) -> dict[str, Any]:
    if not destination:
        raise ValueError('destination must be nonempty')
    row = cache.conn.execute('SELECT receipt FROM verified_exports WHERE export_id=?', (export_id,)).fetchone()
    if row is None:
        raise ValueError('no locally verified export receipt for this export id')
    expected = json.loads(row['receipt'])
    feed = expected.get('feed')
    if not feed:
        raise ValueError('delivery checkpoints require a changefeed export')
    with _open_remote(receipt_url) as response:
        metadata = response.read(65537)
    if len(metadata) > 65536:
        raise ValueError('published receipt exceeds 64 KiB')
    try:
        published = json.loads(metadata)
    except (ValueError, UnicodeError):
        raise ValueError('published receipt is not valid JSON') from None
    if published != expected:
        raise ValueError('published receipt does not match the exact verified export')
    # No local archive/cache shortcut: hash every retrieved ciphertext byte on every proof attempt.
    digest = hashlib.sha256()
    transferred = 0
    with _open_remote(archive_url) as response:
        while chunk := response.read(1 << 20):
            transferred += len(chunk)
            if transferred > min(expected['ciphertext_bytes'], DEFAULT_LIMITS['max_total_bytes']):
                raise ValueError('retrieved archive exceeds expected size')
            digest.update(chunk)
    actual = 'sha256:' + digest.hexdigest()
    if transferred != expected['ciphertext_bytes'] or actual != expected['ciphertext_sha256']:
        raise ValueError('remotely retrieved ciphertext does not match verified export')
    proof = {'schema': 'chatstore-delivery-proof-v1', 'destination': destination, 'export_id': export_id,
             'ciphertext_sha256': actual, 'feed': feed, 'verified_at': now_ms(),
             'metadata_bytes': len(metadata), 'ciphertext_bytes': transferred,
             'checks': ['published_receipt_matches', 'remote_ciphertext_hash_matches']}
    with cache.write():
        already = cache.conn.execute('SELECT 1 FROM delivery_proofs WHERE destination=? AND export_id=?',
                                     (destination, export_id)).fetchone()
        if already:
            return proof
        head = cache.conn.execute('SELECT * FROM delivery_checkpoints WHERE destination=? AND stream_id=?',
                                  (destination, feed['stream'])).fetchone()
        if ((head is None and feed['after'] != 0) or (head is not None and (
                head['sequence'] != feed['after'] or head['export_id'] != feed['parent']
                or head['ciphertext_sha256'] != feed['parent_sha256']))):
            raise ValueError('confirm preceding batches before advancing this destination checkpoint')
        cache.conn.execute('INSERT INTO delivery_proofs VALUES (?,?,?)', (destination, export_id, _j(proof)))
        cache.conn.execute('INSERT OR REPLACE INTO delivery_checkpoints VALUES (?,?,?,?,?)',
                           (destination, feed['stream'], feed['through'], export_id, actual))
    return proof
