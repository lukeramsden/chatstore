"""Transactional source reuse and its race/invalidation boundary."""

from chatstore.adapters.base import CapturedUnit, Emit, ExtractStats, Observation, ReusedUnit
from chatstore.cache import Cache, SourceObservation
from chatstore.cache.extraction import ExtractionMemo
from chatstore.canonical.records import restamp
from tests.unit.test_cache import SCOPE, msg


def test_pending_reuse_is_revalidated_against_a_concurrent_projection_edit(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite3')
    other = Cache(cache.path)
    memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
    original = msg(1, 'source text', None, None, 1)
    def build():
        yield Emit(dict(original), Observation('message', [1], 'm1', 'fingerprint'))
    stats = ExtractStats()
    try:
        unit = memo.capture('m1', 'native', build, stats)
        with cache.write():
            cache.upsert(original, observed_at=100)
            cache.observe(SourceObservation(original['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'fingerprint', '1', 100, None))
            memo.save([unit])
        pending = next(memo.extract('m1', 'native', build, stats))
        assert isinstance(pending, ReusedUnit)
        with other.write():
            other.upsert(restamp(dict(original, text='concurrent local change')), observed_at=200)
        with cache.write():
            rebuilt = memo.revalidate(pending, stats)
            assert isinstance(rebuilt, CapturedUnit)
            assert rebuilt.records[0].record['text'] == original['text']
        assert stats.counts['canonical_units_reused'] == 0
    finally:
        other.close()
        cache.close()


def test_units_with_errors_do_not_become_reusable(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite3')
    memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
    original = msg(1, 'partially captured', None, None, 1)
    stats = ExtractStats()
    def build():
        stats.error('partial_decode')
        yield Emit(original)
    try:
        unit = memo.capture('m1', 'native', build, stats)
        with cache.write():
            cache.upsert(original, observed_at=100)
            memo.save([unit])
        assert not unit.cacheable
        assert isinstance(next(memo.extract('m1', 'native', build, stats)), CapturedUnit)
    finally:
        cache.close()


def test_shared_entity_memo_only_matches_the_actual_final_head(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite3')
    memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
    a, b = msg(1, 'first source view', None, None, 1), msg(1, 'second source view', None, None, 1)
    stats = ExtractStats()
    def first():
        yield Emit(a)
    def second():
        yield Emit(b)
    try:
        one, two = memo.capture('one', 'native-one', first, stats), memo.capture('two', 'native-two', second, stats)
        with cache.write():
            cache.upsert_many([a, b], observed_at=100)
            memo.save([one, two])
        assert isinstance(next(memo.extract('one', 'native-one', first, stats)), CapturedUnit)
        assert isinstance(next(memo.extract('two', 'native-two', second, stats)), ReusedUnit)
    finally:
        cache.close()


def test_shared_observation_memo_matches_final_evidence_not_just_head(tmp_path):
    cache = Cache(tmp_path / 'cache.sqlite3')
    memo = ExtractionMemo(cache, 'whatsapp', SCOPE, 'policy', reuse=True)
    record = msg(1, 'same canonical content', None, None, 1)
    stats = ExtractStats()
    def first():
        yield Emit(record, Observation('message', [1], 'm1', 'first-witness'))
    def second():
        yield Emit(record, Observation('message', [2], 'm1', 'second-witness'))
    try:
        one, two = memo.capture('one', 'native-one', first, stats), memo.capture('two', 'native-two', second, stats)
        with cache.write():
            cache.upsert(record, observed_at=100)
            cache.observe_many([
                SourceObservation(record['urn'], 'whatsapp', SCOPE, 'message', [1], 'm1', 'first-witness', '1', 100, None),
                SourceObservation(record['urn'], 'whatsapp', SCOPE, 'message', [2], 'm1', 'second-witness', '1', 100, None)])
            memo.save([one, two])
        assert isinstance(next(memo.extract('one', 'native-one', first, stats)), CapturedUnit)
        assert isinstance(next(memo.extract('two', 'native-two', second, stats)), ReusedUnit)
    finally:
        cache.close()


def test_policy_encoding_preserves_types_without_marker_collisions():
    import uuid
    from decimal import Decimal

    from chatstore.cache.extraction import _literal
    assert _literal(Decimal(1)) != _literal({'decimal': '1'})
    namespace = uuid.UUID('3f14f01c-5cc4-4325-950f-a56812da489b')
    assert _literal(namespace) != _literal({'uuid': str(namespace)})
    assert _literal(True) != _literal(1)
    assert _literal([1, 2]) != _literal((1, 2))


def test_policy_signature_is_stable_when_schema_validators_warm_up(monkeypatch):
    from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
    from chatstore.cache.extraction import code_signature
    from chatstore.canonical import schemas
    from chatstore.config import Config

    monkeypatch.setattr(schemas, '_VALIDATORS', {})
    adapter, config = WhatsAppAdapter(), Config()
    before = code_signature(adapter, config)
    assert schemas.validate_record(dict(msg(1, 'valid source message', None, None, 1), observed_at=100, sync_run=None)) is None
    assert schemas._VALIDATORS
    assert code_signature(adapter, config) == before



def test_policy_signature_covers_declared_decoder_and_routing_modules(monkeypatch):
    from chatstore.adapters.messages import dependencies
    from chatstore.adapters.messages.adapter import MessagesAdapter
    from chatstore.cache.extraction import code_signature
    from chatstore.config import Config

    adapter, config = MessagesAdapter(), Config()
    before = code_signature(adapter, config)
    monkeypatch.setattr(dependencies, 'CK_IDENTIFIER', 'changed inference policy')
    assert code_signature(adapter, config) != before


def test_policy_covers_function_defaults_and_wrapped_generator_body(monkeypatch):
    from contextlib import contextmanager

    from chatstore.adapters.messages import decoder
    from chatstore.adapters.messages.adapter import MessagesAdapter
    from chatstore.cache.extraction import code_signature
    from chatstore.config import Config

    @contextmanager
    def wrapped(value=1):
        yield value

    monkeypatch.setattr(decoder, 'policy_probe', wrapped, raising=False)
    adapter, config = MessagesAdapter(), Config()
    before = code_signature(adapter, config)
    wrapped.__wrapped__.__defaults__ = (2,)
    defaults_changed = code_signature(adapter, config)
    assert defaults_changed != before

    def replacement(value=2):
        yield value + 1

    monkeypatch.setattr(wrapped.__wrapped__, '__code__', replacement.__code__)
    assert code_signature(adapter, config) != defaults_changed


def test_policy_covers_properties_and_rejects_decorator_cycles(monkeypatch):
    import pytest

    from chatstore.adapters.messages import decoder
    from chatstore.adapters.messages.adapter import MessagesAdapter
    from chatstore.cache.extraction import code_signature
    from chatstore.config import Config

    class Probe:
        @property
        def value(self):
            return 1

    Probe.__module__ = decoder.__name__
    monkeypatch.setattr(decoder, 'PolicyProbe', Probe, raising=False)
    adapter, config = MessagesAdapter(), Config()
    before = code_signature(adapter, config)
    monkeypatch.setattr(Probe, 'value', property(lambda self: 2))
    assert code_signature(adapter, config) != before
    def cyclic():
        pass
    cyclic.__wrapped__ = cyclic
    monkeypatch.setattr(decoder, 'policy_probe', cyclic, raising=False)
    with pytest.raises(RuntimeError, match='cyclic'):
        code_signature(adapter, config)
