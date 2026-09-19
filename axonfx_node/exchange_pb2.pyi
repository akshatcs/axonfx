from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SubmitTransferRequest(_message.Message):
    __slots__ = ("request_id", "source_currency", "dest_currency", "source_amount", "recipient_name", "recipient_account")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CURRENCY_FIELD_NUMBER: _ClassVar[int]
    DEST_CURRENCY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_NAME_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    source_currency: str
    dest_currency: str
    source_amount: float
    recipient_name: str
    recipient_account: str
    def __init__(self, request_id: _Optional[str] = ..., source_currency: _Optional[str] = ..., dest_currency: _Optional[str] = ..., source_amount: _Optional[float] = ..., recipient_name: _Optional[str] = ..., recipient_account: _Optional[str] = ...) -> None: ...

class FillDetail(_message.Message):
    __slots__ = ("via_currency", "drained_amount", "produced_amount", "rate", "used_pool_surplus")
    VIA_CURRENCY_FIELD_NUMBER: _ClassVar[int]
    DRAINED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    PRODUCED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    RATE_FIELD_NUMBER: _ClassVar[int]
    USED_POOL_SURPLUS_FIELD_NUMBER: _ClassVar[int]
    via_currency: str
    drained_amount: float
    produced_amount: float
    rate: float
    used_pool_surplus: bool
    def __init__(self, via_currency: _Optional[str] = ..., drained_amount: _Optional[float] = ..., produced_amount: _Optional[float] = ..., rate: _Optional[float] = ..., used_pool_surplus: _Optional[bool] = ...) -> None: ...

class SubmitTransferResponse(_message.Message):
    __slots__ = ("success", "request_id", "dest_amount_paid", "locked_rate", "pool_covered_amount", "fills", "handled_by_node", "error", "idempotent_replay")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    DEST_AMOUNT_PAID_FIELD_NUMBER: _ClassVar[int]
    LOCKED_RATE_FIELD_NUMBER: _ClassVar[int]
    POOL_COVERED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    FILLS_FIELD_NUMBER: _ClassVar[int]
    HANDLED_BY_NODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENT_REPLAY_FIELD_NUMBER: _ClassVar[int]
    success: bool
    request_id: str
    dest_amount_paid: float
    locked_rate: float
    pool_covered_amount: float
    fills: _containers.RepeatedCompositeFieldContainer[FillDetail]
    handled_by_node: str
    error: str
    idempotent_replay: bool
    def __init__(self, success: _Optional[bool] = ..., request_id: _Optional[str] = ..., dest_amount_paid: _Optional[float] = ..., locked_rate: _Optional[float] = ..., pool_covered_amount: _Optional[float] = ..., fills: _Optional[_Iterable[_Union[FillDetail, _Mapping]]] = ..., handled_by_node: _Optional[str] = ..., error: _Optional[str] = ..., idempotent_replay: _Optional[bool] = ...) -> None: ...

class GetBalancesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetBalancesResponse(_message.Message):
    __slots__ = ("balances", "fx_pnl", "handled_by_node")
    class BalancesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    class FxPnlEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    BALANCES_FIELD_NUMBER: _ClassVar[int]
    FX_PNL_FIELD_NUMBER: _ClassVar[int]
    HANDLED_BY_NODE_FIELD_NUMBER: _ClassVar[int]
    balances: _containers.ScalarMap[str, float]
    fx_pnl: _containers.ScalarMap[str, float]
    handled_by_node: str
    def __init__(self, balances: _Optional[_Mapping[str, float]] = ..., fx_pnl: _Optional[_Mapping[str, float]] = ..., handled_by_node: _Optional[str] = ...) -> None: ...

class GetStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetStatusResponse(_message.Message):
    __slots__ = ("node_id", "raft_state", "leader_id", "has_quorum", "raft_term", "commit_index", "log_len")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    RAFT_STATE_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    HAS_QUORUM_FIELD_NUMBER: _ClassVar[int]
    RAFT_TERM_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    LOG_LEN_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    raft_state: str
    leader_id: str
    has_quorum: bool
    raft_term: int
    commit_index: int
    log_len: int
    def __init__(self, node_id: _Optional[str] = ..., raft_state: _Optional[str] = ..., leader_id: _Optional[str] = ..., has_quorum: _Optional[bool] = ..., raft_term: _Optional[int] = ..., commit_index: _Optional[int] = ..., log_len: _Optional[int] = ...) -> None: ...

class GetHistoryRequest(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class GetHistoryResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[TransferRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[TransferRecord, _Mapping]]] = ...) -> None: ...

class TransferRecord(_message.Message):
    __slots__ = ("request_id", "source_currency", "dest_currency", "source_amount", "dest_amount_paid", "locked_rate", "scenario", "timestamp")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CURRENCY_FIELD_NUMBER: _ClassVar[int]
    DEST_CURRENCY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    DEST_AMOUNT_PAID_FIELD_NUMBER: _ClassVar[int]
    LOCKED_RATE_FIELD_NUMBER: _ClassVar[int]
    SCENARIO_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    source_currency: str
    dest_currency: str
    source_amount: float
    dest_amount_paid: float
    locked_rate: float
    scenario: str
    timestamp: float
    def __init__(self, request_id: _Optional[str] = ..., source_currency: _Optional[str] = ..., dest_currency: _Optional[str] = ..., source_amount: _Optional[float] = ..., dest_amount_paid: _Optional[float] = ..., locked_rate: _Optional[float] = ..., scenario: _Optional[str] = ..., timestamp: _Optional[float] = ...) -> None: ...
