from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestVoteRequest(_message.Message):
    __slots__ = ("term", "candidate_id", "last_log_index", "last_log_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    def __init__(self, term: _Optional[int] = ..., candidate_id: _Optional[str] = ..., last_log_index: _Optional[int] = ..., last_log_term: _Optional[int] = ...) -> None: ...

class RequestVoteReply(_message.Message):
    __slots__ = ("term", "vote_granted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_granted: bool
    def __init__(self, term: _Optional[int] = ..., vote_granted: _Optional[bool] = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("index", "term", "command")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    index: int
    term: int
    command: bytes
    def __init__(self, index: _Optional[int] = ..., term: _Optional[int] = ..., command: _Optional[bytes] = ...) -> None: ...

class AppendEntriesRequest(_message.Message):
    __slots__ = ("term", "leader_id", "prev_index", "prev_term", "commit_index", "entries")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    PREV_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_TERM_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    term: int
    leader_id: str
    prev_index: int
    prev_term: int
    commit_index: int
    entries: _containers.RepeatedCompositeFieldContainer[LogEntry]
    def __init__(self, term: _Optional[int] = ..., leader_id: _Optional[str] = ..., prev_index: _Optional[int] = ..., prev_term: _Optional[int] = ..., commit_index: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[LogEntry, _Mapping]]] = ...) -> None: ...

class AppendEntriesReply(_message.Message):
    __slots__ = ("term", "entry_appended", "match_index")
    TERM_FIELD_NUMBER: _ClassVar[int]
    ENTRY_APPENDED_FIELD_NUMBER: _ClassVar[int]
    MATCH_INDEX_FIELD_NUMBER: _ClassVar[int]
    term: int
    entry_appended: bool
    match_index: int
    def __init__(self, term: _Optional[int] = ..., entry_appended: _Optional[bool] = ..., match_index: _Optional[int] = ...) -> None: ...
