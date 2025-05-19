import collections
import inspect
import threading
from typing import Callable, Any

from rethinkdb import r
# noinspection PyProtectedMember
from rethinkdb.asyncio_net.net_asyncio import AsyncioCursor

r.set_loop_type("asyncio")

from .errors import IllegalAccessError, AlreadyExistsError
from .registry import registry

__all__ = ["db_conn", "init_app_db", "configure_db_connection",
           "aiter_changes", "ChangesAsyncMap", "CursorAsyncIterator",
           "CursorAsyncMap", "_run_query"]


###############################################################################
# DB connections
###############################################################################

class _OneConnPerThreadPool:
    """Keeps track of one RethinkDB connection per thread.

    Get (or create) the current thread's connection with get() or just
    __await__. close() closes and discards the current thread's connection
    so that a later __await__ or get opens a new connection.
    """

    def __init__(self):
        self._tl = threading.local()
        self._connect_kwargs = None

    def configure_db_connection(self, **connect_kwargs):
        if self._connect_kwargs is not None:
            raise AlreadyExistsError("Can not re-configure DB connection(s)")
        self._connect_kwargs = connect_kwargs

    def __await__(self):
        return self.get().__await__()

    async def get(self):
        """Gets or opens the thread's DB connection.
        """
        if self._connect_kwargs is None:
            raise IllegalAccessError("DB connection parameters not set yet")

        if not hasattr(self._tl, "conn"):
            self._tl.conn = await r.connect(**self._connect_kwargs)

        if self._tl.conn.is_open():
            return self._tl.conn

        self._tl.conn = await r.connect(**self._connect_kwargs)
        return self._tl.conn

    async def close(self, no_reply_wait=True):
        """Closes the thread's DB connection.
        """
        if hasattr(self._tl, "conn"):
            if self._tl.conn.is_open():
                await self._tl.conn.close(no_reply_wait)
            del self._tl.conn

    @property
    def connect_kwargs(self):
        return self._connect_kwargs


db_conn = _OneConnPerThreadPool()


def configure_db_connection(db, **kwargs_for_rethink_connect):
    """Sets DB connection parameters. This function should be called exactly
    once, before init_app_db is called or db_conn is first used.
    """
    db_conn.configure_db_connection(db=db, **kwargs_for_rethink_connect)


###############################################################################
# DB setup (tables and such)
###############################################################################

async def init_app_db(reconfigure_db=False, conn=None):
    cn = conn or await db_conn

    # create DB if it doesn't exist
    our_db = db_conn.connect_kwargs["db"]
    dbs = await r.db_list().run(cn)
    if our_db not in dbs:
        await r.db_create(our_db).run(cn)

    # (re)configure DB tables
    for doc_class in registry.values():
        if not await doc_class.table_exists(cn):
            await doc_class.create_table(cn)
        elif reconfigure_db:
            await doc_class._reconfigure_table(cn)


###############################################################################
# DB query helpers
###############################################################################

async def _run_query(query, conn=None):
    """`run()`s query if a caller hasn't already done so, then awaits and returns
    its result.

    If run() has already been called, then the query (strictly speaking, the
    awaitable) is just awaited. This gives the caller the opportunity to
    customize the run() call.

    If run() has not been called, then the query is run on the given connection
    (or the default connection). This is more convenient for the caller than
    the other version.
    """
    # run() it if caller didn't do that already
    if not inspect.isawaitable(query):
        if not isinstance(query, r.RqlQuery):
            raise TypeError("query is neither awaitable nor a RqlQuery")
        cn = conn or await db_conn
        query = query.run(cn)

    return await query


async def aiter_changes(query, value_type, conn=None):
    """Runs any changes() query, and from its result stream constructs "Python
    world" objects as determined by value_type (which may equal None when
    data is deleted from the DB).

    The function returns an asynchronous iterator (a ``ChangesAsyncMap``),
    which yields `(constructed python object, changefeed message)` tuples.
    Note that `constructed python object` might well be None.

    The `query` might or might not yet have called `run()`, but it should
    not have been awaited on yet (check ``_run_query`` for details).
    """
    feed = await _run_query(query, conn)
    mapper = value_type.dbval_to_pyval
    return ChangesAsyncMap(feed, mapper)


###############################################################################
# Asynchronous iterators over cursors and changefeeds
###############################################################################

class CursorAsyncIterator(collections.abc.AsyncIterator):
    def __init__(self, cursor):
        if not hasattr(cursor, 'next'):
            raise ValueError("Cursor must have 'next' method")
        self.cursor = cursor

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.cursor.next()
        except r.ReqlCursorEmpty:
            raise StopAsyncIteration
        except Exception as e:
            raise ValueError(f"Cursor iteration failed: {e}") from e

    async def aclose(self):
        """Properly close the cursor when done"""
        if hasattr(self.cursor, 'close'):
            await self.cursor.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()


class CursorAsyncMap:
    """Async iterator that iterates through a RethinkDB cursor, mapping each
    object coming out of the cursor to a supplied mapper function.

    Example: Document.from_cursor(cursor) returns a CursorAsyncMap that maps
    each object from the cursor to Document.from_doc().

    The ``as_list()`` coroutine creates a list out of the iterated items.
    """

    def __init__(self, cursor: AsyncioCursor, mapper: Callable[[Any], Any]) -> None:

        self.cursor = cursor
        self.mapper = mapper

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.cursor.__anext__()
        return self.mapper(item)

    async def as_list(self):
        result = []
        async for item in self:
            result.append(item)
        return result


class ChangesAsyncMap(CursorAsyncIterator):
    """Async iterator that iterates over a RethinkDB changefeed, mapping each
    new_val coming in to a supplied mapper function (that typically makes some
    Python object out of it). On each iteration, a tuple (mapped object,
    changefeed message) is yielded. Note that the mapped object might well be
    None, for instance, when documents are deleted from the DB.

    Changefeed messages that do not contain a `new_val` (status messages) are
    ignored.

    Example: ``Document.aiter_changes()`` returns a ChangesAsyncMap that maps
    each new_val (i.e., changed and inserted documents) to Document.from_doc().
    """

    def __init__(self, changefeed, mapper):
        """`changefeed` is a RethinkDB changes stream (technically, a RethinkDB
        cursor). `mapper` is a function accepting one parameter: a `new_val`
        from a changefeed message.
        """
        super().__init__(changefeed)
        self.mapper = mapper

    async def __anext__(self):
        """
        Provides functionality for asynchronously iterating over messages, applying a
        mapping function to transform the "new_val" field, and returning
        the transformed result alongside the original message. This is useful for
        handling asynchronous data streams where certain transformations are needed on
        specific fields of the incoming messages.

        Raises a ValueError if the mapping function fails to process the "new_val" field
        correctly. Relies on an asynchronous iterator to retrieve the next message.

        Returns the mapped value along with the original message if successful.

        Args:
            --
        Raises:
            ValueError: Indicates that the mapping function failed to process the
                        "new_val" field.
            StopAsyncIteration: Raised when the underlying asynchronous iterator has no
                                more items to yield.

        Returns:
            tuple: A tuple containing the mapped value and the original message.
        """
        while True:
            message = await super().__anext__()
            if isinstance(message, dict) and "new_val" in message:
                try:
                    mapped = self.mapper(message["new_val"])
                    return mapped, message
                except Exception as e:
                    raise ValueError(f"Failed to map value: {e}") from e

    async def as_list(self):
        """This is verboten on changefeeds as they have infinite length.
        """
        raise NotImplementedError("as_list makes no sense on changefeeds")
