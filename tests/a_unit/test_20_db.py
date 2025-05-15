import asyncio
import re
import time

import pytest
from rethinkdb import r

import aiorethink as ar


@pytest.mark.asyncio
async def test_simple_db_conn(db_conn):
    cn = await db_conn
    cn_alt = await db_conn.get()
    assert cn == cn_alt

    dbs = await r.db_list().run(cn)
    assert dbs is not None

    await db_conn.close()


@pytest.mark.asyncio
async def test_cant_reconfigure_db_connection(db_conn):
    with pytest.raises(ar.AlreadyExistsError):
        ar.configure_db_connection(db="foo")


@pytest.mark.asyncio
async def test_double_close(db_conn):
    cn = await db_conn
    await db_conn.close()
    await db_conn.close()


@pytest.mark.asyncio
async def test_double_close2(db_conn):
    cn = await db_conn
    await cn.close()
    await cn.close()


@pytest.mark.asyncio
async def test_double_close3(db_conn):
    cn = await db_conn
    await cn.close()
    await db_conn.close()


@pytest.mark.asyncio
async def test_double_close4(db_conn):
    cn = await db_conn
    await db_conn.close()
    await cn.close()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_multithreading(db_conn, capsys):
    import threading
    import queue

    cns = []
    cns_l = threading.Lock()
    ev = threading.Event()
    completion_queue = queue.Queue()

    def third(i):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def do_stuff():
            try:
                # First connection
                cn = await db_conn
                try:
                    dbs = await r.db_list().run(cn)
                    assert dbs is not None
                finally:
                    await cn.close(False)
                # noinspection PyUnreachableCode
                assert not cn.is_open()

                with cns_l:
                    cns.append(cn)

                # Wait for the main thread signal
                await loop.run_in_executor(None, ev.wait)

                # Second connection
                cn = await db_conn
                try:
                    await r.db_list().run(cn)
                finally:
                    await cn.close(False)
                # noinspection PyUnreachableCode
                assert not cn.is_open()

            except Exception as e:
                completion_queue.put((i, e))
                return

            completion_queue.put((i, None))

        try:
            loop.run_until_complete(do_stuff())

            # Clean up remaining tasks
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        finally:
            loop.close()
        # noinspection PyUnreachableCode
        print("##TEST thread ran all the way through##")

    num_threads = 10
    threads = [threading.Thread(target=third, args=(i,)) for i in range(num_threads)]
    [t.start() for t in threads]

    # Wait for all connections with timeout
    timeout = 10
    start_time = time.time()
    while time.time() - start_time < timeout:
        await asyncio.sleep(0.1)
        with cns_l:
            if len(cns) == num_threads:
                break
    else:
        raise TimeoutError("Threads did not complete in time")

    # Verify unique connections
    cns_set = set(cns)
    assert len(cns_set) == num_threads

    # Let threads continue and wait for completion
    ev.set()
    [t.join() for t in threads]

    # Check for thread errors
    errors = []
    while not completion_queue.empty():
        thread_id, error = completion_queue.get()
        if error:
            errors.append(f"Thread {thread_id}: {error}")

    if errors:
        raise Exception("Thread errors occurred:\n" + "\n".join(errors))

    # Verify all threads completed
    out, err = capsys.readouterr()
    matches = re.findall("##TEST thread ran all the way through##", out)
    assert len(matches) == num_threads

@pytest.fixture
def doc_classes(db_conn):
    class Doc1(ar.Document):
        pass

    class Doc2(ar.Document):
        @classmethod
        async def _create_table_extras(cls, conn=None):
            cn = conn or await db_conn
            await cls.cq().index_create('some_field').run(cn)

    class Doc3(ar.Document):
        @classmethod
        async def _reconfigure_table(cls, conn=None):
            cn = conn or await db_conn
            indexes = await cls.cq().index_list().run(cn)
            if len(indexes) == 0:
                await cls.cq().index_create('some_field').run(cn)

    return Doc1, Doc2, Doc3


@pytest.mark.asyncio
async def test_init_db_create_db(db_conn):
    cn = await db_conn
    dbs = await r.db_list().run(cn)

    assert "testing" not in dbs
    await ar.init_app_db()
    dbs = await r.db_list().run(cn)
    assert "testing" in dbs

    await r.db_drop("testing").run(cn)


@pytest.mark.asyncio
async def test_init_db_create_and_reconfigure_tables(doc_classes,
                                                     db_conn, aiorethink_db_session):
    cn = await db_conn

    await ar.init_app_db()

    tables = await r.table_list().run(cn)
    assert len(tables) == len(doc_classes)
    for D in doc_classes:
        assert D._tablename in tables

    Doc1, Doc2, Doc3 = doc_classes

    indexes = await Doc1.cq().index_list().run(cn)
    assert len(indexes) == 0

    indexes = await Doc2.cq().index_list().run(cn)
    assert len(indexes) == 1

    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 0

    await ar.init_app_db()
    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 0

    await ar.init_app_db(reconfigure_db=True)
    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 1


@pytest.mark.asyncio
async def test_run_query(db_conn, aiorethink_db_session):
    cn = await db_conn

    q1 = r.db_list()
    q2 = r.db_list().run(cn)

    for q in [q1, q2]:
        res = await ar.db._run_query(q)
        assert "testing" in res

    with pytest.raises(TypeError):
        await ar.db._run_query("Hello")


@pytest.mark.asyncio
async def test_cursor_async_map(db_conn, aiorethink_db_session):
    from aiorethink.db import CursorAsyncMap
    cn = await db_conn

    await r.table_create("test").run(cn)
    for v in [1, 2, 3]:
        await r.table("test").insert({"v": v}).run(cn)

    cursor = await r.table("test").run(cn)
    mapper = lambda d: d["v"]

    vs = []
    async for v in CursorAsyncMap(cursor, mapper):
        assert type(v) == int
        vs.append(v)
    vs.sort()
    assert vs == [1, 2, 3]

    cursor = await r.table("test").run(cn)
    vs = await CursorAsyncMap(cursor, mapper).as_list()
    vs.sort()
    assert vs == [1, 2, 3]


@pytest.mark.asyncio
async def test_aiter_changes(event_loop, db_conn, aiorethink_db_session):
    cn = await db_conn

    await r.table_create("test").run(cn)

    async def track_table_changes(num_changes, vt):
        i = 0
        q = r.table("test").pluck("v").changes(include_states=True, include_initial=True)
        values = []
        async for obj, msg in await ar.aiter_changes(q, vt):
            i += 1
            assert obj is None or isinstance(list(obj.values())[0], set)
            if obj is not None:
                values.append(list(obj.values())[0])
            assert "new_val" in msg
            if i >= num_changes:
                break
        return values

    table_tracker = event_loop.create_task(track_table_changes(3,
                                                               ar.DictValueType(ar.StringValueType(),
                                                                                ar.SetValueType(ar.IntValueType()))
                                                               ))

    await asyncio.sleep(0.5)  # make reasonably sure that table_tracker is listening
    await r.table("test").insert({"id": 1, "v": [1, 2, 3]}).run(cn)
    await r.table("test").insert({"v": [4, 5]}).run(cn)
    await r.table("test").get(1).delete().run(cn)
    await r.table("test").insert({"v": [6]}).run(cn)
    await r.table("test").insert({"v": [7]}).run(cn)

    done, pending = await asyncio.wait([table_tracker], timeout=5.0)
    assert table_tracker in done
    if table_tracker.exception() is not None:
        table_tracker.print_stack()
    assert table_tracker.exception() is None
    assert table_tracker.result() == [{1, 2, 3}, {4, 5}]