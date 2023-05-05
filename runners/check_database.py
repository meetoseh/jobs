"""Checks the health of the database, pinging slack if it's not fully available"""
import logging
from typing import Dict, List, Tuple
from itgs import Itgs
from graceful_death import GracefulDeath
import aiohttp
from jobs import JobCategory
from urllib.parse import urlparse
import asyncio
import traceback

category = JobCategory.LOW_RESOURCE_COST


async def execute(itgs: Itgs, gd: GracefulDeath):
    """Checks on the health of the rqlite cluster, reporting partial or complete outages.
    Specifically, the following checks are performed:

    - Database responsive, i.e., "SELECT 1" works at the strong consistency level on
      any node, using the standard rqlite client
    - All nodes are responsive and agree on the leader, i.e., "SELECT 1" at the weak
      consistency level on all nodes agrees on which node should serve the request
    - All nodes agree on the cluster (as if by .nodes), that the cluster is fully
      reachable (i.e., all nodes in the cluster are reachable) and that cluster matches
      our configured cluster

    Args:
        itgs (Itgs): the integration to use; provided automatically
        gd (GracefulDeath): the signal tracker; provided automatically
    """
    await verify_any_node_responsive(itgs)
    await verify_all_nodes_responsive_and_agree_on_leader(itgs)
    await verify_nodes_agree_on_cluster(itgs)


async def verify_any_node_responsive(itgs: Itgs) -> None:
    """Verifies that any node in the cluster is responsive, using the
    standard client. Raises an exception if not.
    """
    conn = await itgs.conn()
    cursor = conn.cursor("strong")
    response = await cursor.execute("SELECT 1")
    assert (
        response.results is not None
        and len(response.results) == 1
        and len(response.results[0]) == 1
        and response.results[0][0] == 1
    ), f"invalid response to SELECT 1: {response.results=}"
    logging.info("Database is responsive to strong-selects")


async def verify_all_nodes_responsive_and_agree_on_leader(itgs: Itgs) -> None:
    conn = await itgs.conn()
    tasks: Dict[Tuple[str, int], asyncio.Task[Tuple[str, int]]] = dict()
    for ip, port in conn.hosts:
        task = asyncio.create_task(_get_node_leader_using_weak_select(ip, port))
        tasks[(ip, port)] = task

    answers: Dict[Tuple[str, int], Tuple[str, int]] = {}
    errors: Dict[Tuple[str, int], Exception] = {}

    await asyncio.wait(tasks.values(), return_when=asyncio.ALL_COMPLETED)
    for node, task in tasks.items():
        exc = task.exception()
        if exc is not None:
            errors[node] = exc
        else:
            answers[node] = task.result()

    if errors:
        if len(errors) == 1:
            for node, exc in errors.items():
                raise Exception(
                    f"node {node} failed to respond to weak select"
                ) from exc

        pretty_errors = []
        failing_nodes: List[str] = []
        for node, exc in errors.items():
            formatted_exception = traceback.format_exception(
                type(exc), exc, exc.__traceback__
            )
            joined_exception = "\n".join(formatted_exception)
            failing_nodes.append(f"{node[0]}:{node[1]}")
            pretty_errors.append(
                f"node {node} failed to respond to weak select:\n\n```{joined_exception}```"
            )

        logging.warn(
            f"multiple nodes failed to respond to weak select:\n\n"
            + "\n\n".join(pretty_errors)
        )
        raise Exception(
            f"multiple nodes failed to respond to weak select: {failing_nodes}"
        )

    leader_according_to_nodes = set(answers.values())
    if len(leader_according_to_nodes) != 1:
        raise Exception(f"multiple leaders according to nodes: {answers=}")

    logging.info(f"All nodes agree on the leader: {list(leader_according_to_nodes)[0]}")


async def _get_node_leader_using_weak_select(
    ip: str, port: int, *, timeout: int = 5
) -> Tuple[str, int]:
    """Gets the leader of the cluster according to the given ip address,
    using a weak select. If the node is not the leader, it should direct
    us to the leader, otherwise it should answer the request.
    """
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as session:
        response = await session.post(
            f"http://{ip}:{port}/db/query?level=weak&redirect",
            json=[["SELECT 1"]],
            headers={"Content-Type": "application/json"},
        )

        if response.status >= 200 and response.status <= 299:
            return (ip, port)

        if response.status in (301, 302, 303, 307, 308):
            new_location = response.headers["Location"]
            parsed_location = urlparse(new_location)
            assert (
                parsed_location.scheme == "http"
            ), f"unexpected scheme: {new_location=}"
            assert (
                parsed_location.hostname is not None
            ), f"unexpected lack of hostname: {new_location=}"
            return (
                parsed_location.hostname,
                parsed_location.port if parsed_location.port is not None else 80,
            )

        text = await response.text(encoding="utf-8", errors="replace")
        raise Exception(
            f"unexpected response from {ip}: {response.status=} {response.headers=}\n\n```{text}```"
        )


async def verify_nodes_agree_on_cluster(itgs: Itgs) -> None:
    conn = await itgs.conn()
    tasks: Dict[Tuple[str, int], asyncio.Task[List[Tuple[str, int]]]] = dict()

    for ip, port in conn.hosts:
        task = asyncio.create_task(_get_cluster_using_node(ip, port))
        tasks[(ip, port)] = task

    answers: Dict[Tuple[str, int], List[Tuple[str, int]]] = {}
    errors: Dict[Tuple[str, int], Exception] = {}

    await asyncio.wait(tasks.values(), return_when=asyncio.ALL_COMPLETED)
    for node, task in tasks.items():
        exc = task.exception()
        if exc is not None:
            errors[node] = exc
        else:
            answers[node] = task.result()

    if errors:
        if len(errors) == 1:
            for node, exc in errors.items():
                raise Exception(f"node {node} failed to get cluster") from exc

        pretty_errors = []
        failing_nodes: List[str] = []
        for node, exc in errors.items():
            formatted_exception = traceback.format_exception(
                type(exc), exc, exc.__traceback__
            )
            joined_exception = "\n".join(formatted_exception)
            failing_nodes.append(f"{node[0]}:{node[1]}")
            pretty_errors.append(
                f"node {node} failed to get cluster:\n\n```{joined_exception}```"
            )

        logging.warn(
            f"multiple nodes failed to get cluster:\n\n" + "\n\n".join(pretty_errors)
        )
        raise Exception(f"multiple nodes failed to get cluster: {failing_nodes}")

    clusters = set(tuple(sorted(x)) for x in answers.values())

    if len(clusters) != 1:
        raise Exception(f"split brain; not all nodes agree on the cluster {answers=}")

    expected_cluster = tuple(sorted(conn.hosts))
    if expected_cluster not in clusters:
        raise Exception(
            f"stale configuration; this job node has cluster {expected_cluster} but the real cluster is {list(clusters)[0]}"
        )

    logging.info(
        "All nodes agree on the cluster, and the cluster matches our configuration"
    )


async def _get_cluster_using_node(
    ip: str, port: int, *, timeout: int = 5
) -> List[Tuple[str, int]]:
    """Gets the cluster according to the node at the given ip and port, raising
    an exception if the given node cannot reach any of the other nodes.
    """
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as session:
        response = await session.get(f"http://{ip}:{port}/nodes")
        response.raise_for_status()

        data = await response.json(encoding="utf-8")
        assert isinstance(data, dict), f"unexpected data type: {data=}"

        result: List[Tuple[str, int]] = []
        for _, info in data.items():
            assert isinstance(
                info, dict
            ), f"unexpected data type: {info=} within {data=}"
            api_addr = info.get("api_addr")
            assert isinstance(
                api_addr, str
            ), f"unexpected api_addr: {info=} within {data=}"
            parsed_api_addr = urlparse(api_addr)
            assert (
                parsed_api_addr.scheme == "http"
            ), f"unexpected api_addr scheme: {info=} within {data=}"
            assert (
                parsed_api_addr.hostname is not None
            ), f"unexpected lack of hostname: {info=} within {data=}"

            node = (
                parsed_api_addr.hostname,
                parsed_api_addr.port if parsed_api_addr.port is not None else 80,
            )
            assert (
                node not in result
            ), f"unexpected duplicate node: {node=} within {data=}"
            reachable = info.get("reachable")
            assert isinstance(
                reachable, bool
            ), f"unexpected reachable: {info=} within {data=}"
            if not reachable:
                raise Exception(
                    f"node {ip}:{port} reports that {node[0]}:{node[1]} is unreachable"
                )

            result.append(node)

        return result


if __name__ == "__main__":
    import asyncio

    async def main():
        async with Itgs() as itgs:
            jobs = await itgs.jobs()
            await jobs.enqueue("runners.check_database")

    asyncio.run(main())
