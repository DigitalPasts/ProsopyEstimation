"""
u-iter.py

This module calculates the maximal number of dateable documents based on the U_iter measurement described in the paper.
It can calculate this measurement for any given dataftrame of tablets.
It is ran (seperately) on both the undated and the originally dated texts.
The module is completely independent of the estimation and validation modules and only has theoratical significance.

"""

from collections import defaultdict, deque
from typing import Iterable, Hashable, Tuple, Set, Dict, List, Optional
from helpers import get_fully_dated_rows_by_julian


# ---------- U_iter for UNDATED tablets (full dataset) ----------

def compute_u_iter_undated(
    edges: Iterable[Tuple[Hashable, Hashable]],
    dated_tablets: Set[Hashable],
    all_tablets: Set[Hashable],
):
    """
    Compute U_iter for UNDATED tablets, using the FULL dataset (dated + undated).

    Parameters
    ----------
    edges : iterable of (tablet_id, person_id)
        All attestations in the corpus (dated + undated tablets).
    dated_tablets : set
        Set of tablet IDs that have some date (T^dated).
    all_tablets : set
        Set of ALL tablet IDs in the corpus (T).

    Returns
    -------
    u_iter_undated : float
        Theoretical upper bound for undated tablets.
    reachable_undated : set
        The undated tablets that lie in components with >=1 dated tablet.
    """
    edges = list(edges)

    undated_tablets: Set[Hashable] = set(all_tablets) - set(dated_tablets)
    if not undated_tablets:
        return 0.0, set()

    # Build bipartite adjacency
    # Nodes are ("T", tablet_id) and ("P", person_id)
    adj: Dict[Tuple[str, Hashable], Set[Tuple[str, Hashable]]] = defaultdict(set)

    for t, p in edges:
        t_node = ("T", t)
        p_node = ("P", p)
        adj[t_node].add(p_node)
        adj[p_node].add(t_node)

    # Ensure isolated tablets (no persons) exist as nodes
    for t in all_tablets:
        t_node = ("T", t)
        if t_node not in adj:
            adj[t_node] = set()

    visited: Set[Tuple[str, Hashable]] = set()
    reachable_undated: Set[Hashable] = set()

    # Traverse components
    for node in list(adj.keys()):
        if node in visited:
            continue

        queue = deque([node])
        visited.add(node)

        component_tablets: List[Hashable] = []

        while queue:
            current = queue.popleft()
            kind, obj_id = current

            if kind == "T":
                component_tablets.append(obj_id)

            for neigh in adj[current]:
                if neigh not in visited:
                    visited.add(neigh)
                    queue.append(neigh)

        # Tablets in this component
        comp_dated = [t for t in component_tablets if t in dated_tablets]
        comp_undated = [t for t in component_tablets if t in undated_tablets]

        # If there is any dated tablet, all undated in this component are structurally dateable
        if len(comp_dated) > 0:
            reachable_undated.update(comp_undated)

    u_iter = len(reachable_undated) / len(undated_tablets)
    return u_iter, reachable_undated


# ---------- U_iter for FULLY DATED tablets (validation setting) ----------

def compute_u_iter_dated(
    edges: Iterable[Tuple[Hashable, Hashable]],
    fully_dated_tablets: Set[Hashable],
):
    """
    Compute U_iter for FULLY DATED tablets, in the validation regime where
    only fully dated tablets are used as anchors, and undated tablets are ignored.

    Graph is built ONLY from fully dated tablets + people they mention.

    A fully dated tablet is structurally testable iff its connected component
    contains at least one other fully dated tablet (>= 2 tablets in that component).

    Parameters
    ----------
    edges : iterable of (tablet_id, person_id)
        Attestations restricted to fully dated tablets (i.e., built from fully_dated_df).
    fully_dated_tablets : set
        Set of fully dated tablet IDs (T^full).

    Returns
    -------
    u_iter_dated : float
        Theoretical upper bound for validation on fully dated tablets.
    testable_dated : set
        The subset of fully dated tablets that lie in components with >=2 tablets.
    """
    edges = list(edges)
    if not fully_dated_tablets:
        return 0.0, set()

    # Build bipartite adjacency
    adj: Dict[Tuple[str, Hashable], Set[Tuple[str, Hashable]]] = defaultdict(set)

    for t, p in edges:
        t_node = ("T", t)
        p_node = ("P", p)
        adj[t_node].add(p_node)
        adj[p_node].add(t_node)

    # Ensure isolated fully dated tablets appear as nodes
    for t in fully_dated_tablets:
        t_node = ("T", t)
        if t_node not in adj:
            adj[t_node] = set()

    visited: Set[Tuple[str, Hashable]] = set()
    testable_dated: Set[Hashable] = set()

    for node in list(adj.keys()):
        if node in visited:
            continue

        queue = deque([node])
        visited.add(node)

        component_tablets: List[Hashable] = []

        while queue:
            current = queue.popleft()
            kind, obj_id = current

            if kind == "T":
                component_tablets.append(obj_id)

            for neigh in adj[current]:
                if neigh not in visited:
                    visited.add(neigh)
                    queue.append(neigh)

        # All tablets in this graph are fully dated by construction
        if len(component_tablets) >= 2:
            testable_dated.update(component_tablets)

    u_iter = len(testable_dated) / len(fully_dated_tablets)
    return u_iter, testable_dated

import pandas as pd


def build_edges_from_df(df, tablet_col="Tablet ID", person_col="PID"):
    """Extract (tablet_id, person_id) edges from a DataFrame of attestations."""
    # Drop rows where tablet or person is missing
    mask = df[tablet_col].notna() & df[person_col].notna()
    sub = df.loc[mask, [tablet_col, person_col]].astype(str)
    return list(zip(sub[tablet_col], sub[person_col]))


def compute_u_iter_both_from_df(
    df: pd.DataFrame,
    tablet_col: str = "Tablet ID",
    person_col: str = "PID",
    julian_col: str = "Split_Julian_dates",
    fully_dated_df: Optional[pd.DataFrame] = None,
):
    """
    Convenience wrapper:
    - computes U_iter for undated tablets using full df
    - computes U_iter for fully dated tablets using fully_dated_df
    """

    # ----- 1) U_iter for UNDATED tablets: use full df -----
    all_tablets = set(df[tablet_col].dropna().astype(str))

    # 'dated_tablets' here can be all tablets that have ANY non-empty julian date
    dated_mask = df[julian_col].notna() & (df[julian_col].astype(str).str.strip() != "")
    dated_tablets = set(df.loc[dated_mask, tablet_col].astype(str))

    edges_full = build_edges_from_df(df, tablet_col=tablet_col, person_col=person_col)

    U_undated, reachable_undated = compute_u_iter_undated(
        edges=edges_full,
        dated_tablets=dated_tablets,
        all_tablets=all_tablets,
    )

    # ----- 2) U_iter for FULLY DATED tablets: use fully_dated_df -----
    # You already have get_fully_dated_rows_by_julian(df), so you can pass its result.
    if fully_dated_df is None:
        raise ValueError(
            "Please provide fully_dated_df (e.g. from get_fully_dated_rows_by_julian(df))."
        )

    fully_dated_df = fully_dated_df.copy()
    fully_dated_df[tablet_col] = fully_dated_df[tablet_col].astype(str)
    fully_dated_df[person_col] = fully_dated_df[person_col].astype(str)

    fully_dated_tablets = set(fully_dated_df[tablet_col].unique())
    edges_fully_dated = build_edges_from_df(
        fully_dated_df, tablet_col=tablet_col, person_col=person_col
    )

    U_dated, testable_dated = compute_u_iter_dated(
        edges=edges_fully_dated,
        fully_dated_tablets=fully_dated_tablets,
    )

    return {
        "U_iter_undated": U_undated,
        "reachable_undated": reachable_undated,
        "U_iter_dated": U_dated,
        "testable_dated": testable_dated,
        "n_all_tablets": len(all_tablets),
        "n_dated_tablets_any": len(dated_tablets),
        "n_undated_tablets": len(all_tablets - dated_tablets),
        "n_fully_dated_tablets": len(fully_dated_tablets),
    }

if __name__ == '__main__':
    df = pd.read_csv("../data/input/preprocessed_whole_data.csv")
    fully_dated_df = get_fully_dated_rows_by_julian(df)
    stats = compute_u_iter_both_from_df(
        df,
        tablet_col="Tablet ID",
        person_col="PID",
        julian_col="Split_Julian_dates",
        fully_dated_df=fully_dated_df,
    )

    print("U_iter (undated) =", stats["U_iter_undated"])
    print("U_iter (fully dated, validation) =", stats["U_iter_dated"])
    print("Undated tablets structurally dateable:", len(stats["reachable_undated"]))
    print("Fully dated tablets structurally testable:", len(stats["testable_dated"]))
