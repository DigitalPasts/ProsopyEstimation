"""
main.py — ProsopyEstimation pipeline runner

Runs the full estimation pipeline in order:
  1. estimate()  → ../data/output/estimation_results.csv
  2. validate()  → ../data/output/validation_results_after_correction_17.csv

Prerequisites:
  - Place preprocessed_whole_data.csv in ../data/input/ (download from ProsopyBase)

Optional (Gephi export — output not committed to the repo):
  from main import export_tablet_network_to_gephi
  export_tablet_network_to_gephi()
"""

import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from collections import defaultdict

from estimation import estimate
from validation import validate

estimation_results_path = "../data/output/estimation_results.csv"
preprocessed_data_path = "../data/input/preprocessed_whole_data.csv"


def visualize_bipartite_network(results_path=estimation_results_path, data_path=preprocessed_data_path):
    """
    Display a bipartite network of people and tablets.

    - Tablets on one side, people (PID) on the other
    - Edges connect tablets to the people mentioned in them
    - Colors:
        - Blue: pre-dated tablets
        - Red: newly estimated tablets
        - Grey: unestimatable tablets
        - White: people (uncolored)
    """
    results_df = pd.read_csv(results_path)
    data_df = pd.read_csv(data_path)

    tablet_status = dict(zip(results_df['Tablet ID'], results_df['status']))

    G = nx.Graph()

    tablets = data_df['Tablet ID'].unique()
    for tab_id in tablets:
        status = tablet_status.get(tab_id, 'unestimatable')
        G.add_node(f"T_{tab_id}", bipartite=0, node_type='tablet', status=status)

    people = data_df['PID'].unique()
    for pid in people:
        G.add_node(f"P_{pid}", bipartite=1, node_type='person')

    for _, row in data_df.iterrows():
        tab_node = f"T_{row['Tablet ID']}"
        person_node = f"P_{row['PID']}"
        if not G.has_edge(tab_node, person_node):
            G.add_edge(tab_node, person_node)

    tablet_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'tablet']
    person_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'person']

    color_map = {
        'pre_dated': 'blue',
        'newly_estimated': 'red',
        'unestimatable': 'grey'
    }
    tablet_colors = [color_map.get(G.nodes[n].get('status', 'unestimatable'), 'grey') for n in tablet_nodes]
    person_colors = ['white'] * len(person_nodes)

    pos = {}
    for i, node in enumerate(tablet_nodes):
        pos[node] = (0, i)
    for i, node in enumerate(person_nodes):
        pos[node] = (1, i * (len(tablet_nodes) / max(len(person_nodes), 1)))

    plt.figure(figsize=(20, 16))
    nx.draw_networkx_edges(G, pos, alpha=0.3, edge_color='lightgray')
    nx.draw_networkx_nodes(G, pos, nodelist=tablet_nodes, node_color=tablet_colors, node_size=50, alpha=0.8)
    nx.draw_networkx_nodes(G, pos, nodelist=person_nodes, node_color=person_colors,
                           node_size=30, alpha=0.8, edgecolors='black', linewidths=0.5)

    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=10, label='Pre-dated'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='Newly estimated'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='grey', markersize=10, label='Unestimatable'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='white',
                   markeredgecolor='black', markersize=10, label='Person')
    ]
    plt.legend(handles=legend_elements, loc='upper right')
    plt.title("Bipartite Network: Tablets and People")
    plt.axis('off')
    plt.tight_layout()
    plt.show()

    print(f"\nNetwork Statistics:")
    print(f"  Total tablets: {len(tablet_nodes)}")
    print(f"  Total people: {len(person_nodes)}")
    print(f"  Total edges: {G.number_of_edges()}")

    status_counts = results_df['status'].value_counts()
    print(f"\nTablet Status:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")


def export_tablet_network_to_gephi(results_path=estimation_results_path, data_path=preprocessed_data_path,
                                   output_path="../data/output/tablet_network.gexf"):
    """
    Export tablet co-occurrence network to GEXF format for visualization in Gephi.

    Two tablets are connected if they share a person.
    Node attributes include status for coloring in Gephi:
        - pre_dated
        - newly_estimated
        - unestimatable

    Note: the .gexf output is excluded from version control (see .gitignore).
    """
    print("Loading data...")
    results_df = pd.read_csv(results_path)
    data_df = pd.read_csv(data_path)

    tablet_status = dict(zip(results_df['Tablet ID'], results_df['status']))

    print("Building person-tablet mapping...")
    person_to_tablets = defaultdict(set)
    for _, row in data_df.iterrows():
        person_to_tablets[row['PID']].add(row['Tablet ID'])

    G = nx.Graph()

    print("Adding nodes...")
    tablets = data_df['Tablet ID'].unique()
    for tab_id in tablets:
        status = tablet_status.get(tab_id, 'unestimatable')
        color_map = {
            'pre_dated': {'r': 0, 'g': 0, 'b': 255},
            'newly_estimated': {'r': 255, 'g': 0, 'b': 0},
            'unestimatable': {'r': 128, 'g': 128, 'b': 128}
        }
        color = color_map.get(status, color_map['unestimatable'])
        G.add_node(tab_id, status=status, viz={'color': color})

    print("Adding edges...")
    for pid, tab_set in person_to_tablets.items():
        tab_list = list(tab_set)
        for i in range(len(tab_list)):
            for j in range(i + 1, len(tab_list)):
                if not G.has_edge(tab_list[i], tab_list[j]):
                    G.add_edge(tab_list[i], tab_list[j])

    print(f"Exporting to {output_path}...")
    nx.write_gexf(G, output_path)

    print(f"\nNetwork exported to {output_path}")
    print(f"  Total tablets (nodes): {G.number_of_nodes()}")
    print(f"  Total connections (edges): {G.number_of_edges()}")
    print(f"  Connected components: {nx.number_connected_components(G)}")

    status_counts = results_df['status'].value_counts()
    print(f"\nTablet Status:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")

    print(f"\nOpen {output_path} in Gephi to visualize")
    print("  In Gephi: Appearance > Nodes > Color > Partition > status")


def main():
    print("=== Step 1: Date Estimation ===")
    estimate()
    print("\n=== Step 2: Validation ===")
    validate()


if __name__ == '__main__':
    main()
