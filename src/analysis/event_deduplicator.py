"""Cross-source event deduplication using local sentence embeddings and DBSCAN clustering."""

from __future__ import annotations

import numpy as np
import structlog
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity

from src.collectors.base import CollectedItem

logger = structlog.get_logger()

# Multilingual model supporting Chinese + English (~120MB)
_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
_model = None

# Source tier ranking for canonical item selection (higher = better)
_SOURCE_TIER_RANK = {"high": 3, "medium": 2, "low": 1}


def _get_model():
    """Lazy-load the sentence-transformers model to avoid startup cost."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        logger.info("loading_embedding_model", model=_MODEL_NAME)
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def _item_text(item: CollectedItem) -> str:
    """Combine title and content into a single string for embedding."""
    parts = [item.title]
    if item.content:
        # Use first 500 chars of content to keep embedding focused
        parts.append(item.content[:500])
    return " ".join(parts)


def embed_items(items: list[CollectedItem]) -> np.ndarray:
    """Compute sentence embeddings for a list of CollectedItems.

    Args:
        items: List of collected items to embed.

    Returns:
        np.ndarray of shape (len(items), embedding_dim).
    """
    if not items:
        return np.array([])

    model = _get_model()
    texts = [_item_text(item) for item in items]
    embeddings = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
    logger.debug("items_embedded", count=len(items), dim=embeddings.shape[1])
    return embeddings


def cluster_events(
    items: list[CollectedItem],
    embeddings: np.ndarray,
    threshold: float = 0.85,
) -> list[list[CollectedItem]]:
    """Cluster items by semantic similarity using DBSCAN with cosine distance.

    Args:
        items: List of collected items.
        embeddings: Pre-computed embeddings from embed_items().
        threshold: Cosine similarity threshold for clustering (0-1).
            Items with similarity >= threshold are grouped together.

    Returns:
        List of clusters, where each cluster is a list of CollectedItems.
        Singleton items (no duplicates) appear as single-element clusters.
    """
    if len(items) <= 1:
        return [[item] for item in items]

    # DBSCAN uses distance, not similarity: distance = 1 - similarity
    eps = 1.0 - threshold

    # Compute cosine distance matrix
    sim_matrix = cosine_similarity(embeddings)
    distance_matrix = 1.0 - sim_matrix

    # Clip to avoid floating point issues
    distance_matrix = np.clip(distance_matrix, 0.0, 2.0)

    clustering = DBSCAN(eps=eps, min_samples=1, metric="precomputed")
    labels = clustering.fit_predict(distance_matrix)

    # Group items by cluster label
    cluster_map: dict[int, list[CollectedItem]] = {}
    for idx, label in enumerate(labels):
        cluster_map.setdefault(label, []).append(items[idx])

    clusters = list(cluster_map.values())
    multi = sum(1 for c in clusters if len(c) > 1)
    logger.info(
        "clustering_complete",
        total_items=len(items),
        clusters=len(clusters),
        multi_item_clusters=multi,
    )
    return clusters


def pick_canonical_item(cluster: list[CollectedItem]) -> CollectedItem:
    """Select the best representative item from a cluster.

    Selection criteria (in order):
    1. Highest source tier (high > medium > low)
    2. Longest content (more detail is better)

    The chosen item gets cluster_size added to its metadata.

    Args:
        cluster: A list of CollectedItems in the same semantic cluster.

    Returns:
        The canonical CollectedItem with cluster_size in metadata.
    """
    if len(cluster) == 1:
        item = cluster[0]
        item.metadata["cluster_size"] = 1
        return item

    def sort_key(item: CollectedItem) -> tuple[int, int]:
        tier = item.metadata.get("priority", "medium")
        tier_rank = _SOURCE_TIER_RANK.get(tier, 1)
        content_length = len(item.content)
        return (tier_rank, content_length)

    best = max(cluster, key=sort_key)
    best.metadata["cluster_size"] = len(cluster)

    # Collect source URLs from all items in cluster for reference
    all_urls = [item.url for item in cluster if item.url and item.url != best.url]
    if all_urls:
        best.metadata["related_urls"] = all_urls

    return best


def deduplicate_items(items: list[CollectedItem]) -> list[CollectedItem]:
    """Main entry point: deduplicate items across sources using semantic similarity.

    Pipeline:
    1. Embed all items using multilingual sentence transformer.
    2. Cluster by cosine similarity (threshold=0.85).
    3. Pick canonical representative from each cluster.

    Args:
        items: List of collected items, potentially from multiple sources.

    Returns:
        Deduplicated list of canonical items, each with cluster_size in metadata.
    """
    if not items:
        return []

    if len(items) == 1:
        items[0].metadata["cluster_size"] = 1
        return items

    logger.info("deduplication_started", item_count=len(items))

    embeddings = embed_items(items)
    clusters = cluster_events(items, embeddings, threshold=0.85)
    canonical = [pick_canonical_item(cluster) for cluster in clusters]

    removed = len(items) - len(canonical)
    logger.info(
        "deduplication_complete",
        input_count=len(items),
        output_count=len(canonical),
        duplicates_removed=removed,
    )
    return canonical
