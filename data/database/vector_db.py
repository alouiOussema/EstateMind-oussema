"""
EstateMind — Pinecone vector database handler.

- EmbeddingModel: singleton, loaded once per process, CPU-only (no CUDA)
- Dimension: 384 (paraphrase-multilingual-MiniLM-L12-v2, free, local)
- None values stripped from metadata before upsert (Pinecone rejects nulls)
- Proper index-ready polling instead of blind sleep
"""
from __future__ import annotations

import os
import time
from typing import List, Optional, Dict, Any

from loguru import logger

from core.models import PropertyListing


# ─── Embedding config ─────────────────────────────────────────────────────────

EMBEDDING_STRATEGIES = {
    "huggingface": {
        "model_name": "paraphrase-multilingual-MiniLM-L12-v2",
        "dimension": 384,
    },
}


def _clean_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pinecone rejects None / null metadata values.
    Keep only strings, numbers, booleans, and lists of strings.
    """
    clean = {}
    for k, v in raw.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            clean[k] = v
        elif isinstance(v, list):
            # Pinecone accepts list of strings only
            str_list = [str(i) for i in v if i is not None]
            clean[k] = str_list
        # skip dicts and other types silently
    return clean


# ─── Embedding model (singleton) ──────────────────────────────────────────────

class EmbeddingModel:
    """
    Loaded once per process. Runs entirely on CPU — no CUDA, no GPU needed.
    """
    _model = None  # class-level singleton

    @classmethod
    def _get_model(cls, model_name: str):
        if cls._model is None:
            logger.info(f"Loading model: {model_name} (one-time, CPU)")
            from sentence_transformers import SentenceTransformer
            cls._model = SentenceTransformer(model_name)
            test = cls._model.encode(["test"])
            logger.info(f"Model ready, dim={len(test[0])}")
        return cls._model

    @classmethod
    def embed_hf(cls, texts: List[str], model_name: str) -> List[List[float]]:
        model = cls._get_model(model_name)
        return model.encode(texts, convert_to_numpy=True).tolist()


# ─── Pinecone handler ─────────────────────────────────────────────────────────

class VectorDBHandler:
    """
    Pinecone vector database handler for EstateMind property listings.

    Usage:
        db = VectorDBHandler(strategy="huggingface")  # free, local, CPU
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        index_name: str = "property-listings",
        cloud: str = "aws",
        region: str = "us-east-1",
        strategy: str = "huggingface",
    ):
        self.api_key    = api_key or os.getenv("PINECONE_API_KEY")
        if not self.api_key:
            raise ValueError("PINECONE_API_KEY not set in .env")

        self.index_name = index_name
        self.cloud      = cloud
        self.region     = region
        self.strategy   = strategy

        cfg = EMBEDDING_STRATEGIES.get(strategy)
        if not cfg:
            raise ValueError(
                f"Unknown strategy '{strategy}'. Only 'huggingface' is supported."
            )
        self.model_name: str = cfg["model_name"]
        self.dimension: int  = cfg["dimension"]

        try:
            from pinecone import Pinecone, ServerlessSpec
            self._Pinecone       = Pinecone
            self._ServerlessSpec = ServerlessSpec
        except ImportError:
            raise RuntimeError("pip install pinecone")

        self.client = self._Pinecone(api_key=self.api_key)
        self.index  = None
        self._connect()

    # ── Connection ────────────────────────────────────────────────────────────

    def _connect(self):
        existing = {idx.name for idx in self.client.list_indexes()}
        if self.index_name not in existing:
            logger.info(
                f"Creating Pinecone index '{self.index_name}' (dim={self.dimension})"
            )
            self.client.create_index(
                name=self.index_name,
                dimension=self.dimension,
                metric="cosine",
                spec=self._ServerlessSpec(cloud=self.cloud, region=self.region),
            )
            for _ in range(60):
                desc = self.client.describe_index(self.index_name)
                if desc.status.get("ready", False):
                    break
                time.sleep(2)
            logger.info("Index ready")

        self.index = self.client.Index(self.index_name)
        logger.info(f"Connected to Pinecone index '{self.index_name}'")

    # ── Embedding ─────────────────────────────────────────────────────────────

    def _embed(self, texts: List[str]) -> List[List[float]]:
        return EmbeddingModel.embed_hf(texts, self.model_name)

    # ── Upsert single ─────────────────────────────────────────────────────────

    def upsert_listing(self, listing: PropertyListing) -> bool:
        """
        Embed + upsert one listing.
        None metadata values are stripped — Pinecone rejects nulls.
        """
        try:
            text       = listing.to_embedding_text()
            embedding  = self._embed([text])[0]
            vector_id  = f"{listing.source_name}:{listing.source_id}"
            metadata   = _clean_metadata(listing.to_vector_metadata())

            self.index.upsert(vectors=[{
                "id":       vector_id,
                "values":   embedding,
                "metadata": metadata,
            }])
            logger.debug(f"Upserted {vector_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert {listing.source_id}: {e}")
            return False

    # ── Batch upsert ──────────────────────────────────────────────────────────

    def upsert_listings(
        self, listings: List[PropertyListing], batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Batch upsert — embeddings generated in one pass per batch.
        More efficient than calling upsert_listing() one by one.
        """
        stats = {"success": 0, "failed": 0}
        for i in range(0, len(listings), batch_size):
            batch = listings[i : i + batch_size]
            texts = [l.to_embedding_text() for l in batch]
            try:
                embeddings = self._embed(texts)
            except Exception as e:
                logger.error(f"Embedding batch {i // batch_size + 1} failed: {e}")
                stats["failed"] += len(batch)
                continue

            vectors = [
                {
                    "id":       f"{l.source_name}:{l.source_id}",
                    "values":   emb,
                    "metadata": _clean_metadata(l.to_vector_metadata()),
                }
                for l, emb in zip(batch, embeddings)
            ]
            try:
                self.index.upsert(vectors=vectors)
                stats["success"] += len(batch)
                logger.info(
                    f"Batch {i // batch_size + 1}: upserted {len(batch)} listings"
                )
            except Exception as e:
                logger.error(f"Batch upsert failed: {e}")
                stats["failed"] += len(batch)

        return stats

    # ── Semantic search ───────────────────────────────────────────────────────

    def semantic_search(
        self,
        query: str,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Semantic similarity search with optional metadata filters."""
        try:
            q_emb   = self._embed([query])[0]
            results = self.index.query(
                vector=q_emb,
                top_k=top_k,
                filter=filters,
                include_metadata=True,
                include_values=False,
            )
            return [
                {"id": m.id, "score": m.score, "metadata": m.metadata}
                for m in results.matches
            ]
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            return []

    def find_similar(
        self, listing: PropertyListing, top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """Find listings similar to a given one (for price comparison)."""
        return self.semantic_search(
            query=listing.to_embedding_text(),
            top_k=top_k + 1,
            filters={"transaction_type": {"$eq": listing.transaction_type}},
        )

    def check_duplicate(
        self, listing: PropertyListing, threshold: float = 0.97
    ) -> bool:
        """True if a near-identical listing already exists (cross-source dedup)."""
        results = self.semantic_search(listing.to_embedding_text(), top_k=1)
        if results and results[0]["score"] >= threshold:
            existing_id = results[0]["id"]
            if existing_id != f"{listing.source_name}:{listing.source_id}":
                logger.debug(
                    f"Duplicate: {listing.source_id} ≈ {existing_id} "
                    f"(score={results[0]['score']:.3f})"
                )
                return True
        return False

    # ── Stats / maintenance ───────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        try:
            s = self.index.describe_index_stats()
            return {
                "total_vectors": s.total_vector_count,
                "dimension":     s.dimension,
                "namespaces":    dict(s.namespaces) if s.namespaces else {},
            }
        except Exception as e:
            logger.error(f"Stats failed: {e}")
            return {}

    def delete_by_source(self, source_name: str) -> int:
        """Delete all vectors for a given source using ID prefix."""
        try:
            ids_to_delete = []
            for id_batch in self.index.list(prefix=f"{source_name}:"):
                ids_to_delete.extend(id_batch)
            if not ids_to_delete:
                return 0
            for i in range(0, len(ids_to_delete), 1000):
                self.index.delete(ids=ids_to_delete[i : i + 1000])
            logger.info(f"Deleted {len(ids_to_delete)} vectors for {source_name}")
            return len(ids_to_delete)
        except Exception as e:
            logger.error(f"delete_by_source failed: {e}")
            return 0

    def close(self):
        """No-op — Pinecone is a managed service."""
        pass